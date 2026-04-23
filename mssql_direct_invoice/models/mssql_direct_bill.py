from odoo import models, fields
from odoo.exceptions import UserError
from datetime import timedelta, datetime
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlDirectBill(models.Model):
    _inherit = 'mssql.direct.sync'

    # ── Purchase Bill Fields ──────────────────────────────────────────
    purchase_invoice_date = fields.Date(string='Purchase Invoice Date', default=fields.Date.today)

    purchase_payment_method_ids = fields.One2many(
        'mssql.direct.payment.method',
        'sync_config_id',
        string='Purchase Payment Methods',
        domain=[('scope', '=', 'purchase')],
        context={'default_scope': 'purchase'},
    )

    # ── Entry Point ───────────────────────────────────────────────────

    def action_fetch_purchase_payment_methods(self):
        """Fetch distinct vendor payment methods from MSSQL and upsert into
        the O2M. Existing rows keep their journal mapping."""
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT DISTINCT PaymentMethod AS code
                FROM [dbo].[tblSuppliersPayment]
                WHERE PaymentMethod IS NOT NULL
            """)
            rows = cursor.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        name_map = {
            1: 'Cash',
            2: 'Check',
            3: 'Bank Transfer',
        }
        existing = {m.mssql_code: m for m in self.purchase_payment_method_ids}
        to_create = []
        for row in rows:
            code = int(row['code'])
            if code in existing:
                continue
            to_create.append({
                'sync_config_id': self.id,
                'scope': 'purchase',
                'mssql_code': code,
                'name': name_map.get(code, f'PM{code}'),
            })
        if to_create:
            self.env['mssql.direct.payment.method'].create(to_create)
        return self._success_notification(
            'Purchase Payment Methods',
            f"Added {len(to_create)} new; total mapped = {len(self.purchase_payment_method_ids)}."
        )

    def sync_purchase_invoices(self):
        """Sync purchase invoices from SQL Server as direct vendor bills (no PO)."""
        if not self.purchase_invoice_date:
            raise UserError('Please select a purchase invoice date')

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = self.purchase_invoice_date.strftime('%Y-%m-%d')
            next_date = (self.purchase_invoice_date + timedelta(days=1)).strftime('%Y-%m-%d')

            purchase_invoices = self._query_purchase_invoices(cursor, date_str, next_date)
            conn.close()

            if not purchase_invoices:
                raise UserError(f'No purchase invoices found for date {date_str}')

            # Pre-sync vendors if needed
            supplier_ids = list({pi['SupplierID'] for pi in purchase_invoices if pi['SupplierID']})
            vendors = {
                p.x_sql_vendor_id: p for p in self.env['res.partner'].search([
                    ('x_sql_vendor_id', 'in', supplier_ids),
                    ('supplier_rank', '>', 0)
                ])
            }
            missing = [sid for sid in supplier_ids if sid not in vendors]
            if missing:
                _logger.info("Missing vendors detected. Auto-syncing...")
                try:
                    self.sync_vendors()
                except Exception as e:
                    _logger.error(f"Auto-sync vendors failed: {e}")

            existing_refs = set(
                self.env['account.move'].search([
                    ('ref', '!=', False),
                    ('move_type', 'in', ('in_invoice', 'in_refund')),
                ]).mapped('ref')
            )

            queue = self.env['mssql.direct.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'purchase_bill',
                'sync_date': self.purchase_invoice_date,
            })

            line_vals_list = []
            for pi in purchase_invoices:
                inv_id = pi['PurchaseInvoiceID']
                if f"MSSQL-PI-{inv_id}" in existing_refs:
                    continue
                record_data = json.dumps({'invoice': dict(pi)}, default=str)
                supplier_name = pi.get('SupplierName') or f"Supplier {pi['SupplierID']}"
                line_vals_list.append({
                    'queue_id': queue.id,
                    'name': f"PI {inv_id} - {supplier_name}",
                    'mssql_id': str(inv_id),
                    'mssql_table': 'tblPurchaseInvoice',
                    'record_data': record_data,
                })

            if not line_vals_list:
                queue.unlink()
                return self._success_notification(
                    'Purchase Sync', 'No new purchase invoices to process')

            self.env['mssql.direct.sync.queue.line'].create(line_vals_list)
            _logger.info(f"Queue {queue.name}: {len(line_vals_list)} bill lines")

            queue.action_process_queue()

            return {
                'type': 'ir.actions.act_window',
                'name': f'Purchase Queue - {self.purchase_invoice_date}',
                'res_model': 'mssql.direct.sync.queue',
                'res_id': queue.id,
                'view_mode': 'form',
                'target': 'current',
            }

        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            raise UserError(f'Purchase invoice sync failed: {str(e)}')

    # ── Queue Line Processor ─────────────────────────────────────────

    def _process_queue_purchase_bill(self, data, queue_line):
        """Build one aggregate in_invoice (or in_refund) whose total == MSSQL
        NetTotal, then register vendor payments via the purchase payment-method
        mapping."""
        purchase_invoice = data['invoice']

        invoice_id = purchase_invoice['PurchaseInvoiceID']
        is_return = bool(purchase_invoice.get('IsReturn'))
        move_type = 'in_refund' if is_return else 'in_invoice'
        ref = f"MSSQL-PI-{invoice_id}"

        existing_bill = self.env['account.move'].search([
            ('ref', '=', ref),
            ('move_type', '=', move_type),
        ], limit=1)
        if existing_bill:
            _logger.info(f"Skipping PI {invoice_id} — already imported as {existing_bill.name}")
            return {'model': 'account.move', 'id': existing_bill.id, 'skipped': True}

        supplier_id = int(purchase_invoice['SupplierID'])
        vendor = self.env['res.partner'].search([
            ('x_sql_vendor_id', '=', supplier_id),
            ('supplier_rank', '>', 0),
        ], limit=1)
        if not vendor:
            raise UserError(
                f'Vendor (SupplierID: {supplier_id}) not found. '
                f'Run "Sync Vendors" first.')

        net_total = round(abs(float(purchase_invoice['NetTotal'] or 0)), 2)
        if net_total <= 0:
            raise UserError(
                f"PI {invoice_id}: NetTotal {net_total} is not positive — nothing to import.")

        inv_date = purchase_invoice['InvoiceDate']
        if isinstance(inv_date, str):
            inv_date = inv_date[:10]
        inv_due_date = purchase_invoice.get('InvoiceDueDate') or inv_date
        if isinstance(inv_due_date, str):
            inv_due_date = inv_due_date[:10]

        tax = self._get_or_create_vat_15_inclusive('purchase')
        product = self._get_or_create_pos_purchase_product()

        purchase_journal = self.env['account.journal'].search([
            ('type', '=', 'purchase'),
            ('company_id', '=', self.env.company.id),
        ], limit=1)
        if not purchase_journal:
            raise UserError('No purchase journal found.')

        bill = self.env['account.move'].create({
            'move_type': move_type,
            'partner_id': vendor.id,
            'invoice_date': inv_date,
            'invoice_date_due': inv_due_date,
            'date': inv_date,
            'ref': ref,
            'narration': purchase_invoice.get('InvoiceNote', '') or '',
            'journal_id': purchase_journal.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id,
                'quantity': 1,
                'price_unit': net_total,
                'name': f"{'Purchase Return' if is_return else 'Purchase'} - PI {invoice_id}",
                'tax_ids': [(6, 0, [tax.id])],
            })],
        })

        self._assert_total_matches(bill, net_total, f"PI {invoice_id}")

        if is_return:
            if bill.state == 'draft':
                bill.action_post()
        else:
            self._register_vendor_payments(bill, invoice_id, purchase_invoice)

        doc_type = 'Credit Note' if is_return else 'Bill'
        _logger.info(f"PI {invoice_id}: {doc_type} {bill.name} created (residual={bill.amount_residual})")
        return {'model': 'account.move', 'id': bill.id}

    # ── Vendor Payment Registration ───────────────────────────────────

    def _register_vendor_payments(self, bill, purchase_invoice_id, purchase_invoice_data):
        """Register vendor payments using the purchase_payment_method_ids mapping."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            vendor_payments = self._query_vendor_payments(cursor, purchase_invoice_id)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        _logger.info(
            f"PI {purchase_invoice_id}: fetched {len(vendor_payments)} vendor payment rows")

        mssql_posted = purchase_invoice_data.get('Posted', False)

        if mssql_posted and bill.state == 'draft':
            bill.action_post()

        if not vendor_payments:
            if not mssql_posted and bill.state == 'draft':
                _logger.info(f"PI {purchase_invoice_id}: no payments + not posted — bill stays draft")
            return

        if bill.state != 'posted':
            bill.action_post()

        mapping = {
            m.mssql_code: m.journal_id
            for m in self.purchase_payment_method_ids
            if m.journal_id
        }

        # Batch payments by (journal, date) to minimize wizard calls
        payment_batches = {}
        for payment_data in vendor_payments:
            payment_method = int(payment_data.get('PaymentMethod') or 0)
            net_amount = float(payment_data.get('NetAmount') or 0.0)
            if net_amount <= 0:
                continue
            journal = mapping.get(payment_method)
            if not journal:
                raise UserError(
                    f"PI {purchase_invoice_id}: Vendor payment method {payment_method} "
                    f"has no journal mapped. Configure it via "
                    f"'Fetch Purchase Payment Methods'."
                )

            payment_date = payment_data.get('PaymentDate') or purchase_invoice_data['InvoiceDate']
            if isinstance(payment_date, datetime):
                date_key = payment_date.date()
            else:
                date_key = payment_date

            ref_parts = [f"Vendor Payment - PI#{purchase_invoice_data.get('SupplierInvoiceID', purchase_invoice_id)}"]
            check_no = payment_data.get('CheckNo')
            if check_no:
                ref_parts.append(f"Check: {check_no}")
            note = payment_data.get('PaymentNote') or payment_data.get('InvoicePaymentNote')
            if note:
                ref_parts.append(note)
            ref = ' - '.join(ref_parts)

            payment_batches.setdefault((journal.id, str(date_key)), []).append({
                'amount': net_amount,
                'date': payment_date,
                'journal_id': journal.id,
                'communication': ref,
            })

        if not payment_batches:
            return

        for (journal_id, _date), batch in payment_batches.items():
            bill = self.env['account.move'].browse(bill.id)
            if bill.amount_residual <= 0:
                break
            for payment_vals in batch:
                try:
                    bill = self.env['account.move'].browse(bill.id)
                    if bill.amount_residual <= 0:
                        break
                    payment_register = self.env['account.payment.register'].with_context(
                        active_model='account.move',
                        active_ids=bill.ids,
                        dont_redirect_to_payments=True,
                    ).create({
                        'payment_date': payment_vals['date'],
                        'journal_id': payment_vals['journal_id'],
                        'amount': payment_vals['amount'],
                        'communication': payment_vals['communication'],
                        'group_payment': False,
                    })
                    payment_register.action_create_payments()
                except Exception as e:
                    if 'nothing left to pay' in str(e).lower():
                        break
                    _logger.warning(
                        f"PI {purchase_invoice_id}: vendor payment failed: {e}")
                    continue

        bill = self.env['account.move'].browse(bill.id)
        _logger.info(
            f"PI {purchase_invoice_id}: payments done (residual={bill.amount_residual})")

    # ── SQL Queries ───────────────────────────────────────────────────

    def _query_purchase_invoices(self, cursor, date_str, next_date):
        # ROUND() in T-SQL uses half-away-from-zero (e.g. 5820.035 → 5820.04),
        # which matches SA accounting conventions. Doing it server-side avoids
        # Python float precision loss on money(19,4) values like x.xx5.
        cursor.execute("""
            SELECT
                pi.PurchaseInvoiceID,
                pi.SupplierInvoiceID,
                pi.SupplierID,
                pi.BranchID,
                pi.InvoiceDate,
                pi.InvoiceDueDate,
                ROUND(pi.InvoiceTotal, 2) AS InvoiceTotal,
                ROUND(pi.NetTotal, 2)      AS NetTotal,
                ROUND(pi.TaxAmount, 2)     AS TaxAmount,
                ROUND(pi.Discount, 2)      AS Discount,
                ROUND(pi.PaidAmount, 2)    AS PaidAmount,
                ROUND(pi.DueAmount, 2)     AS DueAmount,
                pi.InvoiceNote,
                pi.IsReturn,
                pi.Posted,
                pi.PostedDate,
                pi.Closed,
                pi.Paid,
                s.SupplierName
            FROM [dbo].[tblPurchaseInvoice] pi
            LEFT JOIN [dbo].[tblSuppliers] s ON pi.SupplierID = s.SupplierID
            WHERE pi.InvoiceDate >= %s
               AND pi.InvoiceDate < %s
               AND pi.Posted = 1
            ORDER BY pi.InvoiceDate DESC, pi.PurchaseInvoiceID
        """, (date_str, next_date))
        return cursor.fetchall()

    def _query_vendor_payments(self, cursor, purchase_invoice_id):
        cursor.execute("""
            SELECT
                sp.PaymentID,
                sp.PaymentDate,
                sp.PaymentAmount,
                sp.DebitAmount,
                sp.CreditAmount,
                sp.PaymentMethod,
                sp.CheckNo,
                sp.CheckDate,
                sp.PaymentNote,
                sp.Posted,
                spi.NetAmount,
                spi.SupplierInvoiceID,
                spi.InvoicePaymentNote
            FROM [dbo].[tblSuppliersPayment] sp
            INNER JOIN [dbo].[tblSuppliersPaymentInvoice] spi
                ON sp.PaymentID = spi.PaymentID
            WHERE spi.PurchaseInvoiceID = %s
            ORDER BY sp.PaymentDate, sp.PaymentID
        """, (purchase_invoice_id,))
        return cursor.fetchall()
