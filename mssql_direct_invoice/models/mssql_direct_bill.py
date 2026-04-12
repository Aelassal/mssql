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

    # ── Entry Point ───────────────────────────────────────────────────

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

            if not purchase_invoices:
                conn.close()
                raise UserError(f'No purchase invoices found for date {date_str}')

            # Fetch all invoice details in bulk
            invoice_ids = [pi['PurchaseInvoiceID'] for pi in purchase_invoices]
            invoice_details = self._query_purchase_invoice_details(cursor, invoice_ids)
            conn.close()

            # Group details by invoice
            details_by_invoice = {}
            for detail in invoice_details:
                inv_id = detail['PurchaseInvoiceID']
                if inv_id not in details_by_invoice:
                    details_by_invoice[inv_id] = []
                details_by_invoice[inv_id].append(detail)

            # Pre-sync vendors if needed
            supplier_ids = list(set([pi['SupplierID'] for pi in purchase_invoices if pi['SupplierID']]))
            vendors = {
                p.x_sql_vendor_id: p for p in self.env['res.partner'].search([
                    ('x_sql_vendor_id', 'in', supplier_ids),
                    ('supplier_rank', '>', 0)
                ])
            }
            missing_supplier_ids = [sid for sid in supplier_ids if sid not in vendors]
            if missing_supplier_ids:
                _logger.info("Missing vendors detected. Auto-syncing...")
                try:
                    self.sync_vendors()
                except Exception as e:
                    _logger.error(f"Failed to auto-sync vendors: {str(e)}")

            # Idempotency: check existing bills by ref (MSSQL-PI-{id} format)
            existing_refs = set(
                self.env['account.move'].search([
                    ('ref', '!=', False),
                    ('move_type', 'in', ('in_invoice', 'in_refund')),
                ]).mapped('ref')
            )

            # Create queue
            queue = self.env['mssql.direct.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'purchase_bill',
                'sync_date': self.purchase_invoice_date,
            })

            line_vals_list = []
            for pi in purchase_invoices:
                inv_id = pi['PurchaseInvoiceID']

                # Skip already-synced (match MSSQL-PI-{id} format)
                if f"MSSQL-PI-{inv_id}" in existing_refs:
                    continue

                details = details_by_invoice.get(inv_id, [])
                if not details:
                    continue

                record_data = json.dumps({
                    'invoice': dict(pi),
                    'details': [dict(d) for d in details],
                }, default=str)

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
            _logger.info(f"Created queue {queue.name} with {len(line_vals_list)} lines")

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
            except:
                pass
            raise UserError(f'Purchase invoice sync failed: {str(e)}')

    # ── Queue Line Processor ─────────────────────────────────────────

    def _process_queue_purchase_bill(self, data, queue_line):
        """Process a single purchase invoice: create account.move (in_invoice) directly.

        Args:
            data: dict parsed from queue line's record_data JSON
            queue_line: mssql.direct.sync.queue.line record

        Returns:
            dict with 'model' and 'id' of created bill
        """
        purchase_invoice = data['invoice']
        invoice_lines = data['details']

        invoice_id = purchase_invoice['PurchaseInvoiceID']
        is_return = bool(purchase_invoice.get('IsReturn'))
        move_type = 'in_refund' if is_return else 'in_invoice'

        # Duplicate guard — skip if this bill was already imported
        existing_bill = self.env['account.move'].search([
            ('ref', '=', f"MSSQL-PI-{invoice_id}"),
            ('move_type', '=', move_type),
        ], limit=1)
        if existing_bill:
            _logger.info(f"Skipping PurchaseInvoice {invoice_id} — already imported as {existing_bill.name}")
            return {'model': 'account.move', 'id': existing_bill.id, 'skipped': True}

        supplier_id = int(purchase_invoice['SupplierID'])

        # Coerce numeric fields
        for line in invoice_lines:
            for key in ('ItemID',):
                if key in line and line[key] is not None:
                    try:
                        line[key] = int(line[key])
                    except (ValueError, TypeError):
                        pass
            for key in ('Quantity', 'UnitPrice', 'LineDiscount', 'SubTotal',
                        'SubNetTotal', 'CostPrice', 'LineTax',
                        'RecivedQuantity'):
                if key in line and line[key] is not None:
                    try:
                        line[key] = float(line[key])
                    except (ValueError, TypeError):
                        pass

        # Get vendor
        vendor = self.env['res.partner'].search([
            ('x_sql_vendor_id', '=', supplier_id),
            ('supplier_rank', '>', 0)
        ], limit=1)
        if not vendor:
            raise UserError(
                f'Vendor (SupplierID: {supplier_id}) not found. '
                f'Please run "Sync Vendors" first.')

        # Get products
        item_ids = list(set([d['ItemID'] for d in invoice_lines if d['ItemID']]))
        products = {
            p.x_sql_item_id: p for p in self.env['product.product'].search([
                ('x_sql_item_id', 'in', item_ids)
            ])
        }

        # Get purchase tax
        tax_15 = self.env['account.tax'].search([
            ('amount', '=', 15),
            ('type_tax_use', '=', 'purchase'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not tax_15:
            tax_15 = self.env['account.tax'].create({
                'name': 'VAT 15%',
                'amount': 15.0,
                'amount_type': 'percent',
                'type_tax_use': 'purchase',
                'company_id': self.env.company.id,
            })

        tax_id_tuple = [(6, 0, [tax_15.id])]

        # Parse invoice date
        inv_date = purchase_invoice['InvoiceDate']
        if isinstance(inv_date, str):
            inv_date = inv_date[:10]

        # Build bill lines — track MSSQL prices to force-write after creation
        bill_line_vals = []
        mssql_prices = []
        for line in invoice_lines:
            item_id = line['ItemID']
            product = products.get(item_id)

            if not product:
                product = self.env['product.product'].create({
                    'name': line['ItemName'] or line['EnglishName'] or f"Item {item_id}",
                    'x_sql_item_id': item_id,
                    'type': 'consu',
                    'is_storable': True,
                })
                products[item_id] = product

            quantity = float(line['Quantity'] or 0.0)
            original_price = float(line['UnitPrice'] or 0.0)
            line_discount = float(line['LineDiscount'] or 0.0)
            subtotal = float(line['SubTotal'] or 0.0)

            # For returns, MSSQL stores negative qty/subtotal — flip to positive
            if is_return:
                quantity = abs(quantity)
                subtotal = abs(subtotal)
                line_discount = abs(line_discount)

            if quantity == 0:
                continue

            if quantity != 0 and subtotal != 0:
                price_unit = subtotal / quantity
            else:
                price_unit = original_price

            discount_pct = 0.0
            if abs(subtotal) > 0 and abs(line_discount) > 0:
                discount_pct = round((abs(line_discount) / abs(subtotal)) * 100, 2)

            bill_line_vals.append((0, 0, {
                'product_id': product.id,
                'quantity': quantity,
                'price_unit': price_unit,
                'discount': discount_pct,
                'name': line['ItemName'] or line['EnglishName'] or product.name,
                'tax_ids': tax_id_tuple,
            }))
            mssql_prices.append(price_unit)

        if not bill_line_vals:
            raise UserError(f'No valid lines for Purchase Invoice {invoice_id}')

        # Get purchase journal
        purchase_journal = self.env['account.journal'].search([
            ('type', '=', 'purchase'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)
        if not purchase_journal:
            raise UserError('No purchase journal found.')

        inv_due_date = purchase_invoice.get('InvoiceDueDate') or inv_date
        if isinstance(inv_due_date, str):
            inv_due_date = inv_due_date[:10]

        # ── Create bill directly ──────────────────────────────────────
        bill = self.env['account.move'].create({
            'move_type': move_type,
            'partner_id': vendor.id,
            'invoice_date': inv_date,
            'invoice_date_due': inv_due_date,
            'date': inv_date,
            'ref': f"MSSQL-PI-{invoice_id}",
            'narration': purchase_invoice.get('InvoiceNote', ''),
            'journal_id': purchase_journal.id,
            'invoice_line_ids': bill_line_vals,
        })

        # Force MSSQL prices — Odoo's computed fields may override price_unit
        non_decimal_lines = bill.invoice_line_ids.filtered(
            lambda l: l.display_type == 'product'
        )
        for idx, bill_line in enumerate(non_decimal_lines):
            if idx < len(mssql_prices) and bill_line.price_unit != mssql_prices[idx]:
                bill_line.write({'price_unit': mssql_prices[idx]})

        # ── Decimal Adjustment ────────────────────────────────────────
        mssql_net_total = round(abs(float(purchase_invoice['NetTotal'] or 0)), 2)
        decimal_product = self._get_or_create_decimal_product()

        for attempt in range(1, 6):
            bill = self.env['account.move'].browse(bill.id)
            bill_total = round(bill.amount_total, 2)
            difference = round(mssql_net_total - bill_total, 2)

            if abs(difference) < 0.01:
                break

            pre_tax_adj = difference / 1.15
            decimal_line = bill.invoice_line_ids.filtered(
                lambda l: l.product_id.id == decimal_product.id
            )

            if decimal_line:
                decimal_line.write({'price_unit': decimal_line.price_unit + pre_tax_adj})
            else:
                bill.write({'invoice_line_ids': [(0, 0, {
                    'product_id': decimal_product.id,
                    'quantity': 1,
                    'price_unit': pre_tax_adj,
                    'name': 'Decimal Adjustment',
                    'tax_ids': tax_id_tuple,
                })]})

        # ── Post & register payments ─────────────────────────────────
        if is_return:
            # Post credit note directly
            if bill.state == 'draft':
                bill.action_post()
        else:
            self._register_vendor_payments(bill, invoice_id, purchase_invoice)

        doc_type = 'Credit Note' if is_return else 'Bill'
        _logger.info(f"Purchase Invoice {invoice_id}: {doc_type} {bill.name} created")
        return {'model': 'account.move', 'id': bill.id}

    # ── Vendor Payment Registration ───────────────────────────────────

    def _register_vendor_payments(self, bill, purchase_invoice_id, purchase_invoice_data):
        """Register vendor payments using account.payment.register wizard."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            vendor_payments = self._query_vendor_payments(cursor, purchase_invoice_id)
            conn.close()

            _logger.info(f"Fetched {len(vendor_payments)} vendor payment records for Purchase Invoice {purchase_invoice_id}")

            mssql_posted = purchase_invoice_data.get('Posted', False)

            # Post bill if MSSQL says posted
            if mssql_posted and bill.state == 'draft':
                bill.action_post()
                _logger.info(f"Posted bill {bill.id} because MSSQL invoice {purchase_invoice_id} has Posted=1")

            if not vendor_payments:
                if not mssql_posted and bill.state == 'draft':
                    _logger.info(f"No vendor payments and not posted - bill {bill.id} remains draft")
                return []

            # Ensure bill is posted before registering payments
            if bill.state != 'posted':
                bill.action_post()

            # Get payment journals
            cash_journal = self.env['account.journal'].search([
                ('type', '=', 'cash'),
                ('company_id', '=', self.env.company.id)
            ], limit=1)
            bank_journal = self.env['account.journal'].search([
                ('type', '=', 'bank'),
                ('company_id', '=', self.env.company.id)
            ], limit=1)

            if not cash_journal and not bank_journal:
                raise UserError('No cash or bank journal found.')

            payment_method_journal_map = {
                1: cash_journal,      # Cash
                2: bank_journal,      # Check
                3: bank_journal,      # Bank Transfer
            }

            # Group payments by journal + date for batch processing
            payment_batches = {}
            skipped_count = 0

            for payment_data in vendor_payments:
                payment_method = payment_data.get('PaymentMethod') or 1
                net_amount = float(payment_data.get('NetAmount') or 0.0)

                if net_amount <= 0:
                    skipped_count += 1
                    continue

                journal = payment_method_journal_map.get(payment_method, bank_journal or cash_journal)
                if not journal:
                    skipped_count += 1
                    continue

                payment_date = payment_data.get('PaymentDate') or purchase_invoice_data['InvoiceDate']

                payment_ref = f"Vendor Payment - PI#{purchase_invoice_data.get('SupplierInvoiceID', purchase_invoice_id)}"
                check_no = payment_data.get('CheckNo')
                if check_no:
                    payment_ref += f" - Check: {check_no}"
                payment_note = payment_data.get('PaymentNote') or payment_data.get('InvoicePaymentNote')
                if payment_note:
                    payment_ref += f" - {payment_note}"

                if isinstance(payment_date, datetime):
                    date_key = payment_date.date()
                else:
                    date_key = payment_date

                batch_key = (journal.id, str(date_key))
                if batch_key not in payment_batches:
                    payment_batches[batch_key] = []

                payment_batches[batch_key].append({
                    'amount': net_amount,
                    'date': payment_date,
                    'journal_id': journal.id,
                    'communication': payment_ref,
                })

            if not payment_batches:
                return []

            # Process each batch
            all_payment_ids = []
            for batch_key, batch_payments in payment_batches.items():
                journal_id, date_str = batch_key

                bill = self.env['account.move'].browse(bill.id)
                if bill.amount_residual <= 0:
                    break

                for payment_vals in batch_payments:
                    try:
                        bill = self.env['account.move'].browse(bill.id)
                        if bill.amount_residual <= 0:
                            break

                        payment_register = self.env['account.payment.register'].with_context(
                            active_model='account.move',
                            active_ids=bill.ids,
                            dont_redirect_to_payments=True
                        ).create({
                            'payment_date': payment_vals['date'],
                            'journal_id': payment_vals['journal_id'],
                            'amount': payment_vals['amount'],
                            'communication': payment_vals['communication'],
                            'group_payment': False,
                        })
                        payment_register.action_create_payments()
                        all_payment_ids.append(True)

                    except Exception as e:
                        error_msg = str(e)
                        if 'nothing left to pay' in error_msg.lower():
                            break
                        else:
                            _logger.warning(f"Failed to create vendor payment: {error_msg}")
                            continue

            bill = self.env['account.move'].browse(bill.id)
            _logger.info(f"Vendor payment registration complete: {len(all_payment_ids)} payments, Bill residual: {bill.amount_residual}")
            return all_payment_ids

        except Exception as e:
            try:
                conn.close()
            except:
                pass
            _logger.error(f"Vendor payment registration failed for bill {bill.id}: {str(e)}")
            return []

    # ── SQL Queries ───────────────────────────────────────────────────

    def _query_purchase_invoices(self, cursor, date_str, next_date):
        """Fetch purchase invoices from MSSQL for date range"""
        cursor.execute("""
            SELECT
                pi.PurchaseInvoiceID,
                pi.SupplierInvoiceID,
                pi.SupplierID,
                pi.BranchID,
                pi.InvoiceDate,
                pi.InvoiceDueDate,
                pi.InvoiceTotal,
                pi.NetTotal,
                pi.TaxAmount,
                pi.Discount,
                pi.PaidAmount,
                pi.DueAmount,
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

    def _query_purchase_invoice_details(self, cursor, invoice_ids):
        """Fetch purchase invoice details for given invoice IDs"""
        placeholders = ','.join(['%s'] * len(invoice_ids))
        cursor.execute(f"""
            SELECT
                pid.PurchaseInvoiceID,
                pid.PurchaseInvoiceDetailID,
                pid.ItemID,
                pid.ItemName,
                pid.EnglishName,
                pid.Quantity,
                pid.RecivedQuantity,
                pid.UnitPrice,
                pid.SubTotal,
                pid.TaxAmount as LineTax,
                pid.LineDiscount,
                pid.SubNetTotal,
                pid.CostPrice,
                pid.LineStatus
            FROM [dbo].[tblPurchaseInvoiceDetail] pid
            WHERE pid.PurchaseInvoiceID IN ({placeholders})
            ORDER BY pid.PurchaseInvoiceID, pid.PurchaseInvoiceDetailID
        """, invoice_ids)
        return cursor.fetchall()

    def _query_vendor_payments(self, cursor, purchase_invoice_id):
        """Fetch vendor payments for a purchase invoice"""
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
                sp.PayDiscount,
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
