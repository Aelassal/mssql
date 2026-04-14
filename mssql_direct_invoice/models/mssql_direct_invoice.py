from odoo import models, fields
from odoo.exceptions import UserError
from datetime import timedelta
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlDirectInvoice(models.Model):
    _inherit = 'mssql.direct.sync'

    # ── Sales Invoice Fields ──────────────────────────────────────────
    invoice_date = fields.Date(string='Invoice Date', default=fields.Date.today)

    payment_method_cash_journal_id = fields.Many2one('account.journal', string='Cash Journal',
                                                      domain=[('type', '=', 'cash')],
                                                      help='Journal for Cash payments (Payment Method 1).')
    payment_method_mada_journal_id = fields.Many2one('account.journal', string='Mada Journal',
                                                      domain=[('type', 'in', ['bank', 'cash'])],
                                                      help='Journal for Mada/Bank Card payments (Payment Method 2).')
    payment_method_visa_journal_id = fields.Many2one('account.journal', string='Visa Journal',
                                                      domain=[('type', 'in', ['bank', 'cash'])],
                                                      help='Journal for Visa payments (Payment Method 3).')
    payment_method_mastercard_journal_id = fields.Many2one('account.journal', string='MasterCard Journal',
                                                            domain=[('type', 'in', ['bank', 'cash'])],
                                                            help='Journal for MasterCard payments (Payment Method 4).')
    payment_method_coupon_journal_id = fields.Many2one('account.journal', string='Coupon Journal',
                                                        domain=[('type', 'in', ['bank', 'cash'])],
                                                        help='Journal for Coupon payments (Payment Method 20).')
    payment_method_stcpay_journal_id = fields.Many2one('account.journal', string='STC Pay Journal',
                                                        domain=[('type', 'in', ['bank', 'cash'])],
                                                        help='Journal for STC Pay payments (Payment Method 60).')
    payment_method_points_journal_id = fields.Many2one('account.journal', string='Points Journal',
                                                        domain=[('type', 'in', ['bank', 'cash'])],
                                                        help='Journal for Points payments (Payment Method 40).')
    payment_method_onaccount_journal_id = fields.Many2one('account.journal', string='On Account Journal',
                                                           domain=[('type', 'in', ['bank', 'cash'])],
                                                           help='Journal for On Account payments (Payment Method 6).')
    payment_method_banktransfer_journal_id = fields.Many2one('account.journal', string='Bank Transfer Journal',
                                                              domain=[('type', 'in', ['bank', 'cash'])],
                                                              help='Journal for Bank Transfer payments (Payment Method 70).')

    # ── Entry Point ───────────────────────────────────────────────────

    def action_create_invoice(self):
        """Create session-based invoices using the invoice_date field"""
        if not self.invoice_date:
            raise UserError('Please select an invoice date')
        return self.create_session_based_invoices(self.invoice_date)

    def create_session_based_invoices(self, invoice_date):
        """Create invoices directly (no SO) based on POS sessions for a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = invoice_date.strftime('%Y-%m-%d')
            next_date = (invoice_date + timedelta(days=1)).strftime('%Y-%m-%d')

            _logger.info("=" * 80)
            _logger.info(f"DIRECT INVOICE SYNC FOR DATE: {date_str}")
            _logger.info("=" * 80)

            # ── Phase 1: Fetch all data from MSSQL ────────────────────────
            sessions = self._query_sessions_for_date(cursor, date_str, next_date)
            if not sessions:
                conn.close()
                raise UserError(f'No POS sessions found for date {date_str}')

            session_ids = [s['SessionID'] for s in sessions]
            _logger.info(f"Found {len(sessions)} sessions for {date_str}")

            all_session_lines = self._query_all_session_lines(cursor, session_ids)
            all_credit_sales = self._query_all_session_credit_sales(cursor, session_ids)
            all_payments = self._query_all_session_payments(cursor, session_ids)
            all_invoice_ranges = self._query_all_session_invoice_ranges(cursor, session_ids)

            conn.close()
            _logger.info("Phase 1 complete. MSSQL connection closed.")

            # ── Phase 2: Create queue with lines ──────────────────────────
            _logger.info("Phase 2: Creating sync queue...")

            # Idempotency: check existing invoices by ref
            existing_refs = set(
                self.env['account.move'].search([
                    ('ref', '!=', False),
                    ('move_type', '=', 'out_invoice'),
                ]).mapped('ref')
            )

            queue = self.env['mssql.direct.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'sales_invoice',
                'sync_date': invoice_date,
            })

            line_vals_list = []
            skipped_existing = 0

            for session in sessions:
                session_id = session['SessionID']

                # Skip already-synced sessions (precise match)
                if any(ref.startswith(f"Session {session_id} -") for ref in existing_refs):
                    skipped_existing += 1
                    continue

                session_lines = all_session_lines.get(session_id, [])
                if not session_lines:
                    continue

                record_data = json.dumps({
                    'session': dict(session),
                    'lines': [dict(l) for l in session_lines],
                    'credit_sales': all_credit_sales.get(session_id, {}),
                    'payments': [dict(p) for p in all_payments.get(session_id, [])],
                    'invoice_range': all_invoice_ranges.get(session_id, {}),
                }, default=str)

                cashier_name = session['CashierName'] or f"Cashier {session['EmployeeID']}"
                line_vals_list.append({
                    'queue_id': queue.id,
                    'name': f"Session {session_id} - {cashier_name}",
                    'mssql_id': str(session_id),
                    'mssql_table': 'tblCashierActivity',
                    'record_data': record_data,
                })

            if skipped_existing:
                _logger.info(f"Skipped {skipped_existing} already-synced sessions")

            if not line_vals_list:
                queue.unlink()
                return self._success_notification(
                    'Sales Sync', 'No new sessions to process')

            self.env['mssql.direct.sync.queue.line'].create(line_vals_list)
            _logger.info(f"Created queue {queue.name} with {len(line_vals_list)} lines")

            # ── Phase 3: Process queue ────────────────────────────────────
            queue.action_process_queue()

            # ── Phase 4: Auto-import credit notes for the same date ──────
            try:
                self.create_sales_credit_notes(invoice_date)
            except Exception as e:
                _logger.warning(f"Auto credit note import for {date_str} failed: {e}")

            return {
                'type': 'ir.actions.act_window',
                'name': f'Sales Queue - {invoice_date}',
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
            raise UserError(f'Direct invoice creation failed: {str(e)}')

    # ── Queue Line Processor ─────────────────────────────────────────

    @staticmethod
    def _coerce_numeric(val):
        """Coerce a value to float if it's a numeric string."""
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return float(val)
            except (ValueError, TypeError):
                return val
        return val

    def _process_queue_sales_invoice(self, data, queue_line):
        """Process a single sales session: create account.move (out_invoice) directly.

        Args:
            data: dict parsed from queue line's record_data JSON
            queue_line: mssql.direct.sync.queue.line record

        Returns:
            dict with 'model' and 'id' of created invoice
        """
        session = data['session']
        session_lines = data['lines']
        session_payments = data.get('payments', [])
        invoice_range = data.get('invoice_range', {})

        # Duplicate guard — skip if this session was already imported
        session_id_check = session['SessionID']
        existing_inv = self.env['account.move'].search([
            ('ref', '=like', f"Session {session_id_check} -%"),
            ('move_type', '=', 'out_invoice'),
        ], limit=1)
        if existing_inv:
            _logger.info(f"Skipping Session {session_id_check} — already imported as {existing_inv.name}")
            return {'model': 'account.move', 'id': existing_inv.id, 'skipped': True}

        # Parse credit sales data
        credit_sales = data.get('credit_sales', {})
        credit_amount = self._coerce_numeric(credit_sales.get('total')) or 0

        session_id = session['SessionID']
        cashier_name = session.get('CashierName') or f"Cashier {session.get('EmployeeID', '?')}"
        net_total = self._coerce_numeric(session['NetTotal'])

        # Coerce numeric fields in session lines
        for line in session_lines:
            for key in ('ItemID', 'AvgPrice', 'TotalQuantity', 'TotalDiscount',
                        'SubTotal', 'Quantity', 'UnitPrice'):
                if key in line:
                    line[key] = self._coerce_numeric(line[key])
            # Keep ItemID as string (Char field on product.product)
            if line.get('ItemID') is not None:
                line['ItemID'] = str(line['ItemID'])

        # Coerce numeric fields in payments
        for pay in session_payments:
            for key in ('PaymentAmount', 'Amount', 'PaymentType'):
                if key in pay:
                    pay[key] = self._coerce_numeric(pay[key])

        # Parse session date
        session_date_raw = session.get('SessionDate')
        if isinstance(session_date_raw, str):
            from datetime import date as date_type
            session_date = date_type.fromisoformat(session_date_raw[:10])
        elif hasattr(session_date_raw, 'date'):
            session_date = session_date_raw.date()
        else:
            session_date = session_date_raw

        _logger.info(f"Processing Session {session_id} - {cashier_name} "
                     f"(NetTotal: {net_total}, Date: {session_date})")

        # ── Prepare products ──────────────────────────────────────────
        all_item_ids = set()
        item_info = {}
        for line in session_lines:
            item_id = line['ItemID']
            if item_id:
                item_id = str(item_id)
                line['ItemID'] = item_id
                all_item_ids.add(item_id)
                if item_id not in item_info:
                    item_info[item_id] = {
                        'name': line.get('ItemName') or line.get('EnglishName') or f"Item {item_id}",
                    }

        product_obj = self.env['product.product']
        existing_records = product_obj.search([
            ('x_sql_item_id', 'in', list(all_item_ids))
        ])
        existing_products = {p.x_sql_item_id: p for p in existing_records}

        # Create missing products
        missing = all_item_ids - set(existing_products.keys())
        if missing:
            new_prods = product_obj.create([{
                'name': item_info[iid]['name'],
                'x_sql_item_id': iid,
                'type': 'consu',
            } for iid in missing])
            for p in new_prods:
                existing_products[p.x_sql_item_id] = p

        # ── Prepare tax ──────────────────────────────────────────────
        tax_15 = self.env['account.tax'].search([
            ('type_tax_use', '=', 'sale'),
            ('amount', '=', 15.0),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not tax_15:
            tax_15 = self.env['account.tax'].create({
                'name': 'VAT 15%',
                'amount': 15.0,
                'amount_type': 'percent',
                'type_tax_use': 'sale',
                'company_id': self.env.company.id,
            })

        # ── Prepare journals ──────────────────────────────────────────
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

        payment_journal_map = {
            1: self.payment_method_cash_journal_id or cash_journal,
            2: self.payment_method_mada_journal_id or bank_journal,
            3: self.payment_method_visa_journal_id or bank_journal,
            4: self.payment_method_mastercard_journal_id or bank_journal,
            5: cash_journal,
            6: self.payment_method_onaccount_journal_id or bank_journal,
            10: cash_journal,
            20: self.payment_method_coupon_journal_id or cash_journal,
            30: cash_journal,
            40: self.payment_method_points_journal_id or cash_journal,
            60: self.payment_method_stcpay_journal_id or bank_journal,
            70: self.payment_method_banktransfer_journal_id or bank_journal,
        }

        # ── Get/create customer partner ───────────────────────────────
        partner_obj = self.env['res.partner']
        customer_name = '\u0639\u0645\u064a\u0644 \u0646\u0642\u062f\u064a'  # عميل نقدي
        partner = partner_obj.search([('name', '=', customer_name)], limit=1)
        if not partner:
            partner = partner_obj.create({
                'name': customer_name,
                'customer_rank': 1,
            })

        # ── Build invoice lines ───────────────────────────────────────
        invoice_line_vals = []
        tax_id_tuple = [(6, 0, [tax_15.id])]

        for line in session_lines:
            item_id = line['ItemID']
            if not item_id:
                continue

            product = existing_products.get(item_id)
            if not product:
                _logger.warning(f"Product not found for ItemID {item_id}, skipping line")
                continue

            avg_price = float(line['AvgPrice'] or 0)
            quantity = float(line['TotalQuantity'] or 0)
            total_discount = float(line['TotalDiscount'] or 0)
            subtotal = float(line['SubTotal'] or 0)

            if quantity <= 0:
                continue

            # Calculate discount percentage
            original_amount = avg_price * quantity
            discount_pct = 0.0
            if original_amount > 0 and total_discount > 0:
                discount_pct = round((total_discount / original_amount) * 100, 2)

            # Back-calculate price_unit from SubTotal
            # SubTotal = price_unit * qty * (1 - discount%) * 1.15
            discount_factor = (100 - discount_pct) / 100
            if quantity > 0 and discount_factor > 0 and subtotal > 0:
                price_unit = subtotal / (quantity * discount_factor * 1.15)
            else:
                price_unit = avg_price

            invoice_line_vals.append((0, 0, {
                'product_id': product.id,
                'quantity': quantity,
                'price_unit': price_unit,
                'discount': discount_pct,
                'name': product.name,
                'tax_ids': tax_id_tuple,
            }))

        if not invoice_line_vals:
            raise UserError(f'No invoice lines generated for session {session_id}')

        # ── Build reference ───────────────────────────────────────────
        min_inv_id = invoice_range.get('MinInvoiceID', '')
        max_inv_id = invoice_range.get('MaxInvoiceID', '')
        ref_text = f"Session {session_id} - {cashier_name} - invs {min_inv_id} to {max_inv_id}"

        # ── Get sale journal ──────────────────────────────────────────
        sale_journal = self.env['account.journal'].search([
            ('type', '=', 'sale'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)
        if not sale_journal:
            raise UserError('No sales journal found.')

        # ── Create invoice directly ───────────────────────────────────
        invoice = self.env['account.move'].create({
            'move_type': 'out_invoice',
            'partner_id': partner.id,
            'invoice_date': session_date,
            'date': session_date,
            'ref': ref_text,
            'journal_id': sale_journal.id,
            'invoice_line_ids': invoice_line_vals,
        })

        # ── Decimal Adjustment ────────────────────────────────────────
        mssql_total = round(float(net_total) + credit_amount, 2)
        decimal_product = self._get_or_create_decimal_product()

        for attempt in range(1, 6):
            invoice = self.env['account.move'].browse(invoice.id)
            inv_total = round(invoice.amount_total, 2)
            difference = round(mssql_total - inv_total, 2)

            if abs(difference) < 0.01:
                break

            pre_tax_adj = difference / 1.15
            decimal_line = invoice.invoice_line_ids.filtered(
                lambda l: l.product_id.id == decimal_product.id
            )

            if decimal_line:
                decimal_line.write({'price_unit': decimal_line.price_unit + pre_tax_adj})
            else:
                invoice.write({'invoice_line_ids': [(0, 0, {
                    'product_id': decimal_product.id,
                    'quantity': 1,
                    'price_unit': pre_tax_adj,
                    'name': 'Decimal Adjustment',
                    'tax_ids': tax_id_tuple,
                })]})

        # ── Add narration for credit sales (unpaid invoices) ─────────
        if credit_amount > 0:
            credit_invoices = credit_sales.get('invoices', [])
            note_lines = [f"Credit sales (unpaid) — {credit_amount:.2f} SAR:"]
            for cinv in credit_invoices:
                inv_id = cinv.get('InvoiceID', '?')
                cust_name = cinv.get('CustomerName') or '?'
                phone = cinv.get('PhoneNo') or ''
                inv_total = self._coerce_numeric(cinv.get('NetTotal')) or 0
                phone_part = f" ({phone})" if phone else ""
                note_lines.append(
                    f"  Invoice {inv_id} | Customer: {cust_name}{phone_part} | {inv_total:.2f} SAR"
                )
                for prod in cinv.get('products', []):
                    p_name = prod.get('ItemName') or f"Item {prod.get('ItemID', '?')}"
                    p_qty = self._coerce_numeric(prod.get('Quantity')) or 0
                    p_price = self._coerce_numeric(prod.get('UnitPrice')) or 0
                    p_sub = self._coerce_numeric(prod.get('SubTotal')) or 0
                    note_lines.append(
                        f"    - {p_name} qty={p_qty:g} @ {p_price:.2f} = {p_sub:.2f}"
                    )
            invoice.write({'narration': '\n'.join(note_lines)})
            _logger.info(f"Session {session_id}: Credit sales {credit_amount:.2f} SAR "
                         f"({len(credit_invoices)} invoice(s)) noted in narration")

        # ── Post invoice ──────────────────────────────────────────────
        invoice.action_post()

        # ── Register payments ─────────────────────────────────────────
        if session_payments:
            self._register_customer_payments(
                invoice, session_payments, session_date,
                payment_journal_map, cash_journal, bank_journal
            )

            # Post cash differences (shortage/surplus)
            try:
                self._post_session_cash_differences(
                    session_payments, session_date,
                    payment_journal_map, cash_journal, bank_journal,
                    session_id
                )
            except Exception as e:
                _logger.warning(f"Session {session_id}: Failed to post cash differences: {e}")

        _logger.info(f"Session {session_id}: Invoice {invoice.name} created successfully")
        return {'model': 'account.move', 'id': invoice.id}

    # ── Customer Payment Registration ─────────────────────────────────

    def _register_customer_payments(self, invoice, session_payments, invoice_date,
                                     payment_journal_map, cash_journal, bank_journal):
        """Register payments for a session invoice using account.payment.register wizard."""
        if not session_payments:
            return []

        if invoice.state != 'posted':
            invoice.action_post()

        payment_method_names = {
            1: 'Cash', 2: 'Mada', 3: 'Visa', 4: 'MasterCard', 5: 'Return Voucher',
            10: 'Donation', 20: 'Coupon', 30: 'Ports', 40: 'Points', 60: 'STC Pay',
        }

        all_payment_ids = []

        for payment in session_payments:
            payment_type = payment['PaymentType']
            amount = round(float(payment['Amount'] or 0), 2)
            if amount <= 0:
                continue

            journal = payment_journal_map.get(payment_type)
            if not journal:
                journal = cash_journal or bank_journal

            # Check if invoice still has residual
            invoice = self.env['account.move'].browse(invoice.id)
            if invoice.amount_residual <= 0:
                break

            try:
                method_name = payment_method_names.get(payment_type, f'Method {payment_type}')
                payment_ref = f"{payment.get('PaymentMethodName') or method_name} - Session Payment"

                payment_register = self.env['account.payment.register'].with_context(
                    active_model='account.move',
                    active_ids=invoice.ids,
                    dont_redirect_to_payments=True
                ).create({
                    'payment_date': invoice_date,
                    'journal_id': journal.id,
                    'amount': amount,
                    'communication': payment_ref,
                    'group_payment': False,
                })
                payment_register.action_create_payments()
                all_payment_ids.append(True)

            except Exception as e:
                error_msg = str(e)
                if 'nothing left to pay' in error_msg.lower():
                    break
                else:
                    _logger.error(f"Failed to create payment: {error_msg}")

        return all_payment_ids

    # ── Cash Differences ────────────────────────────────────────────

    def _post_session_cash_differences(self, session_payments, session_date,
                                       payment_journal_map, cash_journal, bank_journal,
                                       session_id):
        """Create bank statement lines for cash differences (like Odoo POS).

        For each payment type where DifAmount != 0, creates an
        account.bank.statement.line with counterpart_account_id set to
        the journal's loss or profit account.
        """
        for payment in session_payments:
            dif_amount = self._coerce_numeric(payment.get('DifAmount')) or 0
            if abs(dif_amount) < 0.01:
                continue

            payment_type = payment.get('PaymentType') or 1
            journal = payment_journal_map.get(payment_type, cash_journal or bank_journal)
            if not journal:
                continue

            # DifAmount = PCAmount - ActualAmount in MSSQL
            # Positive = shortage (actual < system) -> loss
            # Negative = surplus (actual > system) -> profit
            if dif_amount > 0:  # Shortage
                if not journal.loss_account_id:
                    _logger.warning(f"Session {session_id}: No loss account on journal "
                                    f"{journal.name}, skipping difference {dif_amount}")
                    continue
                counterpart = journal.loss_account_id.id
                ref = (f"Cash shortage - Session {session_id} - "
                       f"{payment.get('PaymentMethodName', '')}")
                stmt_amount = -dif_amount
            else:  # Surplus
                if not journal.profit_account_id:
                    _logger.warning(f"Session {session_id}: No profit account on journal "
                                    f"{journal.name}, skipping difference {dif_amount}")
                    continue
                counterpart = journal.profit_account_id.id
                ref = (f"Cash surplus - Session {session_id} - "
                       f"{payment.get('PaymentMethodName', '')}")
                stmt_amount = -dif_amount

            note = payment.get('DiffNote')
            if note:
                ref += f" ({note})"

            self.env['account.bank.statement.line'].create({
                'journal_id': journal.id,
                'amount': stmt_amount,
                'date': session_date,
                'payment_ref': ref,
                'counterpart_account_id': counterpart,
            })
            _logger.info(f"Session {session_id}: Posted difference {dif_amount} "
                         f"(stmt_amount={stmt_amount}) for "
                         f"{payment.get('PaymentMethodName', '')} to "
                         f"{'loss' if dif_amount > 0 else 'profit'} account")

    # ── SQL Queries ───────────────────────────────────────────────────

    def _query_sessions_for_date(self, cursor, date_str, next_date):
        """Fetch all POS sessions for a given date"""
        cursor.execute("""
            SELECT
                ca.SessionID,
                ca.SessionDate,
                ca.EmployeeID,
                e.EmployeeName AS CashierName,
                ca.InvoiceCount,
                ca.SalesInvoiceCount,
                ca.ReturnInvoiceCount,
                ca.LineCount,
                ca.ItemsCount,
                ROUND(ca.NetTotal, 2) AS NetTotal,
                ROUND(ca.ActualAmount, 2) AS ActualAmount,
                ROUND(ca.NetTotalDiff, 2) AS NetTotalDiff,
                ca.CashierClosed,
                ca.SessionClosed
            FROM [dbo].[tblCashierActivity] ca
            LEFT JOIN [dbo].[tblEmployeesInfo] e ON ca.EmployeeID = e.EmployeeID
            WHERE ca.SessionDate >= %s AND ca.SessionDate < %s
            ORDER BY ca.SessionID
        """, (date_str, next_date))
        return cursor.fetchall()

    def _query_all_session_lines(self, cursor, session_ids):
        """Fetch aggregated invoice lines for ALL sessions in one query"""
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                id.ItemID,
                id.ItemName,
                id.EnglishName,
                id.BarCode,
                id.UnitName,
                ROUND(SUM(id.Quantity), 2) AS TotalQuantity,
                COUNT(*) AS TimesSold,
                AVG(id.UnitPrice) AS AvgPrice,
                ROUND(SUM(id.SubTotal), 2) AS SubTotal,
                ROUND(SUM(id.LineDiscount), 2) AS TotalDiscount
            FROM [dbo].[tblInvoice] i
            INNER JOIN [dbo].[tblInvoiceDetail] id ON i.InvoiceID = id.InvoiceID
            WHERE i.SessionID IN ({placeholders})
              AND i.IsReturned = 0
            GROUP BY
                i.SessionID,
                id.ItemID,
                id.ItemName,
                id.EnglishName,
                id.BarCode,
                id.UnitName
            ORDER BY i.SessionID, id.ItemID
        """, session_ids)

        results = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            if session_id not in results:
                results[session_id] = []
            results[session_id].append(row)

        return results

    def _query_all_session_payments(self, cursor, session_ids):
        """Fetch payment details for ALL sessions in one query"""
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                ca.SessionID,
                cad.PaymentType,
                pt.PaymentType AS PaymentMethodName,
                ROUND(cad.ActualAmount, 2) AS Amount,
                ROUND(cad.DifAmount, 2) AS DifAmount,
                cad.DiffNote
            FROM [dbo].[tblCashierActivityDetail] cad
            INNER JOIN [dbo].[tblCashierActivity] ca ON cad.SessionID = ca.SessionID
            LEFT JOIN [dbo].[tblPaymentType] pt ON cad.PaymentType = pt.PaymentTypeID
            WHERE ca.SessionID IN ({placeholders})
              AND (cad.ActualAmount > 0 OR cad.DifAmount != 0)
              AND cad.PaymentType != 5
            ORDER BY ca.SessionID, cad.PaymentType
        """, session_ids)

        results = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            if session_id not in results:
                results[session_id] = []
            results[session_id].append(row)

        return results

    def _query_all_session_invoice_ranges(self, cursor, session_ids):
        """Fetch invoice ID range (min/max) for ALL sessions in one query"""
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                MIN(i.InvoiceID) AS MinInvoiceID,
                MAX(i.InvoiceID) AS MaxInvoiceID,
                COUNT(i.InvoiceID) AS InvoiceCount
            FROM [dbo].[tblInvoice] i
            WHERE i.SessionID IN ({placeholders})
            GROUP BY i.SessionID
        """, session_ids)

        return {row['SessionID']: {
            'MinInvoiceID': row['MinInvoiceID'],
            'MaxInvoiceID': row['MaxInvoiceID'],
            'InvoiceCount': row['InvoiceCount']
        } for row in cursor.fetchall()}

    def _query_all_session_credit_sales(self, cursor, session_ids):
        """Fetch credit sale invoices (unpaid) for all sessions in one query.

        A credit sale is an invoice where IsReturned=0, NetTotal > 0, but all
        payment method columns are zero — the customer took products without paying.
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                i.InvoiceID,
                i.CustomerName,
                i.PhoneNo,
                ROUND(i.NetTotal, 2) AS NetTotal,
                id.ItemID,
                id.ItemName,
                id.Quantity,
                id.UnitPrice,
                ROUND(id.SubTotal, 2) AS SubTotal
            FROM [dbo].[tblInvoice] i
            INNER JOIN [dbo].[tblInvoiceDetail] id ON i.InvoiceID = id.InvoiceID
            WHERE i.SessionID IN ({placeholders})
              AND i.IsReturned = 0
              AND i.NetTotal > 0
              AND ISNULL(i.CashAmount, 0) = 0
              AND ISNULL(i.SpanAmount, 0) = 0
              AND ISNULL(i.CreditCardAmount, 0) = 0
              AND ISNULL(i.VisaAmount, 0) = 0
              AND ISNULL(i.MasterCard, 0) = 0
              AND ISNULL(i.CheckAmount, 0) = 0
              AND ISNULL(i.ReturnAmount, 0) = 0
              AND ISNULL(i.ReturnSlip, 0) = 0
            ORDER BY i.SessionID, i.InvoiceID
        """, session_ids)

        # Group by SessionID -> InvoiceID -> detail lines
        raw = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            invoice_id = row['InvoiceID']

            session_data = raw.setdefault(session_id, {})
            if invoice_id not in session_data:
                session_data[invoice_id] = {
                    'InvoiceID': invoice_id,
                    'CustomerName': row['CustomerName'],
                    'PhoneNo': row['PhoneNo'],
                    'NetTotal': float(row['NetTotal']),
                    'products': [],
                }

            if row.get('ItemID'):
                session_data[invoice_id]['products'].append({
                    'ItemID': row['ItemID'],
                    'ItemName': row['ItemName'],
                    'Quantity': float(row['Quantity']) if row['Quantity'] else 0,
                    'UnitPrice': float(row['UnitPrice']) if row['UnitPrice'] else 0,
                    'SubTotal': float(row['SubTotal']) if row['SubTotal'] else 0,
                })

        # Build final structure with total per session
        results = {}
        for session_id, invoices_dict in raw.items():
            invoices_list = list(invoices_dict.values())
            results[session_id] = {
                'total': round(sum(inv['NetTotal'] for inv in invoices_list), 2),
                'invoices': invoices_list,
            }

        return results

    # ── Sales Credit Notes (from tblZatcaCreditNote) ────────────────

    def create_sales_credit_notes(self, credit_note_date):
        """Create credit notes from tblZatcaCreditNote for a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = credit_note_date.strftime('%Y-%m-%d')
            next_date = (credit_note_date + timedelta(days=1)).strftime('%Y-%m-%d')

            _logger.info("=" * 80)
            _logger.info(f"DIRECT CREDIT NOTE SYNC FOR DATE: {date_str}")
            _logger.info("=" * 80)

            # Fetch all credit notes + detail lines from MSSQL
            credit_notes = self._query_zatca_credit_notes(cursor, date_str, next_date)
            if not credit_notes:
                conn.close()
                raise UserError(f'No credit notes found for date {date_str}')

            # Fetch original invoice SessionIDs for linking
            original_invoice_ids = list(set(
                cn['ReturnInvoiceID'] for cn in credit_notes if cn.get('ReturnInvoiceID')
            ))
            original_sessions = {}
            if original_invoice_ids:
                original_sessions = self._query_original_invoice_sessions(
                    cursor, original_invoice_ids)

            conn.close()

            # Idempotency: check existing credit notes by ref
            existing_refs = set(
                self.env['account.move'].search([
                    ('ref', '!=', False),
                    ('move_type', '=', 'out_refund'),
                ]).mapped('ref')
            )

            # Create queue
            queue = self.env['mssql.direct.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'sales_credit_note',
                'sync_date': credit_note_date,
            })

            line_vals_list = []
            for cn in credit_notes:
                cn_invoice_id = cn['InvoiceID']
                ref = f"MSSQL-CN-{cn_invoice_id}"

                if ref in existing_refs:
                    continue

                # Enrich with original session ID
                orig_inv_id = cn.get('ReturnInvoiceID')
                cn['OriginalSessionID'] = original_sessions.get(orig_inv_id)

                record_data = json.dumps(cn, default=str)

                customer_name = cn.get('CustomerName') or 'Credit Note'
                line_vals_list.append({
                    'queue_id': queue.id,
                    'name': f"CN {cn_invoice_id} - {customer_name}",
                    'mssql_id': str(cn_invoice_id),
                    'mssql_table': 'tblZatcaCreditNote',
                    'record_data': record_data,
                })

            if not line_vals_list:
                queue.unlink()
                return self._success_notification(
                    'Credit Note Sync', 'No new credit notes to process')

            self.env['mssql.direct.sync.queue.line'].create(line_vals_list)
            _logger.info(f"Created queue {queue.name} with {len(line_vals_list)} credit note lines")

            queue.action_process_queue()

            return {
                'type': 'ir.actions.act_window',
                'name': f'Credit Note Queue - {credit_note_date}',
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
            raise UserError(f'Credit note sync failed: {str(e)}')

    def _process_queue_sales_credit_note(self, data, queue_line):
        """Process a single ZatcaCreditNote: create out_refund directly.

        Args:
            data: dict parsed from queue line's record_data JSON
            queue_line: mssql.direct.sync.queue.line record

        Returns:
            dict with 'model' and 'id' of created credit note
        """
        cn_invoice_id = data['InvoiceID']
        ref = f"MSSQL-CN-{cn_invoice_id}"

        # Duplicate guard
        existing_cn = self.env['account.move'].search([
            ('ref', '=', ref),
            ('move_type', '=', 'out_refund'),
        ], limit=1)
        if existing_cn:
            _logger.info(f"Skipping CreditNote {cn_invoice_id} — already imported as {existing_cn.name}")
            return {'model': 'account.move', 'id': existing_cn.id, 'skipped': True}

        original_invoice_id = data.get('ReturnInvoiceID')
        original_session_id = data.get('OriginalSessionID')
        detail_lines = data.get('detail_lines', [])
        net_total = self._coerce_numeric(data.get('NetTotal')) or 0

        # Parse credit note date
        cn_date_raw = data.get('InvoiceDate')
        if isinstance(cn_date_raw, str):
            from datetime import date as date_type
            cn_date = date_type.fromisoformat(cn_date_raw[:10])
        elif hasattr(cn_date_raw, 'date'):
            cn_date = cn_date_raw.date()
        else:
            cn_date = cn_date_raw

        # Coerce detail lines
        for dl in detail_lines:
            for k in ('Quantity', 'UnitPrice', 'SubTotal', 'TaxAmount', 'TaxPercent'):
                if k in dl:
                    dl[k] = self._coerce_numeric(dl.get(k)) or 0
            # Keep ItemID as string (Char field on product.product)
            if dl.get('ItemID') is not None:
                dl['ItemID'] = str(dl['ItemID'])

        # Get/create partner (use generic cash customer)
        partner_obj = self.env['res.partner']
        customer_name = '\u0639\u0645\u064a\u0644 \u0646\u0642\u062f\u064a'  # عميل نقدي
        partner = partner_obj.search([('name', '=', customer_name)], limit=1)
        if not partner:
            partner = partner_obj.create({'name': customer_name, 'customer_rank': 1})

        # Get 15% sale tax
        tax_15 = self.env['account.tax'].search([
            ('type_tax_use', '=', 'sale'),
            ('amount', '=', 15.0),
            ('company_id', '=', self.env.company.id)
        ], limit=1)
        if not tax_15:
            tax_15 = self.env['account.tax'].create({
                'name': 'VAT 15%', 'amount': 15.0, 'amount_type': 'percent',
                'type_tax_use': 'sale', 'company_id': self.env.company.id,
            })
        tax_id_tuple = [(6, 0, [tax_15.id])]

        # Build credit note lines from ZATCA detail
        product_obj = self.env['product.product']
        return_product = self._get_or_create_return_product()
        cn_line_vals = []

        if detail_lines:
            # Bulk product lookup
            all_item_ids = set()
            for dl in detail_lines:
                if dl.get('ItemID'):
                    all_item_ids.add(str(dl['ItemID']))

            cn_products = {}
            if all_item_ids:
                existing = product_obj.search([('x_sql_item_id', 'in', list(all_item_ids))])
                cn_products = {p.x_sql_item_id: p for p in existing}

                missing = all_item_ids - set(cn_products.keys())
                if missing:
                    new_prods = product_obj.create([{
                        'name': dl.get('ItemName') or f"Item {iid}",
                        'x_sql_item_id': iid,
                        'type': 'consu',
                    } for iid in missing for dl in detail_lines if dl.get('ItemID') and str(dl['ItemID']) == iid][:len(missing)])
                    for p in new_prods:
                        cn_products[p.x_sql_item_id] = p

            for dl in detail_lines:
                item_id = str(dl.get('ItemID')) if dl.get('ItemID') else None
                product = cn_products.get(item_id) if item_id else return_product
                if not product:
                    product = return_product

                cn_line_vals.append((0, 0, {
                    'product_id': product.id,
                    'quantity': abs(dl.get('Quantity', 1)),
                    'price_unit': dl.get('UnitPrice', 0),
                    'tax_ids': tax_id_tuple,
                }))
        else:
            # No detail lines — single Return service line
            pre_tax = net_total / 1.15
            cn_line_vals.append((0, 0, {
                'product_id': return_product.id,
                'quantity': 1,
                'price_unit': pre_tax,
                'tax_ids': tax_id_tuple,
            }))

        if not cn_line_vals:
            raise UserError(f'No lines for credit note {cn_invoice_id}')

        # Get sale journal
        sale_journal = self.env['account.journal'].search([
            ('type', '=', 'sale'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)
        if not sale_journal:
            raise UserError('No sales journal found.')

        # Find original Odoo invoice for linking
        original_odoo_invoice = False
        if original_session_id:
            original_odoo_invoice = self.env['account.move'].search([
                ('ref', '=like', f'Session {original_session_id} -%'),
                ('move_type', '=', 'out_invoice'),
                ('state', '=', 'posted'),
            ], limit=1)

        # Create credit note
        cn_vals = {
            'move_type': 'out_refund',
            'partner_id': partner.id,
            'invoice_date': cn_date,
            'date': cn_date,
            'ref': ref,
            'journal_id': sale_journal.id,
            'invoice_line_ids': cn_line_vals,
        }
        if original_odoo_invoice:
            cn_vals['reversed_entry_id'] = original_odoo_invoice.id

        credit_note = self.env['account.move'].create(cn_vals)

        # Decimal adjustment
        mssql_net_total = round(abs(net_total), 2)
        decimal_product = self._get_or_create_decimal_product()

        for attempt in range(1, 6):
            credit_note = self.env['account.move'].browse(credit_note.id)
            cn_total = round(credit_note.amount_total, 2)
            difference = round(mssql_net_total - cn_total, 2)

            if abs(difference) < 0.01:
                break

            pre_tax_adj = difference / 1.15
            decimal_line = credit_note.invoice_line_ids.filtered(
                lambda l: l.product_id.id == decimal_product.id
            )
            if decimal_line:
                decimal_line.write({'price_unit': decimal_line.price_unit + pre_tax_adj})
            else:
                credit_note.write({'invoice_line_ids': [(0, 0, {
                    'product_id': decimal_product.id,
                    'quantity': 1,
                    'price_unit': pre_tax_adj,
                    'name': 'Decimal Adjustment',
                    'tax_ids': tax_id_tuple,
                })]})

        # Post credit note
        credit_note.action_post()

        # Reconcile with original session invoice
        if original_odoo_invoice:
            try:
                lines_to_reconcile = (credit_note + original_odoo_invoice).line_ids.filtered(
                    lambda l: l.account_id.account_type == 'asset_receivable' and not l.reconciled
                )
                if lines_to_reconcile:
                    lines_to_reconcile.reconcile()
                    _logger.info(f"Reconciled CN {credit_note.name} with invoice {original_odoo_invoice.name}")
            except Exception as e:
                _logger.warning(f"Failed to reconcile CN {credit_note.name}: {e}")

        _logger.info(f"Credit Note {cn_invoice_id}: {credit_note.name} created (amount: {credit_note.amount_total})")
        return {'model': 'account.move', 'id': credit_note.id}

    # ── Credit Note SQL Queries ───────────────────────────────────────

    def _query_zatca_credit_notes(self, cursor, date_str, next_date):
        """Fetch all credit notes with detail lines from tblZatcaCreditNote for a date range.

        Returns:
            List of dicts, each with credit note header fields + 'detail_lines' list
        """
        cursor.execute("""
            SELECT
                zcn.InvoiceID,
                zcn.ReturnInvoiceID,
                zcn.SessionID,
                zcn.CustomerID,
                zcn.CustomerName,
                zcn.PhoneNo,
                ROUND(zcn.Total, 2) AS Total,
                ROUND(zcn.TaxAmountTotal, 2) AS TaxAmountTotal,
                ROUND(zcn.NetTotal, 2) AS NetTotal,
                zcn.InvoiceDate,
                zd.ItemID,
                zd.ItemName,
                zd.LineNumber,
                zd.Quantity,
                ROUND(zd.UnitPrice, 2) AS UnitPrice,
                ROUND(zd.SubTotal, 2) AS SubTotal,
                ROUND(zd.TaxAmount, 2) AS TaxAmount,
                zd.TaxPercent
            FROM [dbo].[tblZatcaCreditNote] zcn
            LEFT JOIN [dbo].[tblZatcaCreditNoteDetail] zd ON zcn.InvoiceID = zd.InvoiceID
            WHERE zcn.InvoiceDate >= %s AND zcn.InvoiceDate < %s
            ORDER BY zcn.InvoiceID, zd.LineNumber
        """, (date_str, next_date))

        raw = {}
        for row in cursor.fetchall():
            inv_id = row['InvoiceID']
            if inv_id not in raw:
                raw[inv_id] = {
                    'InvoiceID': inv_id,
                    'ReturnInvoiceID': row['ReturnInvoiceID'],
                    'SessionID': row['SessionID'],
                    'CustomerID': row['CustomerID'],
                    'CustomerName': row['CustomerName'],
                    'PhoneNo': row['PhoneNo'],
                    'Total': float(row['Total']) if row['Total'] else 0,
                    'TaxAmountTotal': float(row['TaxAmountTotal']) if row['TaxAmountTotal'] else 0,
                    'NetTotal': float(row['NetTotal']) if row['NetTotal'] else 0,
                    'InvoiceDate': str(row['InvoiceDate']) if row['InvoiceDate'] else None,
                    'detail_lines': [],
                }

            if row.get('ItemID'):
                raw[inv_id]['detail_lines'].append({
                    'ItemID': row['ItemID'],
                    'ItemName': row['ItemName'],
                    'Quantity': float(row['Quantity']) if row['Quantity'] else 0,
                    'UnitPrice': float(row['UnitPrice']) if row['UnitPrice'] else 0,
                    'SubTotal': float(row['SubTotal']) if row['SubTotal'] else 0,
                    'TaxAmount': float(row['TaxAmount']) if row['TaxAmount'] else 0,
                    'TaxPercent': float(row['TaxPercent']) if row['TaxPercent'] else 0,
                })

        return list(raw.values())

    def _query_original_invoice_sessions(self, cursor, original_invoice_ids):
        """Map MSSQL InvoiceIDs to their SessionIDs."""
        if not original_invoice_ids:
            return {}

        placeholders = ','.join(['%s'] * len(original_invoice_ids))
        cursor.execute(f"""
            SELECT InvoiceID, SessionID
            FROM [dbo].[tblInvoice]
            WHERE InvoiceID IN ({placeholders})
        """, original_invoice_ids)

        return {row['InvoiceID']: row['SessionID'] for row in cursor.fetchall()}
