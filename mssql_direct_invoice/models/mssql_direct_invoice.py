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

    sales_payment_method_ids = fields.One2many(
        'mssql.direct.payment.method',
        'sync_config_id',
        string='Sales Payment Methods',
        domain=[('scope', '=', 'sales')],
        context={'default_scope': 'sales'},
    )

    # ── Entry Point ───────────────────────────────────────────────────

    def action_create_invoice(self):
        """Create session-based invoices using the invoice_date field."""
        if not self.invoice_date:
            raise UserError('Please select an invoice date')
        return self.create_session_based_invoices(self.invoice_date)

    def action_repair_backfills(self):
        """One-shot cleanup for legacy MSSQL-INV-{id} backfill invoices.

        Scans all posted out_invoices whose ref starts with 'MSSQL-INV-', cancels
        each (along with its payments), and redirects any CN previously
        reconciled against it to the correct session aggregate. CNs whose
        owning session aggregate isn't synced yet are logged and left
        standalone; they'll be handled by the post-session sweep when that
        session eventually syncs.

        This is a legacy-data cleanup tool — the new CN processor no longer
        creates backfills, so after running this once the data shouldn't drift
        again.
        """
        self.ensure_one()
        backfills = self.env['account.move'].search([
            ('ref', '=like', 'MSSQL-INV-%'),
            ('move_type', '=', 'out_invoice'),
        ], order='id')

        if not backfills:
            return self._success_notification(
                'Repair Backfills',
                'No MSSQL-INV-* backfill invoices found. Nothing to repair.'
            )

        cancelled = 0
        cn_redirected = 0
        cn_left_standalone = 0
        cn_unchanged = 0

        for backfill in backfills:
            mssql_invoice_id = backfill.ref[len('MSSQL-INV-'):]

            # Find CNs currently reconciled against this backfill's AR
            backfill_ar = backfill.line_ids.filtered(
                lambda l: l.account_id.account_type == 'asset_receivable')
            matched_cn_ars = self.env['account.move.line']
            for line in backfill_ar:
                if not line.full_reconcile_id and not line.matched_credit_ids and not line.matched_debit_ids:
                    continue
                matches = line.matched_credit_ids.credit_move_id + line.matched_debit_ids.debit_move_id
                for m in matches:
                    if (m.account_id.account_type == 'asset_receivable'
                        and m.move_id != backfill
                        and m.move_id.move_type == 'out_refund'
                        and (m.move_id.ref or '').startswith('MSSQL-CN-')):
                        matched_cn_ars |= m

            affected_cns = matched_cn_ars.mapped('move_id')

            # Remove reconciliations involving this backfill (unreconcile all its AR lines)
            try:
                backfill_ar.remove_move_reconcile()
            except Exception as e:
                _logger.warning(
                    f"Repair: could not unreconcile {backfill.name}: {e}")

            # Cancel payments linked to this backfill + the backfill itself
            payment_moves = backfill.matched_payment_ids.mapped('move_id') if hasattr(backfill, 'matched_payment_ids') else self.env['account.move']
            try:
                # Reverse any payments posted against the backfill
                for pay in payment_moves:
                    if pay.state == 'posted':
                        pay.button_draft()
                        pay.button_cancel()
                if backfill.state == 'posted':
                    backfill.button_draft()
                backfill.button_cancel()
                cancelled += 1
            except Exception as e:
                _logger.warning(
                    f"Repair: could not cancel {backfill.name}: {e}")
                continue

            # Redirect each affected CN to its proper session aggregate
            for cn in affected_cns:
                cn_mssql_id = (cn.ref or '').replace('MSSQL-CN-', '')
                # Figure out what the CN should reconcile against (redemption or original)
                # Pull MSSQL redemption state
                target_session_invoice = self._cn_target_session(cn_mssql_id)
                if target_session_invoice:
                    try:
                        self._reconcile_ar(
                            cn, target_session_invoice, f"Repair CN {cn_mssql_id}")
                        cn_redirected += 1
                    except Exception as e:
                        _logger.warning(
                            f"Repair: could not redirect {cn.name}: {e}")
                        cn_unchanged += 1
                else:
                    cn_left_standalone += 1
                    _logger.info(
                        f"Repair: CN {cn.name} left standalone "
                        f"(target session not synced)")

        return self._success_notification(
            'Repair Backfills',
            f"Cancelled {cancelled} backfill invoice(s). "
            f"Redirected {cn_redirected} CN(s) to session aggregates, "
            f"{cn_left_standalone} left standalone, {cn_unchanged} unchanged."
        )

    def _cn_target_session(self, cn_mssql_id):
        """Look up the correct session aggregate for a CN from MSSQL state.

        Priority: CRA session (authoritative consumption record) →
        tblInvoiceReturnCode.UsedInvoiceID → original invoice's session.
        """
        if not cn_mssql_id:
            return False
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT zcn.ReturnInvoiceID,
                       rc.Used, rc.UsedInvoiceID, rc.Canceled,
                       cra.SessionID AS UsedSessionID
                FROM [dbo].[tblZatcaCreditNote] zcn
                LEFT JOIN [dbo].[tblInvoiceReturnCode] rc ON rc.InvoiceID = zcn.InvoiceID
                LEFT JOIN [dbo].[tblCashierActivityReturnAmount] cra
                    ON cra.ReturnCode = rc.ReturnCode
                WHERE zcn.InvoiceID = %s
            """, (cn_mssql_id,))
            row = cursor.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if not row or row.get('Canceled'):
            return False

        # Preferred: CRA session
        used_session_id = row.get('UsedSessionID') or 0
        if row.get('Used') and used_session_id:
            inv = self.env['account.move'].search([
                ('ref', '=like', f'Session {used_session_id} -%'),
                ('move_type', '=', 'out_invoice'),
                ('state', '=', 'posted'),
            ], limit=1)
            if inv:
                return inv

        target_invoice_id = (
            row['UsedInvoiceID']
            if row.get('Used') and row.get('UsedInvoiceID')
            else row.get('ReturnInvoiceID')
        )
        if not target_invoice_id:
            return False
        return self._find_session_aggregate_for_mssql_invoice(target_invoice_id)

    def action_fetch_sales_payment_methods(self):
        """Fetch payment-method rows from MSSQL and upsert into the O2M.

        Combines the reference tblPaymentType with any PaymentType actually
        seen in tblCashierActivityDetail (picks up orphan PTs like 50/100).
        Existing rows keep their journal mapping; only new codes are inserted.
        """
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT PaymentTypeID AS code, PaymentType AS name
                FROM [dbo].[tblPaymentType]
                UNION
                SELECT DISTINCT PaymentType AS code,
                       CAST(PaymentType AS NVARCHAR(50)) AS name
                FROM [dbo].[tblCashierActivityDetail]
                WHERE PaymentType IS NOT NULL
                  AND PaymentType NOT IN (SELECT PaymentTypeID FROM [dbo].[tblPaymentType])
            """)
            rows = cursor.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        existing = {m.mssql_code: m for m in self.sales_payment_method_ids}
        to_create = []
        for row in rows:
            code = int(row['code'])
            if code in existing:
                continue
            to_create.append({
                'sync_config_id': self.id,
                'scope': 'sales',
                'mssql_code': code,
                'name': str(row.get('name') or f'PT{code}').strip() or f'PT{code}',
            })
        if to_create:
            self.env['mssql.direct.payment.method'].create(to_create)
        return self._success_notification(
            'Sales Payment Methods',
            f"Added {len(to_create)} new; total mapped = {len(self.sales_payment_method_ids)}."
        )

    def create_session_based_invoices(self, invoice_date):
        """Create invoices directly (no SO) based on POS sessions for a date."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = invoice_date.strftime('%Y-%m-%d')
            next_date = (invoice_date + timedelta(days=1)).strftime('%Y-%m-%d')

            _logger.info("=" * 80)
            _logger.info(f"DIRECT INVOICE SYNC FOR DATE: {date_str}")
            _logger.info("=" * 80)

            sessions = self._query_sessions_for_date(cursor, date_str, next_date)
            if not sessions:
                conn.close()
                raise UserError(f'No POS sessions found for date {date_str}')

            session_ids = [s['SessionID'] for s in sessions]
            _logger.info(f"Found {len(sessions)} sessions for {date_str}")

            all_payments = self._query_all_session_payments(cursor, session_ids)
            all_credit_sales = self._query_all_session_credit_sales(cursor, session_ids)
            all_invoice_ranges = self._query_all_session_invoice_ranges(cursor, session_ids)
            all_on_account = self._query_all_session_on_account_invoices(cursor, session_ids)

            # Just-in-time customer sync for any on-account customer not yet
            # in Odoo. Done while the cursor is still open so we can fetch
            # only the rows we need from tblCustomers.
            needed_customer_ids = sorted({
                inv['CustomerID']
                for invs in all_on_account.values()
                for inv in invs
                if inv.get('CustomerID')
            })
            if needed_customer_ids:
                self._ensure_customers_exist(cursor, needed_customer_ids)

            conn.close()
            _logger.info("MSSQL fetch done, connection closed.")

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

                if any(ref.startswith(f"Session {session_id} -") for ref in existing_refs):
                    skipped_existing += 1
                    continue

                record_data = json.dumps({
                    'session': dict(session),
                    'credit_sales': all_credit_sales.get(session_id, {}),
                    'payments': [dict(p) for p in all_payments.get(session_id, [])],
                    'invoice_range': all_invoice_ranges.get(session_id, {}),
                    'on_account_invoices': all_on_account.get(session_id, []),
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
            _logger.info(f"Queue {queue.name}: {len(line_vals_list)} lines")

            queue.action_process_queue()

            # Auto-import credit notes for the same date
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
            except Exception:
                pass
            raise UserError(f'Direct invoice creation failed: {str(e)}')

    # ── Queue Line Processor — Sales Invoice ─────────────────────────

    @staticmethod
    def _coerce_numeric(val):
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return float(val)
            except (ValueError, TypeError):
                return val
        return val

    def _process_queue_sales_invoice(self, data, queue_line):
        """Build the session aggregate (cash + cards portion) and per-customer
        on-account out_invoices.

        - Aggregate amount = NetTotal − SUM(pure-on-account CreditAmount).
        - PT6 already excluded from session_payments at the SQL level.
        - Each pure-on-account invoice (CreditAmount > 0 and all other payment
          columns = 0) becomes its own out_invoice with ref MSSQL-INV-{id},
          partnered to the real MSSQL customer (or عميل نقدي if CustomerID is
          missing). No payment registered — stays open as customer AR.
        """
        session = data['session']
        session_payments = data.get('payments', [])
        invoice_range = data.get('invoice_range', {})
        credit_sales = data.get('credit_sales', {})
        on_account_invoices = data.get('on_account_invoices', [])

        session_id = session['SessionID']
        cashier_name = session.get('CashierName') or f"Cashier {session.get('EmployeeID', '?')}"
        net_total = round(float(self._coerce_numeric(session['NetTotal']) or 0), 2)

        # Duplicate guard — for the session aggregate. Per-invoice on-account
        # moves get their own MSSQL-INV-{id} dedup further below.
        existing_inv = self.env['account.move'].search([
            ('ref', '=like', f"Session {session_id} -%"),
            ('move_type', '=', 'out_invoice'),
        ], limit=1)
        if existing_inv:
            _logger.info(f"Skipping Session {session_id} — already imported as {existing_inv.name}")
            return {'model': 'account.move', 'id': existing_inv.id, 'skipped': True}

        if net_total <= 0:
            raise UserError(
                f"Session {session_id}: NetTotal {net_total} is not positive — "
                f"nothing to import (return-counter session?)."
            )

        session_date = self._parse_mssql_date(session.get('SessionDate'))

        # Coerce payments
        for pay in session_payments:
            for key in ('PaymentAmount', 'Amount', 'PaymentType'):
                if key in pay:
                    pay[key] = self._coerce_numeric(pay[key])

        # Credit sales narration (informational — separate from on-account)
        credit_amount = round(self._coerce_numeric(credit_sales.get('total')) or 0, 2)

        # Compute on-account split
        on_account_total = round(sum(
            float(inv.get('CreditAmount') or 0) for inv in on_account_invoices
        ), 2)
        aggregate_amount = round(net_total - on_account_total, 2)

        # Tax + product + journal + cash partner
        tax = self._get_or_create_vat_15_inclusive('sale')
        product = self._get_or_create_pos_sales_product()
        cash_partner = self._get_cash_customer_partner()
        sale_journal = self.env['account.journal'].search([
            ('type', '=', 'sale'),
            ('company_id', '=', self.env.company.id),
        ], limit=1)
        if not sale_journal:
            raise UserError('No sales journal found.')

        # ── Per-customer on-account invoices ─────────────────────────────
        on_account_moves = self._create_on_account_moves(
            on_account_invoices, cash_partner, tax, product, sale_journal, session_id)

        # ── Session aggregate (cash + cards + STC + ...) ─────────────────
        # If aggregate_amount is 0, the session was purely on-account: skip
        # the aggregate move entirely. The on-account moves above are enough.
        if aggregate_amount <= 0:
            if not on_account_moves:
                # Nothing to import — defensive
                raise UserError(
                    f"Session {session_id}: aggregate_amount={aggregate_amount} "
                    f"with no on-account invoices either. Data anomaly."
                )
            _logger.info(
                f"Session {session_id}: purely on-account session — "
                f"created {len(on_account_moves)} per-customer invoice(s), "
                f"skipping aggregate.")
            return {
                'model': 'account.move',
                'id': on_account_moves[0].id,
                'extras': {'on_account_count': len(on_account_moves)},
            }

        # Ref for the session aggregate
        min_inv = invoice_range.get('MinInvoiceID', '')
        max_inv = invoice_range.get('MaxInvoiceID', '')
        ref_text = f"Session {session_id} - {cashier_name} - invs {min_inv} to {max_inv}"

        invoice = self.env['account.move'].create({
            'move_type': 'out_invoice',
            'partner_id': cash_partner.id,
            'invoice_date': session_date,
            'date': session_date,
            'ref': ref_text,
            'journal_id': sale_journal.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id,
                'quantity': 1,
                'price_unit': aggregate_amount,
                'name': f'POS Sales - Session {session_id}',
                'tax_ids': [(6, 0, [tax.id])],
            })],
        })

        self._assert_total_matches(invoice, aggregate_amount, f"Session {session_id}")

        if credit_amount > 0:
            invoice.write({'narration': self._build_credit_sales_narration(credit_sales)})

        invoice.action_post()

        # ── Payments (PT5 + PT6 excluded at SQL level)
        self._register_customer_payments(
            invoice, session_payments, session_date, session_id)

        # Cash differences (shortage/surplus) per payment method
        try:
            self._post_session_cash_differences(
                session_payments, session_date, session_id)
        except Exception as e:
            _logger.warning(f"Session {session_id}: Failed to post cash differences: {e}")

        # Post-step: sweep any existing Odoo CNs that were redeemed INTO this
        # session's invoice range and reconcile them against this session's AR.
        try:
            self._sweep_session_cn_redemptions(invoice, session_id)
        except Exception as e:
            _logger.warning(f"Session {session_id}: CN redemption sweep failed: {e}")

        # Residual sanity check.
        # Expected = aggregate_amount − sum(non-PT5/6 payments).
        # Drift > 0 means a payment silently failed; drift < 0 means the
        # CN-redemption sweep absorbed PT5 into fully-paid state (fine).
        invoice = self.env['account.move'].browse(invoice.id)
        actual_residual = round(invoice.amount_residual, 2)
        intended_registered = round(sum(
            self._payment_pcamount(p)
            for p in session_payments
            if self._payment_pcamount(p) > 0
        ), 2)
        expected_residual = round(aggregate_amount - intended_registered, 2)
        drift = round(actual_residual - expected_residual, 2)
        if abs(drift) >= 0.01 and drift > 0:
            raise UserError(
                f"Session {session_id}: residual {actual_residual} "
                f"!= expected {expected_residual} "
                f"(aggregate {aggregate_amount} - registered {intended_registered}; "
                f"diff={drift}). A payment registration likely failed silently."
            )

        _logger.info(
            f"Session {session_id}: {invoice.name} created "
            f"(aggregate={aggregate_amount}, on_account_total={on_account_total}, "
            f"on_account_invs={len(on_account_moves)}, residual={actual_residual})")
        return {'model': 'account.move', 'id': invoice.id}

    def _create_on_account_moves(self, on_account_invoices, cash_partner, tax,
                                 product, sale_journal, session_id):
        """For each pure-on-account invoice, create an out_invoice keyed to
        the original customer and posted at CreditAmount. No payment registered
        — the move stays open as customer AR until the customer settles."""
        if not on_account_invoices:
            return []

        # Bulk customer lookup
        customer_ids = sorted({
            inv['CustomerID'] for inv in on_account_invoices if inv.get('CustomerID')
        })
        customer_map = {}
        if customer_ids:
            for p in self.env['res.partner'].search([
                ('x_sql_customer_id', 'in', list(customer_ids)),
                ('customer_rank', '>', 0),
            ]):
                customer_map[p.x_sql_customer_id] = p

        moves = []
        for inv in on_account_invoices:
            mssql_invoice_id = inv['InvoiceID']
            ref = f"MSSQL-INV-{mssql_invoice_id}"

            # Per-invoice idempotency
            existing = self.env['account.move'].search([
                ('ref', '=', ref),
                ('move_type', '=', 'out_invoice'),
            ], limit=1)
            if existing:
                _logger.info(
                    f"Session {session_id}: on-account inv {mssql_invoice_id} "
                    f"already imported as {existing.name}")
                moves.append(existing)
                continue

            credit_amount = round(float(inv.get('CreditAmount') or 0), 2)
            if credit_amount <= 0:
                continue

            customer = customer_map.get(inv.get('CustomerID'))
            if not customer:
                _logger.warning(
                    f"Session {session_id}: on-account inv {mssql_invoice_id} "
                    f"has CustomerID={inv.get('CustomerID')} which is missing in "
                    f"Odoo — falling back to cash customer.")
                customer = cash_partner

            inv_date = self._parse_mssql_date(inv.get('InvoiceDate'))
            cust_label = inv.get('CustomerName') or '?'
            phone = inv.get('PhoneNo') or ''
            phone_part = f" ({phone})" if phone else ""
            line_name = (
                f"POS On-Account - Invoice {mssql_invoice_id} | "
                f"{cust_label}{phone_part}"
            )

            move = self.env['account.move'].create({
                'move_type': 'out_invoice',
                'partner_id': customer.id,
                'invoice_date': inv_date,
                'date': inv_date,
                'ref': ref,
                'journal_id': sale_journal.id,
                'invoice_line_ids': [(0, 0, {
                    'product_id': product.id,
                    'quantity': 1,
                    'price_unit': credit_amount,
                    'name': line_name,
                    'tax_ids': [(6, 0, [tax.id])],
                })],
            })

            self._assert_total_matches(
                move, credit_amount, f"On-account inv {mssql_invoice_id}")
            move.action_post()
            moves.append(move)
            _logger.info(
                f"Session {session_id}: posted on-account {move.name} "
                f"(MSSQL-INV-{mssql_invoice_id}, customer={customer.name}, "
                f"amount={credit_amount})")

        return moves

    # ── Post-step: cross-day CN redemption sweep ──────────────────────

    def _sweep_session_cn_redemptions(self, session_invoice, session_id):
        """Find CNs that redeemed INTO this session (per MSSQL) and reconcile
        them against the session aggregate's AR.

        Two MSSQL sources, unioned:
        - tblCashierActivityReturnAmount: authoritative, always populated when
          a voucher is consumed in a session.
        - tblInvoiceReturnCode.UsedInvoiceID: fallback for any row where the
          POS did fill it in.
        """
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT DISTINCT rc.InvoiceID AS CN_InvoiceID
                FROM [dbo].[tblCashierActivityReturnAmount] cra
                INNER JOIN [dbo].[tblInvoiceReturnCode] rc ON cra.ReturnCode = rc.ReturnCode
                WHERE cra.SessionID = %s
                  AND rc.Used = 1
                  AND ISNULL(rc.Canceled, 0) = 0

                UNION

                SELECT rc.InvoiceID AS CN_InvoiceID
                FROM [dbo].[tblInvoiceReturnCode] rc
                INNER JOIN [dbo].[tblInvoice] i ON rc.UsedInvoiceID = i.InvoiceID
                WHERE i.SessionID = %s
                  AND rc.Used = 1
                  AND ISNULL(rc.Canceled, 0) = 0
                  AND rc.UsedInvoiceID != 0
            """, (session_id, session_id))
            cn_ids = [str(row['CN_InvoiceID']) for row in cursor.fetchall()]
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not cn_ids:
            return

        cn_refs = [f"MSSQL-CN-{cid}" for cid in cn_ids]
        existing_cns = self.env['account.move'].search([
            ('ref', 'in', cn_refs),
            ('move_type', '=', 'out_refund'),
            ('state', '=', 'posted'),
        ])

        reconciled = 0
        for cn in existing_cns:
            cn_ar = cn.line_ids.filtered(
                lambda l: l.account_id.account_type == 'asset_receivable' and not l.reconciled)
            if not cn_ar:
                continue  # already reconciled elsewhere
            session_ar = session_invoice.line_ids.filtered(
                lambda l: l.account_id.account_type == 'asset_receivable' and not l.reconciled)
            if not session_ar:
                break  # nothing left to absorb
            try:
                (cn_ar + session_ar).reconcile()
                reconciled += 1
            except Exception as e:
                _logger.warning(
                    f"Session {session_id}: sweep failed to reconcile {cn.name}: {e}")

        if reconciled:
            _logger.info(
                f"Session {session_id}: sweep reconciled {reconciled} existing CN(s) "
                f"(of {len(cn_ids)} redemption record(s) in MSSQL)")

    # ── Customer Payment Registration ─────────────────────────────────

    def _register_customer_payments(self, invoice, session_payments, invoice_date, session_id):
        """Register one account.payment per MSSQL session payment row using the
        sales_payment_method_ids mapping. Unmapped method → UserError so the
        queue line fails with a clear fix hint."""
        if not session_payments:
            return

        if invoice.state != 'posted':
            invoice.action_post()

        mapping = {
            m.mssql_code: m.journal_id
            for m in self.sales_payment_method_ids
            if m.journal_id
        }

        for payment in session_payments:
            payment_type = int(payment.get('PaymentType') or 0)
            amount = self._payment_pcamount(payment)
            if amount <= 0:
                continue

            journal = mapping.get(payment_type)
            if not journal:
                method_name = payment.get('PaymentMethodName') or f'PT{payment_type}'
                raise UserError(
                    f"Session {session_id}: Payment method {payment_type} ({method_name}) "
                    f"has no journal mapped. Configure it on the sync config "
                    f"via 'Fetch Sales Payment Methods' and assign a journal."
                )

            invoice = self.env['account.move'].browse(invoice.id)
            if invoice.amount_residual <= 0:
                break

            try:
                method_name = payment.get('PaymentMethodName') or f'PT{payment_type}'
                payment_register = self.env['account.payment.register'].with_context(
                    active_model='account.move',
                    active_ids=invoice.ids,
                    dont_redirect_to_payments=True,
                ).create({
                    'payment_date': invoice_date,
                    'journal_id': journal.id,
                    'amount': amount,
                    'communication': f"{method_name} - Session {session_id}",
                    'group_payment': False,
                })
                payment_register.action_create_payments()
            except Exception as e:
                if 'nothing left to pay' in str(e).lower():
                    break
                raise

    # ── Cash Differences ──────────────────────────────────────────────

    def _post_session_cash_differences(self, session_payments, session_date, session_id):
        """For each payment type with DifAmount != 0, post a bank statement
        line to the journal's loss/profit account (mirrors the POS flow)."""
        mapping = {
            m.mssql_code: m.journal_id
            for m in self.sales_payment_method_ids
            if m.journal_id
        }

        for payment in session_payments:
            dif_amount = self._coerce_numeric(payment.get('DifAmount')) or 0
            if abs(dif_amount) < 0.01:
                continue

            payment_type = int(payment.get('PaymentType') or 0)
            journal = mapping.get(payment_type)
            if not journal:
                _logger.warning(
                    f"Session {session_id}: PT{payment_type} has no mapping — "
                    f"skipping cash-difference {dif_amount}")
                continue

            if dif_amount > 0:  # Shortage
                if not journal.loss_account_id:
                    _logger.warning(
                        f"Session {session_id}: journal {journal.name} has no "
                        f"loss_account_id, skipping shortage {dif_amount}")
                    continue
                counterpart = journal.loss_account_id.id
                label = 'shortage'
            else:  # Surplus
                if not journal.profit_account_id:
                    _logger.warning(
                        f"Session {session_id}: journal {journal.name} has no "
                        f"profit_account_id, skipping surplus {dif_amount}")
                    continue
                counterpart = journal.profit_account_id.id
                label = 'surplus'

            ref = f"Cash {label} - Session {session_id} - {payment.get('PaymentMethodName', '')}"
            note = payment.get('DiffNote')
            if note:
                ref += f" ({note})"

            self.env['account.bank.statement.line'].create({
                'journal_id': journal.id,
                'amount': -dif_amount,
                'date': session_date,
                'payment_ref': ref,
                'counterpart_account_id': counterpart,
            })
            _logger.info(
                f"Session {session_id}: posted cash-{label} {dif_amount} "
                f"for PT{payment_type}")

    # ── Narration ─────────────────────────────────────────────────────

    def _build_credit_sales_narration(self, credit_sales):
        """Format the unpaid-invoice list into a multi-line narration."""
        credit_amount = round(self._coerce_numeric(credit_sales.get('total')) or 0, 2)
        lines = [f"Credit sales (unpaid) — {credit_amount:.2f} SAR:"]
        for cinv in credit_sales.get('invoices', []):
            inv_id = cinv.get('InvoiceID', '?')
            cust_name = cinv.get('CustomerName') or '?'
            phone = cinv.get('PhoneNo') or ''
            inv_total = self._coerce_numeric(cinv.get('NetTotal')) or 0
            phone_part = f" ({phone})" if phone else ""
            lines.append(
                f"  Invoice {inv_id} | Customer: {cust_name}{phone_part} | {inv_total:.2f} SAR"
            )
        return '\n'.join(lines)

    # ── Just-in-time customer sync ────────────────────────────────────

    def _ensure_customers_exist(self, cursor, customer_ids):
        """Make sure every MSSQL CustomerID has a matching res.partner.

        Called from create_session_based_invoices for the small subset of
        customers that actually transact on-account during the synced day.
        Avoids running the 93k-row full sync just to capture a handful of
        customers."""
        if not customer_ids:
            return
        existing = {
            p.x_sql_customer_id
            for p in self.env['res.partner'].search([
                ('x_sql_customer_id', 'in', list(customer_ids)),
                ('customer_rank', '>', 0),
            ])
        }
        missing = [cid for cid in customer_ids if cid not in existing]
        if not missing:
            return

        placeholders = ','.join(['%s'] * len(missing))
        cursor.execute(f"""
            SELECT
                CustomerID, CustomerName, CustomerAddress,
                Phone1, Phone2, Mobile, EMail, WebSite,
                CustVatNumber, CRNo, City, StreetName, BuildingNo,
                PostalZone, POBox, Area, ContactPerson,
                CustomerNote, CreditLimit
            FROM [dbo].[tblCustomers]
            WHERE CustomerID IN ({placeholders})
        """, missing)
        rows = cursor.fetchall()

        to_create = []
        for r in rows:
            vals = {
                'name': r['CustomerName'] or f"Customer {r['CustomerID']}",
                'x_sql_customer_id': r['CustomerID'],
                'customer_rank': 1,
            }
            if r.get('CustomerAddress'):
                vals['street'] = r['CustomerAddress']
            phones = [p for p in (r.get('Phone1'), r.get('Phone2')) if p]
            if phones:
                vals['phone'] = phones[0]
            if r.get('Mobile'):
                vals['mobile'] = r['Mobile']
            if r.get('EMail'):
                vals['email'] = r['EMail']
            if r.get('WebSite'):
                vals['website'] = r['WebSite']
            if r.get('CustVatNumber'):
                vals['vat'] = r['CustVatNumber']
            if r.get('CRNo'):
                vals['company_registry'] = str(r['CRNo'])
            if r.get('City'):
                vals['city'] = r['City']
            if r.get('PostalZone'):
                vals['zip'] = r['PostalZone']
            if r.get('CustomerNote'):
                vals['comment'] = r['CustomerNote']
            if r.get('CreditLimit') is not None:
                vals['credit_limit'] = float(r['CreditLimit'])
            # Promote to company when business identifiers are present.
            if vals.get('vat') or vals.get('company_registry'):
                vals['company_type'] = 'company'
            to_create.append(vals)

        if to_create:
            self.env['res.partner'].with_context(
                tracking_disable=True, mail_create_nolog=True,
                mail_create_nosubscribe=True, mail_notrack=True,
                no_vat_validation=True,
            ).create(to_create)
            _logger.info(
                f"on-account: just-in-time created {len(to_create)} customer(s) "
                f"for {len(missing)} missing IDs")

    # ── Sales SQL Queries ─────────────────────────────────────────────

    def _query_sessions_for_date(self, cursor, date_str, next_date):
        cursor.execute("""
            SELECT
                ca.SessionID,
                ca.SessionDate,
                ca.EmployeeID,
                e.EmployeeName AS CashierName,
                ca.InvoiceCount,
                ca.SalesInvoiceCount,
                ca.ReturnInvoiceCount,
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

    def _query_all_session_payments(self, cursor, session_ids):
        if not session_ids:
            return {}
        placeholders = ','.join(['%s'] * len(session_ids))
        # Payment amount = PCAmount (system-expected, SUM equals NetTotal).
        # ActualAmount is what the cashier counted; the difference is the
        # till surplus/shortage which flows through _post_session_cash_differences
        # to the journal's loss/profit account, NOT through payments.
        #
        # Both keys are emitted so _payment_pcamount() can detect the record_data
        # schema version (explicit PCAmount = post-fix; only Amount = legacy).
        #
        # Excluded payment types:
        # - PT5 (return voucher): matching CN reconciles against the redemption
        #   invoice directly (R8).
        # - PT6 (on-account / على الحساب): purely on-account invoices are
        #   extracted from the session aggregate and posted as per-customer
        #   out_invoice records (see _query_all_session_on_account_invoices).
        #   Any residual PT6 covers mixed-payment invoices that stay in the
        #   aggregate; that residual is left open as session-level AR.
        cursor.execute(f"""
            SELECT
                ca.SessionID,
                cad.PaymentType,
                pt.PaymentType AS PaymentMethodName,
                ROUND(cad.PCAmount, 2) AS PCAmount,
                ROUND(cad.PCAmount, 2) AS Amount,
                ROUND(cad.ActualAmount, 2) AS ActualAmount,
                ROUND(cad.DifAmount, 2) AS DifAmount,
                cad.DiffNote
            FROM [dbo].[tblCashierActivityDetail] cad
            INNER JOIN [dbo].[tblCashierActivity] ca ON cad.SessionID = ca.SessionID
            LEFT JOIN [dbo].[tblPaymentType] pt ON cad.PaymentType = pt.PaymentTypeID
            WHERE ca.SessionID IN ({placeholders})
              AND (cad.PCAmount > 0 OR cad.DifAmount != 0)
              AND cad.PaymentType NOT IN (5, 6)
            ORDER BY ca.SessionID, cad.PaymentType
        """, session_ids)
        results = {}
        for row in cursor.fetchall():
            results.setdefault(row['SessionID'], []).append(row)
        return results

    def _query_all_session_on_account_invoices(self, cursor, session_ids):
        """Fetch purely-on-account invoices per session.

        Definition of 'pure on-account' (strict): CreditAmount > 0 and every
        other payment column is 0. In that scenario CreditAmount equals
        NetTotal, so the whole invoice gets extracted as one customer-linked
        out_invoice. Mixed-payment invoices (e.g., 50 cash + 50 on-account)
        stay in the session aggregate.

        CustomerID = 0 / NULL invoices are still extracted; the processor
        falls back to the cash customer with a warning.
        """
        if not session_ids:
            return {}
        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                i.InvoiceID,
                i.CustomerID,
                i.CustomerName,
                i.PhoneNo,
                i.InvoiceDate,
                ROUND(i.NetTotal, 2)     AS NetTotal,
                ROUND(i.CreditAmount, 2) AS CreditAmount
            FROM [dbo].[tblInvoice] i
            WHERE i.SessionID IN ({placeholders})
              AND i.IsReturned = 0
              AND ISNULL(i.CreditAmount, 0) > 0
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
        results = {}
        for row in cursor.fetchall():
            results.setdefault(row['SessionID'], []).append({
                'InvoiceID': row['InvoiceID'],
                'CustomerID': row['CustomerID'] or 0,
                'CustomerName': row['CustomerName'],
                'PhoneNo': row['PhoneNo'],
                'InvoiceDate': str(row['InvoiceDate']) if row['InvoiceDate'] else None,
                'NetTotal': float(row['NetTotal']) if row['NetTotal'] else 0.0,
                'CreditAmount': float(row['CreditAmount']) if row['CreditAmount'] else 0.0,
            })
        return results

    @staticmethod
    def _payment_pcamount(payment):
        """Return the system-expected payment amount (PCAmount) for a CAD row,
        whether the record_data JSON was queued pre- or post-PCAmount fix.

        - Post-fix JSON: PCAmount key present → use it directly.
        - Legacy JSON: only Amount (= ActualAmount) and DifAmount present.
          Reconstruct PCAmount = ActualAmount + DifAmount
          (DifAmount = PCAmount - ActualAmount in MSSQL semantics).
        """
        if payment.get('PCAmount') is not None:
            return round(float(payment['PCAmount']), 2)
        amt = float(payment.get('Amount') or 0)
        dif = float(payment.get('DifAmount') or 0)
        return round(amt + dif, 2)

    def _query_all_session_invoice_ranges(self, cursor, session_ids):
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
            'InvoiceCount': row['InvoiceCount'],
        } for row in cursor.fetchall()}

    def _query_all_session_credit_sales(self, cursor, session_ids):
        """Unpaid sales invoices per session — used only for narration."""
        if not session_ids:
            return {}
        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                i.InvoiceID,
                i.CustomerName,
                i.PhoneNo,
                ROUND(i.NetTotal, 2) AS NetTotal
            FROM [dbo].[tblInvoice] i
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
              AND ISNULL(i.CreditAmount, 0) = 0
            ORDER BY i.SessionID, i.InvoiceID
        """, session_ids)

        results = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            bucket = results.setdefault(session_id, {'total': 0.0, 'invoices': []})
            net = float(row['NetTotal'] or 0)
            bucket['total'] = round(bucket['total'] + net, 2)
            bucket['invoices'].append({
                'InvoiceID': row['InvoiceID'],
                'CustomerName': row['CustomerName'],
                'PhoneNo': row['PhoneNo'],
                'NetTotal': net,
            })
        return results

    # ───────────────────────────────────────────────────────────────────
    # Sales Credit Notes (from tblZatcaCreditNote)
    # ───────────────────────────────────────────────────────────────────

    def create_sales_credit_notes(self, credit_note_date):
        """Create credit notes from tblZatcaCreditNote for a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = credit_note_date.strftime('%Y-%m-%d')
            next_date = (credit_note_date + timedelta(days=1)).strftime('%Y-%m-%d')

            _logger.info("=" * 80)
            _logger.info(f"CREDIT NOTE SYNC FOR DATE: {date_str}")
            _logger.info("=" * 80)

            credit_notes = self._query_zatca_credit_notes(cursor, date_str, next_date)
            if not credit_notes:
                conn.close()
                raise UserError(f'No credit notes found for date {date_str}')

            original_invoice_ids = list({
                cn['ReturnInvoiceID'] for cn in credit_notes if cn.get('ReturnInvoiceID')
            })
            original_sessions = (
                self._query_original_invoice_sessions(cursor, original_invoice_ids)
                if original_invoice_ids else {}
            )

            cn_ids = [cn['InvoiceID'] for cn in credit_notes]
            cn_redemptions = self._query_cn_redemptions(cursor, cn_ids)

            conn.close()

            for cn in credit_notes:
                orig_inv_id = cn.get('ReturnInvoiceID')
                orig_info = original_sessions.get(orig_inv_id) or {}
                cn['OriginalSessionID'] = orig_info.get('SessionID')
                orig_session_date = orig_info.get('SessionDate')
                if orig_session_date:
                    cn['OriginalSessionDate'] = str(orig_session_date)[:10]
                else:
                    orig_inv_date = orig_info.get('InvoiceDate')
                    cn['OriginalSessionDate'] = str(orig_inv_date)[:10] if orig_inv_date else None

                redemption = cn_redemptions.get(cn['InvoiceID']) or {}
                cn['Redemption_Used'] = redemption.get('Used', False)
                cn['Redemption_UsedInvoiceID'] = redemption.get('UsedInvoiceID', 0)
                cn['Redemption_UsedSessionID'] = redemption.get('UsedSessionID', 0)
                cn['Redemption_UsedDate'] = redemption.get('UsedDate')
                cn['Redemption_Canceled'] = redemption.get('Canceled', False)

            existing_refs = set(
                self.env['account.move'].search([
                    ('ref', '!=', False),
                    ('move_type', '=', 'out_refund'),
                ]).mapped('ref')
            )

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
                line_vals_list.append({
                    'queue_id': queue.id,
                    'name': f"CN {cn_invoice_id} - {cn.get('CustomerName') or 'Credit Note'}",
                    'mssql_id': str(cn_invoice_id),
                    'mssql_table': 'tblZatcaCreditNote',
                    'record_data': json.dumps(cn, default=str),
                })

            if not line_vals_list:
                queue.unlink()
                return self._success_notification(
                    'Credit Note Sync', 'No new credit notes to process')

            self.env['mssql.direct.sync.queue.line'].create(line_vals_list)
            _logger.info(f"Queue {queue.name}: {len(line_vals_list)} CN lines")

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
            except Exception:
                pass
            raise UserError(f'Credit note sync failed: {str(e)}')

    # ── Strict session-aggregate lookup ───────────────────────────────

    def _find_session_aggregate_for_mssql_invoice(self, mssql_invoice_id):
        """Return the Odoo move that owns this MSSQL invoice, or False if not
        yet synced.

        Lookup priority:
        1. Per-invoice on-account move (ref = MSSQL-INV-{id}). These are
           created by the on-account extraction path and partnered to the
           real MSSQL customer. CNs against on-account invoices reconcile
           here.
        2. Session aggregate (ref = 'Session {sid} -%'). Default for normal
           cash/card sales.

        Strict rule (no backfills): we never *create* MSSQL-INV-{id} from
        the CN processor. If the owning session hasn't been synced, the CN
        queue line fails so the cron can retry once the session arrives.
        """
        if not mssql_invoice_id:
            return False

        # 1. Per-invoice on-account move
        per_inv = self.env['account.move'].search([
            ('ref', '=', f'MSSQL-INV-{mssql_invoice_id}'),
            ('move_type', '=', 'out_invoice'),
            ('state', '=', 'posted'),
        ], limit=1)
        if per_inv:
            return per_inv

        # 2. Session aggregate via MSSQL SessionID lookup
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute(
                "SELECT SessionID FROM [dbo].[tblInvoice] WHERE InvoiceID = %s",
                (mssql_invoice_id,),
            )
            row = cursor.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if not row or not row.get('SessionID'):
            return False
        session_id = row['SessionID']
        inv = self.env['account.move'].search([
            ('ref', '=like', f'Session {session_id} -%'),
            ('move_type', '=', 'out_invoice'),
            ('state', '=', 'posted'),
        ], limit=1)
        return inv or False

    # ── CN Queue Processor ────────────────────────────────────────────

    def _process_queue_sales_credit_note(self, data, queue_line):
        """Build one aggregate out_refund whose total == MSSQL NetTotal, then
        apply R8 redemption handling (redeem-against-invoice or cash refund)."""
        cn_invoice_id = data['InvoiceID']
        ref = f"MSSQL-CN-{cn_invoice_id}"

        existing_cn = self.env['account.move'].search([
            ('ref', '=', ref),
            ('move_type', '=', 'out_refund'),
        ], limit=1)
        if existing_cn:
            _logger.info(f"Skipping CN {cn_invoice_id} — already imported as {existing_cn.name}")
            return {'model': 'account.move', 'id': existing_cn.id, 'skipped': True}

        original_invoice_id = data.get('ReturnInvoiceID')
        original_session_id = data.get('OriginalSessionID')
        net_total = round(float(self._coerce_numeric(data.get('NetTotal')) or 0), 2)
        cn_date = self._parse_mssql_date(data.get('InvoiceDate'))

        if net_total <= 0:
            raise UserError(
                f"CN {cn_invoice_id}: NetTotal {net_total} is not positive.")

        cash_partner = self._get_cash_customer_partner()
        tax = self._get_or_create_vat_15_inclusive('sale')
        product = self._get_or_create_pos_return_product()
        sale_journal = self.env['account.journal'].search([
            ('type', '=', 'sale'),
            ('company_id', '=', self.env.company.id),
        ], limit=1)
        if not sale_journal:
            raise UserError('No sales journal found.')

        # Look up the original move (per-invoice MSSQL-INV-* preferred over
        # session aggregate). We intentionally do NOT set reversed_entry_id
        # at create time — Odoo 18's account.move._post auto-reconciles any
        # move with a posted reversed_entry_id against it, which steals AR
        # that should go to the cash refund or the redemption invoice
        # (handled by _handle_cn_redemption below). We set reversed_entry_id
        # *after* post only for the outstanding-voucher case as a UX breadcrumb.
        original_odoo_invoice = self._locate_original_for_cn(
            original_session_id, original_invoice_id, cn_date, data, cn_invoice_id)

        # Partner selection: when the original is an on-account per-invoice
        # move (real customer, not the cash customer), put the CN on that
        # customer's ledger so AR-to-AR reconciliation can actually close it.
        # Session-aggregate originals stay on عميل نقدي (same customer both sides).
        partner = cash_partner
        if original_odoo_invoice and original_odoo_invoice.partner_id != cash_partner:
            partner = original_odoo_invoice.partner_id

        cn_vals = {
            'move_type': 'out_refund',
            'partner_id': partner.id,
            'invoice_date': cn_date,
            'date': cn_date,
            'ref': ref,
            'journal_id': sale_journal.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id,
                'quantity': 1,
                'price_unit': net_total,
                'name': f'POS Return - CN {cn_invoice_id}',
                'tax_ids': [(6, 0, [tax.id])],
            })],
        }

        credit_note = self.env['account.move'].create(cn_vals)
        self._assert_total_matches(credit_note, net_total, f"CN {cn_invoice_id}")
        credit_note.action_post()

        # R8 redemption handling
        self._handle_cn_redemption(
            credit_note, data, original_odoo_invoice, cn_date, cn_invoice_id)

        method = 'linked' if original_odoo_invoice else 'standalone'
        _logger.info(
            f"CN {cn_invoice_id}: {credit_note.name} created ({method}, "
            f"amount={credit_note.amount_total}, residual={credit_note.amount_residual})")
        return {'model': 'account.move', 'id': credit_note.id}

    def _locate_original_for_cn(self, original_session_id, original_invoice_id,
                                cn_date, data, cn_invoice_id):
        """Return the Odoo move that owns this CN's original invoice.

        Lookup priority (matches _find_session_aggregate_for_mssql_invoice):
        1. Per-invoice on-account move (ref = MSSQL-INV-{id}) — has the real
           customer, open AR, intended target for CNs against on-account sales.
        2. Session aggregate (ref = 'Session {sid} -%') — for CNs against
           ordinary cash/card sales.

        Strict rule: no backfills. If neither exists, return False and let
        the processor raise UserError (queue line fails, cron retries once
        the missing session is synced).
        """
        # 1. Per-invoice on-account move
        if original_invoice_id:
            per_inv = self.env['account.move'].search([
                ('ref', '=', f'MSSQL-INV-{original_invoice_id}'),
                ('move_type', '=', 'out_invoice'),
                ('state', '=', 'posted'),
            ], limit=1)
            if per_inv:
                return per_inv

        # 2. Session aggregate via the SessionID we already know
        if original_session_id:
            inv = self.env['account.move'].search([
                ('ref', '=like', f'Session {original_session_id} -%'),
                ('move_type', '=', 'out_invoice'),
                ('state', '=', 'posted'),
            ], limit=1)
            if inv:
                return inv

        # 3. Defensive: re-derive SessionID from MSSQL by InvoiceID
        return self._find_session_aggregate_for_mssql_invoice(original_invoice_id)

    def _reconcile_ar(self, move_a, move_b, label):
        """Reconcile the open AR lines of two moves. Safe to call when either
        side's AR is already partially reconciled — only unreconciled parts
        are combined."""
        a_ar = move_a.line_ids.filtered(
            lambda l: l.account_id.account_type == 'asset_receivable' and not l.reconciled)
        b_ar = move_b.line_ids.filtered(
            lambda l: l.account_id.account_type == 'asset_receivable' and not l.reconciled)
        if a_ar and b_ar:
            (a_ar + b_ar).reconcile()
            _logger.info(f"{label}: reconciled {move_a.name} ↔ {move_b.name}")
            return True
        _logger.warning(
            f"{label}: cannot reconcile {move_a.name} ↔ {move_b.name} "
            f"(one side has no open AR)")
        return False

    def _handle_cn_redemption(self, credit_note, data, original_odoo_invoice,
                              cn_date, cn_invoice_id):
        """Route the CN based on its MSSQL redemption state.

        Redemption target priority:
        1. UsedSessionID (from tblCashierActivityReturnAmount) — authoritative,
           always populated when the voucher is consumed in a session.
        2. UsedInvoiceID → its session (fallback for legacy/edge rows).
        3. Neither + Used=1 → genuine cash refund.
        4. Used=0 → outstanding voucher (reconcile with original).
        5. Canceled=1 → customer credit.

        Missing target session → UserError; the queue retries once the target
        session gets synced by the next cron.
        """
        redemption_used = data.get('Redemption_Used', False)
        used_invoice_id = data.get('Redemption_UsedInvoiceID', 0) or 0
        used_session_id = data.get('Redemption_UsedSessionID', 0) or 0
        redemption_canceled = data.get('Redemption_Canceled', False)

        if redemption_canceled:
            _logger.info(
                f"CN {credit_note.name}: voucher was canceled — leaving as customer credit")
            return

        # Preferred: CRA-recorded session consumption
        if redemption_used and used_session_id:
            redemption_invoice = self.env['account.move'].search([
                ('ref', '=like', f'Session {used_session_id} -%'),
                ('move_type', '=', 'out_invoice'),
                ('state', '=', 'posted'),
            ], limit=1)
            if not redemption_invoice:
                # Cross-day-forward: CN was issued today but redeemed in a
                # future session not yet synced. Leave as customer credit;
                # _sweep_session_cn_redemptions on that session's eventual
                # sync will pick this CN up and reconcile retroactively.
                _logger.info(
                    f"CN {credit_note.name}: redeemed in session {used_session_id} "
                    f"(per CRA) but that session isn't in Odoo yet — leaving as "
                    f"customer credit, post-step sweep will reconcile when "
                    f"session {used_session_id} syncs.")
                return
            self._reconcile_ar(
                credit_note, redemption_invoice,
                f"CN {cn_invoice_id} (redeemed via CRA session {used_session_id})")
            return

        # Fallback: tblInvoiceReturnCode.UsedInvoiceID pointer
        if redemption_used and used_invoice_id:
            redemption_invoice = self._find_session_aggregate_for_mssql_invoice(used_invoice_id)
            if not redemption_invoice:
                # Same cross-day-forward case as above; defer to post-step sweep.
                _logger.info(
                    f"CN {credit_note.name}: redemption invoice {used_invoice_id} "
                    f"belongs to a session not yet in Odoo — leaving as customer "
                    f"credit, post-step sweep will reconcile when that session syncs.")
                return
            self._reconcile_ar(credit_note, redemption_invoice, f"CN {cn_invoice_id} (redeemed)")
            return

        if redemption_used and not used_invoice_id:
            # Cash refund
            mapping = {
                m.mssql_code: m.journal_id
                for m in self.sales_payment_method_ids
                if m.journal_id
            }
            cash_journal = mapping.get(1) or self.env['account.journal'].search([
                ('type', '=', 'cash'),
                ('company_id', '=', self.env.company.id),
            ], limit=1)
            if not cash_journal:
                raise UserError(
                    f"CN {cn_invoice_id}: no cash journal available for cash refund. "
                    f"Map PT1 (Cash) on the sync config or add a cash-type journal."
                )
            payment_register = self.env['account.payment.register'].with_context(
                active_model='account.move',
                active_ids=credit_note.ids,
                dont_redirect_to_payments=True,
            ).create({
                'payment_date': cn_date,
                'journal_id': cash_journal.id,
                'amount': credit_note.amount_total,
                'communication': f'Cash refund for CN {cn_invoice_id}',
                'group_payment': False,
            })
            payment_register.action_create_payments()
            _logger.info(
                f"CN {credit_note.name}: cash refund registered "
                f"({credit_note.amount_total} SAR)")
            return

        # Not yet redeemed — reconcile against original session if known, and
        # set reversed_entry_id as a UX breadcrumb (done *after* post so Odoo
        # doesn't re-trigger auto-reconcile).
        if original_odoo_invoice:
            self._reconcile_ar(
                credit_note, original_odoo_invoice,
                f"CN {cn_invoice_id} (outstanding, original link)")
            try:
                credit_note.sudo().write({'reversed_entry_id': original_odoo_invoice.id})
            except Exception as e:
                _logger.debug(
                    f"CN {credit_note.name}: cosmetic reversed_entry_id link skipped: {e}")
        _logger.info(
            f"CN {credit_note.name}: voucher not yet redeemed in MSSQL — customer credit")

    # ── CN SQL Queries ────────────────────────────────────────────────

    def _query_zatca_credit_notes(self, cursor, date_str, next_date):
        """Fetch CN headers for a date range. Detail lines aren't needed for
        the aggregate-line flow."""
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
                zcn.InvoiceDate
            FROM [dbo].[tblZatcaCreditNote] zcn
            WHERE zcn.InvoiceDate >= %s AND zcn.InvoiceDate < %s
            ORDER BY zcn.InvoiceID
        """, (date_str, next_date))
        rows = []
        for r in cursor.fetchall():
            rows.append({
                'InvoiceID': r['InvoiceID'],
                'ReturnInvoiceID': r['ReturnInvoiceID'],
                'SessionID': r['SessionID'],
                'CustomerID': r['CustomerID'],
                'CustomerName': r['CustomerName'],
                'PhoneNo': r['PhoneNo'],
                'Total': float(r['Total']) if r['Total'] else 0,
                'TaxAmountTotal': float(r['TaxAmountTotal']) if r['TaxAmountTotal'] else 0,
                'NetTotal': float(r['NetTotal']) if r['NetTotal'] else 0,
                'InvoiceDate': str(r['InvoiceDate']) if r['InvoiceDate'] else None,
            })
        return rows

    def _query_cn_redemptions(self, cursor, cn_invoice_ids):
        """For each CN InvoiceID, get its redemption row from tblInvoiceReturnCode,
        joined with tblCashierActivityReturnAmount to get the authoritative
        redemption SessionID (POS doesn't always backfill UsedInvoiceID on rc,
        but CRA always carries the consuming session)."""
        if not cn_invoice_ids:
            return {}
        placeholders = ','.join(['%s'] * len(cn_invoice_ids))
        cursor.execute(f"""
            SELECT rc.InvoiceID AS CN_InvoiceID,
                   rc.Used, rc.UsedInvoiceID, rc.UsedDate, rc.Canceled,
                   cra.SessionID AS UsedSessionID
            FROM [dbo].[tblInvoiceReturnCode] rc
            LEFT JOIN [dbo].[tblCashierActivityReturnAmount] cra
                ON cra.ReturnCode = rc.ReturnCode
            WHERE rc.InvoiceID IN ({placeholders})
        """, cn_invoice_ids)
        return {
            row['CN_InvoiceID']: {
                'Used': bool(row['Used']),
                'UsedInvoiceID': row['UsedInvoiceID'] or 0,
                'UsedSessionID': row['UsedSessionID'] or 0,
                'UsedDate': str(row['UsedDate']) if row['UsedDate'] else None,
                'Canceled': bool(row['Canceled']),
            }
            for row in cursor.fetchall()
        }

    def _query_original_invoice_sessions(self, cursor, original_invoice_ids):
        if not original_invoice_ids:
            return {}
        placeholders = ','.join(['%s'] * len(original_invoice_ids))
        cursor.execute(f"""
            SELECT i.InvoiceID, i.SessionID, ca.SessionDate, i.InvoiceDate
            FROM [dbo].[tblInvoice] i
            LEFT JOIN [dbo].[tblCashierActivity] ca ON i.SessionID = ca.SessionID
            WHERE i.InvoiceID IN ({placeholders})
        """, original_invoice_ids)
        return {
            row['InvoiceID']: {
                'SessionID': row['SessionID'],
                'SessionDate': row['SessionDate'],
                'InvoiceDate': row['InvoiceDate'],
            }
            for row in cursor.fetchall()
        }
