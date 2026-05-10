from odoo import models, fields
from odoo.exceptions import UserError
from datetime import timedelta
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlDirectExpense(models.Model):
    _inherit = 'mssql.direct.sync'

    # ── Category Fetch ────────────────────────────────────────────────

    def action_fetch_expense_categories(self):
        """Pull tblExpenseCat into the O2M mapping list. Existing rows keep
        their account assignment; new categories are appended with no
        account_id (user fills in)."""
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT ExpenseCatID, ExpenseCat
                FROM [dbo].[tblExpenseCat]
                ORDER BY ExpenseCatID
            """)
            rows = cursor.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        existing = {m.mssql_cat_id: m for m in self.expense_category_map_ids}
        to_create = []
        for row in rows:
            cat_id = int(row['ExpenseCatID'])
            cat_name = (row.get('ExpenseCat') or '').strip()
            if cat_id in existing:
                if cat_name and existing[cat_id].mssql_cat_name != cat_name:
                    existing[cat_id].mssql_cat_name = cat_name
                continue
            to_create.append({
                'sync_config_id': self.id,
                'mssql_cat_id': cat_id,
                'mssql_cat_name': cat_name,
            })

        if to_create:
            self.env['mssql.expense.category.map'].create(to_create)

        self.expenses_fetched = True
        _logger.info(
            f"Fetched {len(rows)} expense categories "
            f"({len(to_create)} new, {len(rows) - len(to_create)} existing)")
        return self._success_notification(
            'Expense Categories',
            f'Fetched {len(rows)} categories ({len(to_create)} new). '
            f'Map any unmapped categories to an Odoo account before syncing.')

    # ── Entry Point ───────────────────────────────────────────────────

    def sync_expenses(self, target_date=None):
        """Cron + wizard callable. Pulls tblExpense rows for the given date,
        queues one mssql.direct.sync.queue.line per row with sync_type
        'expense', and kicks off processing. Idempotent via
        ref = 'MSSQL-EXP-{ExpenseID}'.

        target_date defaults to yesterday when not provided (cron path).
        """
        self.ensure_one()
        if target_date is None:
            target_date = fields.Date.today() - timedelta(days=1)

        self._validate_expense_config()

        date_str = target_date.strftime('%Y-%m-%d')
        next_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

        _logger.info("=" * 80)
        _logger.info(f"EXPENSE SYNC FOR DATE: {date_str}")
        _logger.info("=" * 80)

        rows = self._query_expenses_for_date(date_str, next_date)
        if not rows:
            _logger.info(f"sync_expenses: no expenses for {date_str}")
            return self._success_notification(
                'Expense Sync', f'No expenses for {date_str}')

        existing_refs = set(self.env['account.move'].search([
            ('ref', '=like', 'MSSQL-EXP-%'),
        ]).mapped('ref'))

        queue = self.env['mssql.direct.sync.queue'].create({
            'sync_config_id': self.id,
            'sync_type': 'expense',
            'sync_date': target_date,
        })

        line_vals_list = []
        skipped_existing = 0
        for row in rows:
            expense_id = row['ExpenseID']
            ref = f"MSSQL-EXP-{expense_id}"
            if ref in existing_refs:
                skipped_existing += 1
                continue

            shop = (row.get('ShopName') or '').strip() or 'Unknown'
            line_vals_list.append({
                'queue_id': queue.id,
                'name': f"Expense {expense_id} - {shop}",
                'mssql_id': str(expense_id),
                'mssql_table': 'tblExpense',
                'record_data': json.dumps(dict(row), default=str),
            })

        if skipped_existing:
            _logger.info(
                f"Skipped {skipped_existing} already-imported expenses")

        if not line_vals_list:
            queue.unlink()
            return self._success_notification(
                'Expense Sync',
                f'All {skipped_existing} expenses for {date_str} are already imported.')

        self.env['mssql.direct.sync.queue.line'].create(line_vals_list)
        _logger.info(f"Queue {queue.name}: {len(line_vals_list)} lines")

        queue.action_process_queue()

        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mssql.direct.sync.queue',
            'res_id': queue.id,
            'view_mode': 'form',
            'target': 'current',
        }

    # ── Queue Line Processor ──────────────────────────────────────────

    def _process_queue_expense(self, data, queue_line):
        """Build a balanced account.move(move_type='entry') for one MSSQL
        expense row. Negative rows are silently flipped (debit ↔ credit).
        VAT line is omitted when TaxAmount is 0."""
        ref = f"MSSQL-EXP-{data['ExpenseID']}"

        # Idempotency guard inside the processor as well
        existing = self.env['account.move'].search(
            [('ref', '=', ref)], limit=1)
        if existing:
            _logger.info(
                f"Skipping {ref} — already imported as {existing.name}")
            return {
                'model': 'account.move',
                'id': existing.id,
                'skipped': True,
            }

        cat_to_account = self._build_expense_cat_index()
        expense_amount = float(data.get('ExpenseAmount') or 0)
        tax_amount = float(data.get('TaxAmount') or 0)
        net_amount = float(data.get('NetExpenseAmount') or 0)
        shop = (data.get('ShopName') or '').strip()

        if expense_amount == 0 and net_amount == 0:
            raise UserError(f"ExpenseID {data['ExpenseID']}: zero-amount row")

        # ZATCA tax settlement detection: negative-amount rows from the
        # tax authority are VAT/tax payments, not vendor refunds. Route
        # them through tax_settlement_account_id instead of the silent
        # debit/credit flip used for regular returns.
        is_zatca_settlement = (
            net_amount < 0 and 'الزكاة' in shop)
        if is_zatca_settlement:
            return self._post_zatca_settlement(data, ref, abs(net_amount))

        is_refund = net_amount < 0
        expense_amount = abs(expense_amount)
        tax_amount = abs(tax_amount)
        net_amount = abs(net_amount)

        expense_account = self._resolve_expense_account(
            data.get('ExpenseCatID'), cat_to_account)
        if not expense_account:
            raise UserError(
                f"ExpenseID {data['ExpenseID']}: no account mapped for "
                f"category {data.get('ExpenseCatID')} and no Default "
                f"Expense Account configured.")

        if tax_amount > 0 and not self.vat_input_account_id:
            raise UserError(
                f"ExpenseID {data['ExpenseID']} has VAT {tax_amount} but no "
                f"VAT Input account is configured.")

        expense_date = self._parse_mssql_date(data.get('ExpenseDate'))
        invoice_date = self._parse_mssql_date(data.get('InvoiceDate'))
        shop = (data.get('ShopName') or '').strip()
        invoice_id = (data.get('InvoiceID') or '').strip()
        descr = (data.get('ExpenseDescreption') or '').strip()
        note = (data.get('ExpenseNote') or '').strip()

        narration = (
            f"Shop: {shop}\n"
            f"Invoice: {invoice_id}"
            + (f" ({invoice_date})" if invoice_date else "")
            + f"\n{descr}"
            + (f"\n{note}" if note else "")
            + ("\n[REVERSAL — original NetTotal was negative]" if is_refund else "")
        )

        line_label = (descr or shop or f"Expense {data['ExpenseID']}")[:200]

        if is_refund:
            expense_dr, expense_cr = 0.0, expense_amount
            vat_dr, vat_cr = 0.0, tax_amount
            cp_dr, cp_cr = net_amount, 0.0
        else:
            expense_dr, expense_cr = expense_amount, 0.0
            vat_dr, vat_cr = tax_amount, 0.0
            cp_dr, cp_cr = 0.0, net_amount

        line_vals = [
            (0, 0, {
                'account_id': expense_account.id,
                'name': line_label,
                'debit': expense_dr,
                'credit': expense_cr,
            }),
        ]
        if tax_amount > 0:
            tax_pct = float(data.get('TaxPercent') or 0) * 100
            line_vals.append((0, 0, {
                'account_id': self.vat_input_account_id.id,
                'name': f"VAT {tax_pct:.2f}%",
                'debit': vat_dr,
                'credit': vat_cr,
            }))
        line_vals.append((0, 0, {
            'account_id': self.expense_counterpart_account_id.id,
            'name': f"Paid: {shop}" if shop else "Expense counterpart",
            'debit': cp_dr,
            'credit': cp_cr,
        }))

        move = self.env['account.move'].create({
            'move_type': 'entry',
            'journal_id': self.expense_journal_id.id,
            'date': expense_date,
            'ref': ref,
            'narration': narration,
            'line_ids': line_vals,
        })
        move.action_post()
        return {
            'model': 'account.move',
            'id': move.id,
        }

    def _post_zatca_settlement(self, data, ref, amount):
        """Post a tax-authority settlement entry:
            Dr Tax Settlement Account
            Cr Counterpart (Cash/Bank)
        Used for negative ZATCA rows where the company actually paid VAT
        to the authority — the MSSQL row records it as a negative expense
        but the real-world event is a tax payment, not a vendor refund.
        """
        if not self.tax_settlement_account_id:
            raise UserError(
                f"ExpenseID {data['ExpenseID']} is a ZATCA tax settlement "
                f"({amount}) but no Tax Settlement Account is configured "
                f"on the sync configuration.")

        expense_date = self._parse_mssql_date(data.get('ExpenseDate'))
        invoice_date = self._parse_mssql_date(data.get('InvoiceDate'))
        shop = (data.get('ShopName') or '').strip()
        invoice_id = (data.get('InvoiceID') or '').strip()
        descr = (data.get('ExpenseDescreption') or '').strip()
        note = (data.get('ExpenseNote') or '').strip()

        narration = (
            f"[ZATCA TAX SETTLEMENT — Dr Tax Settlement / Cr Cash]\n"
            f"Authority: {shop}\n"
            f"Reference: {invoice_id}"
            + (f" ({invoice_date})" if invoice_date else "")
            + f"\n{descr}"
            + (f"\n{note}" if note else "")
        )

        line_label = (descr or shop or f"Tax settlement {data['ExpenseID']}")[:200]

        line_vals = [
            (0, 0, {
                'account_id': self.tax_settlement_account_id.id,
                'name': line_label,
                'debit': amount,
                'credit': 0.0,
            }),
            (0, 0, {
                'account_id': self.expense_counterpart_account_id.id,
                'name': f"Paid to: {shop}" if shop else "Tax authority",
                'debit': 0.0,
                'credit': amount,
            }),
        ]

        move = self.env['account.move'].create({
            'move_type': 'entry',
            'journal_id': self.expense_journal_id.id,
            'date': expense_date,
            'ref': ref,
            'narration': narration,
            'line_ids': line_vals,
        })
        move.action_post()
        return {
            'model': 'account.move',
            'id': move.id,
        }

    # ── Helpers ───────────────────────────────────────────────────────

    def _validate_expense_config(self):
        missing = []
        if not self.expense_journal_id:
            missing.append('Expense Journal')
        if not self.expense_counterpart_account_id:
            missing.append('Counterpart Account')
        if not self.expense_default_account_id:
            missing.append('Default Expense Account')
        if missing:
            raise UserError(
                'Expense sync requires the following fields on the sync '
                f'configuration: {", ".join(missing)}. '
                'Set them under the "Expense Configuration" page.')

    def _build_expense_cat_index(self):
        """Return {mssql_cat_id: account.account record} from the mapping
        O2M, falling back to the default account when a category is
        unmapped."""
        index = {}
        for m in self.expense_category_map_ids:
            if m.account_id:
                index[m.mssql_cat_id] = m.account_id
        return index

    def _resolve_expense_account(self, mssql_cat_id, cat_to_account):
        return cat_to_account.get(mssql_cat_id) or self.expense_default_account_id

    def _query_expenses_for_date(self, date_str, next_date):
        """Pull tblExpense rows for the date window [date_str, next_date)."""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT
                    ExpenseID, ExpenseCatID, ExpenseDate,
                    ExpenseDescreption, ExpenseNote,
                    ExpenseAmount, TaxPercent, TaxAmount, NetExpenseAmount,
                    InvoiceID, InvoiceDate, ShopName,
                    IncludeVat, ModifiedBy, ModifiedDate
                FROM [dbo].[tblExpense]
                WHERE ExpenseDate >= %s AND ExpenseDate < %s
                ORDER BY ExpenseID
            """, (date_str, next_date))
            return cursor.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass
