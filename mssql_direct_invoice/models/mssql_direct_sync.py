from odoo import models, fields, api
from odoo.exceptions import UserError
from datetime import date as date_type, timedelta
import pymssql
import logging

_logger = logging.getLogger(__name__)

CASH_CUSTOMER_NAME = 'عميل نقدي'  # عميل نقدي


class MssqlDirectSync(models.Model):
    _name = 'mssql.direct.sync'
    _description = 'SQL Server Direct Invoice Sync Configuration'

    # ── Connection Fields ─────────────────────────────────────────────
    name = fields.Char(string='Name', default='SQL Server Connection', required=True)
    server = fields.Char(string='Server', default='localhost', required=True)
    port = fields.Integer(string='Port', default=1433, required=True)
    database = fields.Char(string='Database', default='EPOSData', required=True)
    username = fields.Char(string='Username', default='SA', required=True)
    password = fields.Char(string='Password', required=True)
    trust_cert = fields.Boolean(string='Trust Server Certificate', default=True)

    # ── Shared Tracking Fields ────────────────────────────────────────
    products_fetched = fields.Boolean(string='Products Fetched', default=False)
    vendors_fetched = fields.Boolean(string='Vendors Fetched', default=False)
    customers_fetched = fields.Boolean(string='Customers Fetched', default=False)

    # ── Connection ────────────────────────────────────────────────────

    def _get_connection(self):
        """Create and return SQL Server connection"""
        try:
            conn = pymssql.connect(
                server=self.server,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database
            )
            return conn
        except Exception as e:
            raise UserError(f'Connection failed: {str(e)}')

    def test_connection(self):
        """Test SQL Server connection"""
        try:
            conn = self._get_connection()
            conn.close()
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': 'Connection successful!',
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            raise UserError(f'Connection test failed: {str(e)}')

    # ── Shared UI Helpers ─────────────────────────────────────────────

    def _success_notification(self, title, message):
        """Prepare success notification dict"""
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': title,
                'message': message,
                'type': 'success',
                'sticky': False,
            }
        }

    # ── Aggregate Line Products ───────────────────────────────────────

    def _get_or_create_aggregate_product(self, name):
        """Get or create a service product used as a single aggregate line."""
        product = self.env['product.product'].search([
            ('name', '=', name),
            ('type', '=', 'service'),
        ], limit=1)
        if not product:
            product = self.env['product.product'].create({
                'name': name,
                'type': 'service',
            })
            _logger.info(f"Created aggregate product '{name}' with ID {product.id}")
        return product

    def _get_or_create_pos_sales_product(self):
        return self._get_or_create_aggregate_product('POS Sales')

    def _get_or_create_pos_return_product(self):
        return self._get_or_create_aggregate_product('POS Return')

    def _get_or_create_pos_purchase_product(self):
        return self._get_or_create_aggregate_product('POS Purchase')

    # ── Tax (price-included 15%) ──────────────────────────────────────

    def _get_or_create_vat_15_inclusive(self, type_tax_use):
        """Fetch or create a price-included VAT 15% tax.

        Kept separate from the standard price-excluded VAT 15% so existing
        flows are not affected. Using price_include=True means price_unit
        equals the tax-inclusive MSSQL NetTotal exactly and Odoo splits it
        into untaxed + tax internally, so amount_total = NetTotal to the cent.
        """
        assert type_tax_use in ('sale', 'purchase'), f"Bad type_tax_use {type_tax_use!r}"
        name = f"VAT 15% Incl ({type_tax_use})"
        company = self.env.company
        tax = self.env['account.tax'].search([
            ('name', '=', name),
            ('type_tax_use', '=', type_tax_use),
            ('company_id', '=', company.id),
        ], limit=1)
        if not tax:
            vals = {
                'name': name,
                'amount': 15.0,
                'amount_type': 'percent',
                'type_tax_use': type_tax_use,
                'price_include': True,
                'company_id': company.id,
            }
            # Odoo 18 renamed the flag to `price_include_override`; keep both
            # assignments so the tax works regardless of which version the
            # running instance exposes.
            if 'price_include_override' in self.env['account.tax']._fields:
                vals['price_include_override'] = 'tax_included'
            tax = self.env['account.tax'].create(vals)
            _logger.info(f"Created tax {name} (id={tax.id})")
        return tax

    # ── Partner + guard helpers ───────────────────────────────────────

    def _get_cash_customer_partner(self):
        """Get or create the generic POS cash customer (عميل نقدي)."""
        partner = self.env['res.partner'].search(
            [('name', '=', CASH_CUSTOMER_NAME)], limit=1)
        if not partner:
            partner = self.env['res.partner'].create({
                'name': CASH_CUSTOMER_NAME,
                'customer_rank': 1,
            })
        return partner

    @staticmethod
    def _parse_mssql_date(raw):
        """Coerce MSSQL-provided date/datetime/str into a python date."""
        if raw is None:
            return False
        if isinstance(raw, str):
            return date_type.fromisoformat(raw[:10])
        if hasattr(raw, 'date'):
            return raw.date()
        return raw

    # ── Daily automatic sync ──────────────────────────────────────────

    @api.model
    def cron_daily_sync(self):
        """Cron entry point: sync yesterday's sales + purchases for every
        configured connection. Failures on one config don't block the others.

        `create_session_based_invoices` auto-chains to `create_sales_credit_notes`
        for the same date, so this call covers sessions + CNs + purchase bills
        in one pass.
        """
        target_date = fields.Date.today() - timedelta(days=1)
        configs = self.env['mssql.direct.sync'].search([])
        if not configs:
            _logger.info("cron_daily_sync: no sync configurations, skipping")
            return

        for config in configs:
            _logger.info(
                f"cron_daily_sync: running {config.name} for {target_date}")
            try:
                config.create_session_based_invoices(target_date)
            except Exception as e:
                _logger.error(
                    f"cron_daily_sync[{config.name}] sales sync failed "
                    f"for {target_date}: {e}")
            try:
                config.with_context(
                    purchase_invoice_date=target_date
                ).write({'purchase_invoice_date': target_date})
                config.sync_purchase_invoices()
            except Exception as e:
                _logger.error(
                    f"cron_daily_sync[{config.name}] purchase sync failed "
                    f"for {target_date}: {e}")

    @staticmethod
    def _assert_total_matches(move, mssql_total, label, tolerance=0.01):
        """Raise UserError if move.amount_total drifts from the MSSQL target.

        The aggregate-line design means any drift is a bug (either tax setup
        or MSSQL data issue), not sub-cent rounding noise — so we fail the
        queue line loudly instead of silently plugging with a decimal line.
        """
        diff = round(float(move.amount_total) - float(mssql_total), 2)
        if abs(diff) >= tolerance:
            raise UserError(
                f"{label}: Odoo amount_total={move.amount_total} does not match "
                f"MSSQL NetTotal={mssql_total} (diff={diff}). Aborting."
            )

    # ── Sync Log Helpers ──────────────────────────────────────────────

    def _is_already_synced(self, sync_type, mssql_id, mssql_table):
        """Check if a record has already been synced (idempotency check)."""
        return bool(self.env['mssql.direct.sync.log'].search_count([
            ('sync_type', '=', sync_type),
            ('mssql_id', '=', str(mssql_id)),
            ('mssql_table', '=', mssql_table),
            ('status', '=', 'success'),
        ], limit=1))

    def _log_sync(self, sync_type, mssql_id, mssql_table, odoo_model=False,
                  odoo_record_id=False, status='success', error_message=False, notes=False):
        """Create a sync log entry."""
        return self.env['mssql.direct.sync.log'].create({
            'sync_type': sync_type,
            'mssql_id': str(mssql_id),
            'mssql_table': mssql_table,
            'odoo_model': odoo_model or '',
            'odoo_record_id': odoo_record_id or 0,
            'status': status,
            'error_message': error_message or '',
            'notes': notes or '',
        })

    def _get_synced_ids(self, sync_type, mssql_table):
        """Get all successfully synced MSSQL IDs for a given type/table."""
        logs = self.env['mssql.direct.sync.log'].search([
            ('sync_type', '=', sync_type),
            ('mssql_table', '=', mssql_table),
            ('status', '=', 'success'),
        ])
        return set(logs.mapped('mssql_id'))

    # ── Change Detection Helper ───────────────────────────────────────

    def _has_record_changed(self, record, new_vals, skip_fields=None):
        """Check if record values have actually changed (DRY helper)"""
        if skip_fields is None:
            skip_fields = []

        for field, new_value in new_vals.items():
            if field in skip_fields:
                continue

            current_value = getattr(record, field, None)

            # Handle None/False comparison
            if current_value in [None, False] and new_value in [None, False]:
                continue

            # Handle float comparison with tolerance
            if isinstance(new_value, (int, float)):
                if abs(float(current_value or 0) - float(new_value)) > 0.01:
                    return True
            elif str(current_value or '').strip() != str(new_value or '').strip():
                return True

        return False

    # ── Generic Partner Sync ──────────────────────────────────────────

    def _generic_partner_sync(self, sql_records, sql_id_field, odoo_id_field, partner_type, field_mapping, only_new=False):
        """Generic partner sync logic - works for vendors and customers"""
        partner_obj = self.env['res.partner']

        sql_ids = [r[sql_id_field] for r in sql_records if r.get(sql_id_field)]
        if not sql_ids:
            _logger.info(f'No {partner_type}s with {sql_id_field} found in MSSQL')
            return 0, 0, 0

        rank_field = 'supplier_rank' if partner_type == 'supplier' else 'customer_rank'
        existing_partners = {
            getattr(p, odoo_id_field): p for p in partner_obj.search([
                (odoo_id_field, 'in', sql_ids),
                (rank_field, '>', 0)
            ])
        }

        to_create = []
        to_update = []
        skipped = 0

        for record in sql_records:
            record_id = record[sql_id_field]
            if not record_id:
                continue

            vals = {
                'name': record[field_mapping['name']] or f"{partner_type.title()} {record_id}",
                odoo_id_field: record_id,
                rank_field: 1,
            }
            # POS suppliers are business entities (VAT/CR/registered address) so
            # always come in as companies. Customers default to person; promote
            # to company only when the row carries business identifiers (VAT
            # or CR number — typical of B2B clients).
            if partner_type == 'supplier':
                vals['company_type'] = 'company'
            else:
                cust_vat_field = field_mapping.get('vat')
                cust_cr_field = field_mapping.get('company_registry')
                has_vat = cust_vat_field and record.get(cust_vat_field)
                has_cr = cust_cr_field and record.get(cust_cr_field)
                if has_vat or has_cr:
                    vals['company_type'] = 'company'

            for odoo_field, sql_field in field_mapping.items():
                if odoo_field == 'name':
                    continue

                if isinstance(sql_field, dict):
                    if '_concat' in sql_field:
                        parts = [str(record.get(f, '') or '').strip() for f in sql_field['_concat']]
                        parts = [p for p in parts if p]
                        if parts:
                            vals[odoo_field] = ' / '.join(parts)
                    elif '_combine' in sql_field:
                        parts = [str(record.get(f, '') or '').strip() for f in sql_field['_combine']]
                        parts = [p for p in parts if p]
                        if parts:
                            vals[odoo_field] = ', '.join(parts)
                    elif '_note' in sql_field:
                        note_parts = []
                        for label, field_name in sql_field['_note']:
                            value = str(record.get(field_name, '') or '').strip()
                            if value:
                                note_parts.append(f"{label}: {value}")
                        if note_parts:
                            vals[odoo_field] = '\n'.join(note_parts)
                elif isinstance(sql_field, list):
                    for sf in sql_field:
                        if record.get(sf):
                            vals[odoo_field] = record[sf]
                            break
                elif record.get(sql_field):
                    value = record[sql_field]
                    if odoo_field == 'ref':
                        value = str(value)
                    vals[odoo_field] = value

            if record_id in existing_partners:
                if only_new:
                    skipped += 1
                    continue
                to_update.append((existing_partners[record_id], vals))
            else:
                to_create.append(vals)

        created = 0
        if to_create:
            _logger.info(f"Creating {len(to_create)} new {partner_type}s in batches...")
            batch_size = 2000
            fast_create = partner_obj.with_context(
                tracking_disable=True,
                mail_create_nolog=True,
                mail_create_nosubscribe=True,
                mail_notrack=True,
                no_vat_validation=True,
            )
            for i in range(0, len(to_create), batch_size):
                batch = to_create[i:i + batch_size]
                fast_create.create(batch)
                created += len(batch)
                _logger.info(f"{partner_type.title()} creation progress: {created}/{len(to_create)}")
                self.env.cr.commit()
                self.env.clear()

        updated = 0
        if to_update:
            update_map = {p.id: vals for p, vals in to_update}
            batch_size = 1000
            partner_ids = list(update_map.keys())
            for i in range(0, len(partner_ids), batch_size):
                batch_ids = partner_ids[i:i + batch_size]
                batch_partners = partner_obj.browse(batch_ids)
                for partner in batch_partners:
                    partner.write(update_map[partner.id])
                updated += len(batch_ids)
                if i % (batch_size * 10) == 0:
                    self.env.clear()

        return created, updated, skipped

    # ── Product Lookup Helper ─────────────────────────────────────────

    def _get_product_map(self, item_ids=None):
        """Build a dict mapping x_sql_item_id -> product.product record."""
        domain = [('x_sql_item_id', '!=', False)]
        if item_ids:
            domain.append(('x_sql_item_id', 'in', list(item_ids)))
        products = self.env['product.product'].search(domain)
        return {p.x_sql_item_id: p for p in products}
