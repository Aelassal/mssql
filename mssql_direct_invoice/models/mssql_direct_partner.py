from odoo import models
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)


class MssqlDirectPartner(models.Model):
    _inherit = 'mssql.direct.sync'

    # ── Vendor / Customer Sync ──────────────────────────────────────────

    def sync_vendors(self):
        """Fetch vendors from SQL Server and create in Odoo.

        Also performs a one-shot legacy heal at the top: any previously-synced
        vendor still flagged as an individual (is_company=False) is promoted
        to company. Suppliers in POS data are business entities — earlier
        versions of this module didn't set company_type at create time, so
        existing records need this corrective pass.
        """
        # ── Heal legacy vendors created before company_type was set ───────
        legacy_individual_vendors = self.env['res.partner'].search([
            ('x_sql_vendor_id', '!=', False),
            ('supplier_rank', '>', 0),
            ('is_company', '=', False),
        ])
        legacy_fixed = 0
        if legacy_individual_vendors:
            legacy_individual_vendors.write({'company_type': 'company'})
            legacy_fixed = len(legacy_individual_vendors)
            _logger.info(
                f"sync_vendors: healed {legacy_fixed} legacy vendor(s) "
                f"from individual → company")

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            sql_vendors = self._query_vendors(cursor)
            conn.close()

            if not sql_vendors:
                _logger.info('No vendors found in SQL Server')
                msg = 'No vendors found'
                if legacy_fixed:
                    msg += f' (healed {legacy_fixed} legacy individual → company)'
                return self._success_notification('Vendor Sync Complete', msg)

            field_mapping = {
                'name': 'SupplierName',
                'ref': 'SupplierID',
                'street': 'SupplierAddress',
                'street2': {'_combine': ['StreetName', 'BuildingNo', 'Area', 'POBox']},
                'phone': {'_concat': ['Phone1', 'Phone2']},
                'mobile': 'Mobile',
                'email': 'EMailAdress',
                'website': 'WebSite',
                'vat': 'SuppliervatNumber',
                'city': 'City',
                'zip': 'PostalZone',
                'comment': {'_note': [('Note', 'SupplierNote'), ('Representative', 'RepresentativeName')]},
                'company_registry': 'CRNO',
            }

            created, updated, skipped = self._generic_partner_sync(
                sql_records=sql_vendors,
                sql_id_field='SupplierID',
                odoo_id_field='x_sql_vendor_id',
                partner_type='supplier',
                field_mapping=field_mapping,
                only_new=True
            )

            if created > 0:
                self.write({'vendors_fetched': True})

            heal_note = f' Healed {legacy_fixed} legacy individual → company.' if legacy_fixed else ''
            if created == 0:
                return self._success_notification(
                    'Vendor Sync Complete',
                    f'No new vendors found (checked: {len(sql_vendors)} vendors, '
                    f'{skipped} already exist).{heal_note}'
                )
            return self._success_notification(
                'Vendor Sync Complete',
                f'Created: {created} new vendors ({skipped} already existed).{heal_note}'
            )
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Vendor sync failed: {str(e)}')

    def sync_customers(self):
        """Fetch customers from SQL Server and create in Odoo"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            sql_customers = self._query_customers(cursor)
            conn.close()

            if not sql_customers:
                _logger.info('No customers found in SQL Server')
                return self._success_notification('Customer Sync Complete', 'No customers found')

            field_mapping = {
                'name': 'CustomerName',
                'street': 'CustomerAddress',
                'phone': ['Phone1', 'Phone2'],
                'mobile': 'Mobile',
                'email': 'EMail',
                'website': 'WebSite',
                'vat': 'CustVatNumber',
                'city': 'City',
                'zip': 'PostalZone',
                'comment': 'CustomerNote',
                'company_registry': 'CRNo',
                'credit_limit': 'CreditLimit',
            }

            created, updated, skipped = self._generic_partner_sync(
                sql_records=sql_customers,
                sql_id_field='CustomerID',
                odoo_id_field='x_sql_customer_id',
                partner_type='customer',
                field_mapping=field_mapping,
                only_new=True
            )

            if created > 0:
                self.write({'customers_fetched': True})

            if created == 0:
                return self._success_notification('Customer Sync Complete', f'No new customers found (checked: {len(sql_customers)} customers, {skipped} already exist)')
            else:
                return self._success_notification('Customer Sync Complete', f'Created: {created} new customers ({skipped} already existed)')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Customer sync failed: {str(e)}')

    # ── SQL Queries ─────────────────────────────────────────────────────

    def _query_vendors(self, cursor):
        """Fetch vendors from MSSQL"""
        cursor.execute("""
            SELECT
                SupplierID,
                SupplierName,
                SupplierAddress,
                Phone1,
                Phone2,
                Mobile,
                Fax,
                EMailAdress,
                WebSite,
                SuppliervatNumber,
                CRNO,
                City,
                StreetName,
                BuildingNo,
                PostalZone,
                POBox,
                Area,
                SupplierAccountNumber,
                SupplierIBAN,
                SupplierBankName,
                SupplierNote,
                RepresentativeName
            FROM [dbo].[tblSuppliers]
        """)
        return cursor.fetchall()

    def _query_customers(self, cursor):
        """Fetch customers from MSSQL"""
        cursor.execute("""
            SELECT
                CustomerID,
                CardNumber,
                CustomerName,
                CustomerAddress,
                Phone1,
                Phone2,
                Mobile,
                Fax,
                EMail,
                WebSite,
                CustVatNumber,
                CRNo,
                City,
                StreetName,
                BuildingNo,
                PostalZone,
                POBox,
                Area,
                ContactPerson,
                CustomerNote,
                CreditLimit,
                CustomerBalance
            FROM [dbo].[tblCustomers]
        """)
        return cursor.fetchall()
