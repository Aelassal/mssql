from odoo import models, fields


class MssqlDirectSyncLog(models.Model):
    _name = 'mssql.direct.sync.log'
    _description = 'MSSQL Direct Sync Log - Tracks synced records for idempotency'
    _order = 'sync_date desc'

    sync_type = fields.Selection([
        ('product', 'Product'),
        ('vendor', 'Vendor'),
        ('customer', 'Customer'),
        ('sales_invoice', 'Sales Invoice'),
        ('purchase_bill', 'Purchase Bill'),
    ], string='Sync Type', required=True, index=True)

    mssql_id = fields.Char(
        string='MSSQL Record ID', required=True, index=True,
        help='Primary key from the MSSQL source table')

    mssql_table = fields.Char(
        string='MSSQL Source Table', required=True,
        help='Name of the MSSQL source table')

    odoo_model = fields.Char(
        string='Odoo Model',
        help='Model of the created Odoo record (account.move)')

    odoo_record_id = fields.Integer(
        string='Odoo Record ID',
        help='ID of the created Odoo record')

    sync_date = fields.Datetime(
        string='Sync Date', default=fields.Datetime.now, required=True)

    status = fields.Selection([
        ('success', 'Success'),
        ('error', 'Error'),
        ('skipped', 'Skipped'),
    ], string='Status', default='success', index=True)

    error_message = fields.Text(string='Error Message')
    notes = fields.Text(string='Notes')
