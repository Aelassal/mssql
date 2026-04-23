from odoo import models, fields


class MssqlDirectPaymentMethod(models.Model):
    _name = 'mssql.direct.payment.method'
    _description = 'MSSQL Direct Sync Payment Method Mapping'
    _order = 'scope, mssql_code'

    sync_config_id = fields.Many2one(
        'mssql.direct.sync', string='Sync Configuration',
        required=True, ondelete='cascade', index=True)

    scope = fields.Selection([
        ('sales', 'Sales'),
        ('purchase', 'Purchase'),
    ], string='Scope', required=True, index=True)

    mssql_code = fields.Integer(
        string='MSSQL Code', required=True,
        help='tblPaymentType.PaymentTypeID (sales) or tblSuppliersPayment.PaymentMethod (purchase)')

    name = fields.Char(string='Name', required=True)

    journal_id = fields.Many2one(
        'account.journal', string='Journal',
        help='Journal used when registering payments of this type.')

    _sql_constraints = [
        ('uniq_config_scope_code',
         'UNIQUE(sync_config_id, scope, mssql_code)',
         'Payment method code must be unique per (config, scope).'),
    ]
