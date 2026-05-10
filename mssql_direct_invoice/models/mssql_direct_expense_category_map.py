from odoo import models, fields


class MssqlExpenseCategoryMap(models.Model):
    _name = 'mssql.expense.category.map'
    _description = 'MSSQL Expense Category → Odoo Account'
    _rec_name = 'mssql_cat_name'
    _order = 'mssql_cat_id'

    sync_config_id = fields.Many2one(
        'mssql.direct.sync', required=True, ondelete='cascade')
    mssql_cat_id = fields.Integer(
        string='MSSQL Category ID', required=True, readonly=True)
    mssql_cat_name = fields.Char(
        string='MSSQL Category Name', readonly=True)
    account_id = fields.Many2one(
        'account.account', string='Odoo Expense Account',
        help="Map this MSSQL category to a specific Odoo account. Leave "
             "empty to fall back to the Default Expense Account on the "
             "sync configuration.")

    _sql_constraints = [
        ('uniq_cat_per_config',
         'UNIQUE(sync_config_id, mssql_cat_id)',
         'Each MSSQL category can be mapped only once per sync config.'),
    ]
