from odoo import models, fields
from odoo.exceptions import UserError
from datetime import timedelta
import logging

_logger = logging.getLogger(__name__)


class MssqlDirectOperations(models.TransientModel):
    _name = 'mssql.direct.operations'
    _description = 'MSSQL Direct Sync Operations Wizard'

    sync_config_id = fields.Many2one(
        'mssql.direct.sync', string='Sync Configuration', required=True,
        default=lambda self: self.env['mssql.direct.sync'].search([], limit=1).id)

    operation = fields.Selection([
        ('sync_products', 'Import Products'),
        ('sync_vendors', 'Import Vendors'),
        ('sync_customers', 'Import Customers'),
        ('sync_sales_invoices', 'Import Sales Invoices (Direct)'),
        ('sync_sales_credit_notes', 'Import Sales Credit Notes'),
        ('sync_purchase_bills', 'Import Purchase Bills (Direct)'),
        ('update_products', 'Update Products (Prices + Barcode)'),
    ], string='Operation', required=True)

    date_from = fields.Date(string='Date From')
    date_to = fields.Date(string='Date To')

    def execute(self):
        """Execute the selected operation."""
        self.ensure_one()
        config = self.sync_config_id
        if not config:
            raise UserError('Please select a Sync Configuration.')

        op = self.operation
        _logger.info(f"Direct Operations Wizard: executing '{op}' on config '{config.name}'")

        if op == 'sync_products':
            return config.sync_products()

        elif op == 'sync_vendors':
            return config.sync_vendors()

        elif op == 'sync_customers':
            return config.sync_customers()

        elif op == 'update_products':
            return config.action_update_products()

        elif op == 'sync_sales_invoices':
            if not self.date_from:
                raise UserError('Please specify a Date From for sales invoice import.')
            date_from = self.date_from
            date_to = self.date_to or self.date_from
            current_date = date_from
            results = []
            while current_date <= date_to:
                try:
                    result = config.create_session_based_invoices(current_date)
                    results.append(f"{current_date}: OK")
                except Exception as e:
                    results.append(f"{current_date}: Error - {str(e)}")
                current_date += timedelta(days=1)
            summary = '\n'.join(results)
            return config._success_notification('Sales Invoice Import', summary)

        elif op == 'sync_sales_credit_notes':
            if not self.date_from:
                raise UserError('Please specify a Date From for credit note import.')
            date_from = self.date_from
            date_to = self.date_to or self.date_from
            current_date = date_from
            results = []
            while current_date <= date_to:
                try:
                    config.create_sales_credit_notes(current_date)
                    results.append(f"{current_date}: OK")
                except Exception as e:
                    results.append(f"{current_date}: Error - {str(e)}")
                current_date += timedelta(days=1)
            summary = '\n'.join(results)
            return config._success_notification('Sales Credit Note Import', summary)

        elif op == 'sync_purchase_bills':
            if not self.date_from:
                raise UserError('Please specify a Date From for purchase bill import.')
            date_from = self.date_from
            date_to = self.date_to or self.date_from
            current_date = date_from
            results = []
            while current_date <= date_to:
                try:
                    config.write({'purchase_invoice_date': current_date})
                    result = config.sync_purchase_invoices()
                    results.append(f"{current_date}: OK")
                except Exception as e:
                    results.append(f"{current_date}: Error - {str(e)}")
                current_date += timedelta(days=1)
            summary = '\n'.join(results)
            return config._success_notification('Purchase Bill Import', summary)

        else:
            raise UserError(f'Unknown operation: {op}')
