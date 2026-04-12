from odoo import api, fields, models


class ReportLineAnnotation(models.Model):
    _name = 'report.line.annotation'
    _description = 'Report Line Annotation'
    _order = 'create_date desc'

    report_type = fields.Selection([
        ('general_ledger', 'General Ledger'),
        ('partner_ledger', 'Partner Ledger'),
        ('trial_balance', 'Trial Balance'),
        ('partner_ageing', 'Partner Ageing'),
        ('analytic_report', 'Analytic Report'),
        ('financial_report', 'Financial Report'),
    ], string='Report Type', required=True, index=True)
    line_ref = fields.Char(string='Line Reference', required=True, index=True,
        help='Unique identifier for the report line (e.g., account_id, partner_id)')
    note = fields.Text(string='Note', required=True)
    user_id = fields.Many2one('res.users', string='User', default=lambda self: self.env.user, required=True)
    company_id = fields.Many2one('res.company', string='Company', default=lambda self: self.env.company)
    date = fields.Date(string='Date', default=fields.Date.today)

    @api.model
    def get_annotations(self, report_type, line_refs):
        """Get annotations for a list of line references."""
        annotations = self.search([
            ('report_type', '=', report_type),
            ('line_ref', 'in', line_refs),
            ('company_id', '=', self.env.company.id),
        ])
        result = {}
        for ann in annotations:
            if ann.line_ref not in result:
                result[ann.line_ref] = []
            result[ann.line_ref].append({
                'id': ann.id,
                'note': ann.note,
                'user': ann.user_id.name,
                'date': str(ann.date),
            })
        return result

    @api.model
    def save_annotation(self, report_type, line_ref, note):
        """Save or update an annotation."""
        existing = self.search([
            ('report_type', '=', report_type),
            ('line_ref', '=', line_ref),
            ('user_id', '=', self.env.uid),
            ('company_id', '=', self.env.company.id),
        ], limit=1)
        if existing:
            if note:
                existing.write({'note': note, 'date': fields.Date.today()})
                return existing.id
            else:
                existing.unlink()
                return False
        elif note:
            new = self.create({
                'report_type': report_type,
                'line_ref': line_ref,
                'note': note,
            })
            return new.id
        return False

    @api.model
    def delete_annotation(self, annotation_id):
        """Delete an annotation."""
        ann = self.browse(annotation_id)
        if ann.exists() and ann.user_id.id == self.env.uid:
            ann.unlink()
            return True
        return False
