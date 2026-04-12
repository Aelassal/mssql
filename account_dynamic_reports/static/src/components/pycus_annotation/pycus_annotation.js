/** @odoo-module **/

import { Component, useState, useRef } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

export class PycusAnnotation extends Component {
    setup() {
        this.ormService = useService("orm");
        this.state = useState({
            isEditing: false,
            note: this.props.note || '',
            editNote: this.props.note || '',
            annotations: this.props.annotations || [],
        });
    }

    toggleEdit() {
        this.state.isEditing = !this.state.isEditing;
        if (this.state.isEditing) {
            this.state.editNote = this.state.note;
        }
    }

    async saveNote() {
        const result = await this.ormService.call(
            'report.line.annotation', 'save_annotation',
            [this.props.reportType, String(this.props.lineRef), this.state.editNote]
        );
        this.state.note = this.state.editNote;
        this.state.isEditing = false;
        if (this.props.onSave) {
            this.props.onSave(this.props.lineRef, this.state.editNote);
        }
    }

    async deleteNote() {
        if (this.state.note) {
            await this.ormService.call(
                'report.line.annotation', 'save_annotation',
                [this.props.reportType, String(this.props.lineRef), '']
            );
            this.state.note = '';
            this.state.editNote = '';
            this.state.isEditing = false;
        }
    }

    get hasNote() {
        return !!this.state.note;
    }
}

PycusAnnotation.template = 'account_dynamic_reports.PycusAnnotation';
PycusAnnotation.props = {
    reportType: String,
    lineRef: { type: [String, Number] },
    note: { type: String, optional: true },
    annotations: { type: Array, optional: true },
    onSave: { type: Function, optional: true },
};
