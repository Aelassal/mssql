/** @odoo-module **/

import { registry } from '@web/core/registry';
import { loadJS } from "@web/core/assets";
import { _t } from "@web/core/l10n/translation";
import { parseDate, formatDate } from "@web/core/l10n/dates";
import { formatFloat, formatFloatTime, formatMonetary } from "@web/views/fields/formatters";
import {
    Component,
    EventBus,
    onWillStart,
    onMounted,
    status,
    useEffect,
    useExternalListener,
    useRef,
    useState,
    useChildSubEnv,
} from "@odoo/owl";
import { useService,  } from "@web/core/utils/hooks";
import { PycusFilters } from "../pycus_filters/pycus_filters";

export class PycusFinancialReportFilters extends PycusFilters {
    setup(){
        super.setup();
        this.state.defaultJournalValues = this.props.filterValues.defaultJournalValues
        this.state.defaultAccountTagValues = this.props.filterValues.defaultAccountTagValues;
        this.state.comparison_date_range = {
                choices: [],
                selectedValue: {}
            }
        this.comparison_date_from = ''
        this.comparison_date_to = ''
        this.state.hide_zero_balance = {
                choices: [],
                selectedValue: {}
            }
        this.state.view_format_selection = {
                choices: [],
                selectedValue: {}
            }
        this.state.budget_selection = {
                choices: [],
                selectedValue: {}
            }

        onMounted(() => {
            this.state.hide_zero_balance = this.props.filterValues.hide_zero_balance
            this.state.view_format_selection = this.props.filterValues.view_format_selection
            this.state.comparison_date_range = this.props.filterValues.comparison_date_range
            this.state.comparison_date_from = luxon.DateTime.fromISO(this.props.filterValues.comparison_date_from)
            this.state.comparison_date_to = luxon.DateTime.fromISO(this.props.filterValues.comparison_date_to)
            this.state.defaultJournalValues = this.props.filterValues.defaultJournalValues
            this.state.defaultAccountTagValues = this.props.filterValues.defaultAccountTagValues
            // Budget selection
            let budgetChoices = (this.props.filterValues.budget_choices || []).map(b => ({value: b.value, label: b.label}));
            budgetChoices.unshift({value: 0, label: 'No Budget'});
            this.state.budget_selection = {
                choices: budgetChoices,
                selectedValue: {value: this.props.filterValues.budget_id || 0}
            }
        });

        this.handleComparisonDateRangeSelect = async (val) => {
            this.state.comparison_date_range.selectedValue.value = val
            this.props.updateValues(this)
        }

        this.handleHideZerBalanceSelect = async (val) => {
            this.state.hide_zero_balance.selectedValue.value = val
            this.props.updateValues(this)
        }

        this.handleViewFormatSelect = async (val) => {
            this.state.view_format_selection.selectedValue.value = val
            this.props.updateValues(this)
        }

        this.handleBudgetSelect = async (val) => {
            this.state.budget_selection.selectedValue.value = val
            this.props.updateValues(this)
        }
    }

    onComparisonDateFromChanged(dateFrom) {
        this.state.comparison_date_from = dateFrom
        this.props.updateValues(this)
    }

    onComparisonDateToChanged(dateTo) {
        this.state.comparison_date_to = dateTo
        this.props.updateValues(this)
    }
}
PycusFinancialReportFilters.template = 'account_dynamic_reports.PycusFinancialReportFilters';