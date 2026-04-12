{
    "name": "SQL Server Direct Invoice Sync",
    "version": "18.0.1.0.0",
    "category": "Accounting",
    "summary": "Sync products, partners, invoices and bills directly from SQL Server (no SO/PO)",
    "depends": ["product", "account", "mail"],
    "data": [
        "security/ir.model.access.csv",
        "data/ir_sequence_data.xml",
        "views/mssql_direct_sync_views.xml",
        "views/mssql_direct_sync_queue_views.xml",
        "views/mssql_direct_operations_views.xml",
    ],
    "installable": True,
    "application": False,
    "license": "LGPL-3",
}
