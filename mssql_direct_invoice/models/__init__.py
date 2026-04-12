# Base model FIRST, then inheriting domain files
from . import mssql_direct_sync
from . import mssql_direct_partner
from . import mssql_direct_product
from . import mssql_direct_invoice
from . import mssql_direct_bill
from . import mssql_direct_sync_log
from . import mssql_direct_sync_queue
from . import mssql_direct_sync_queue_line
from . import product_product
from . import res_partner
