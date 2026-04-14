from odoo import models, fields
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)


class MssqlDirectProduct(models.Model):
    _inherit = 'mssql.direct.sync'

    # ── Product Tracking Fields ─────────────────────────────────────────
    last_product_sync_date = fields.Datetime(
        string='Last Product Sync Date',
        help='Watermark for new product detection')

    # ── Product Sync ────────────────────────────────────────────────────

    def sync_products(self):
        """Fetch products from SQL Server and create/update in Odoo"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            sql_products = self._query_products_with_prices(cursor)
            conn.close()

            if not sql_products:
                _logger.info('No products found in SQL Server')
                return self._success_notification('Product Sync Complete', 'No products found')

            # Deduplicate by ItemID
            seen_item_ids = set()
            unique_products = []
            for item in sql_products:
                item_id = item['ItemID']
                if item_id and item_id not in seen_item_ids:
                    seen_item_ids.add(item_id)
                    unique_products.append(item)

            sql_products = unique_products
            product_obj = self.env['product.product']

            item_ids = [str(item['ItemID']) for item in sql_products if item['ItemID']]
            existing_products = {
                p.x_sql_item_id: p for p in product_obj.search([('x_sql_item_id', 'in', item_ids)])
            }

            to_create = []
            skipped = 0

            for item in sql_products:
                item_id = item['ItemID']
                if not item_id:
                    continue
                item_id = str(item_id)

                name = item['ItemName'] or item['EnglishName'] or f"Item {item_id}"
                vals = {
                    'name': name,
                    'x_sql_item_id': item_id,
                    'x_english_name': item['EnglishName'],
                    'type': 'consu',
                }

                if item.get('PurchasePrice') is not None:
                    vals['standard_price'] = float(item['PurchasePrice'])
                if item.get('SellPrice') is not None:
                    vals['list_price'] = float(item['SellPrice'])
                if item.get('BarCode'):
                    vals['barcode'] = item['BarCode']
                    vals['default_code'] = item['BarCode']

                if item_id in existing_products:
                    skipped += 1
                    continue
                else:
                    to_create.append(vals)

            created = 0
            if to_create:
                _logger.info(f"Creating {len(to_create)} new products in batches...")
                batch_size = 2000
                fast_create = product_obj.with_context(
                    tracking_disable=True,
                    mail_create_nolog=True,
                    mail_create_nosubscribe=True,
                    mail_notrack=True,
                )
                for i in range(0, len(to_create), batch_size):
                    batch = to_create[i:i + batch_size]
                    fast_create.create(batch)
                    created += len(batch)
                    _logger.info(f"Product creation progress: {created}/{len(to_create)}")
                    self.env.cr.commit()
                    self.env.clear()

            if created > 0:
                self.write({'products_fetched': True})

            if created == 0:
                return self._success_notification('Product Sync Complete', f'No new products found (checked: {len(sql_products)} products, {skipped} already exist)')
            else:
                return self._success_notification('Product Sync Complete', f'Created: {created} new products ({skipped} already existed)')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Product sync failed: {str(e)}')

    def action_update_products(self):
        """Update product prices, barcode, and default_code from MSSQL.

        Fetches prices + barcode from tblItemsUnits (StockUnit=1) and updates
        standard_price, list_price, barcode, default_code.
        """
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            price_data = self._query_current_prices(cursor)
            conn.close()

            if not price_data:
                raise UserError('No product data found in SQL Server')

            product_obj = self.env['product.product']
            item_ids = [str(item['ItemID']) for item in price_data if item['ItemID']]
            existing_products = {
                p.x_sql_item_id: p for p in product_obj.search([('x_sql_item_id', 'in', item_ids)])
            }

            product_updates = {}
            for item in price_data:
                item_id = item['ItemID']
                if not item_id:
                    continue
                item_id = str(item_id)
                if item_id not in existing_products:
                    continue

                product = existing_products[item_id]
                if product.id not in product_updates:
                    product_updates[product.id] = {}

                if item.get('PurchasePrice') is not None:
                    product_updates[product.id]['standard_price'] = float(item['PurchasePrice'])
                if item.get('SellPrice') is not None:
                    product_updates[product.id]['list_price'] = float(item['SellPrice'])
                if item.get('BarCode'):
                    product_updates[product.id]['barcode'] = item['BarCode']
                    product_updates[product.id]['default_code'] = item['BarCode']

            updated = 0
            if product_updates:
                batch_size = 1000
                product_ids = list(product_updates.keys())
                for i in range(0, len(product_ids), batch_size):
                    batch_ids = product_ids[i:i + batch_size]
                    batch_products = product_obj.browse(batch_ids)
                    for product in batch_products:
                        if product.id in product_updates:
                            product.write(product_updates[product.id])
                    updated += len(batch_ids)
                    if i % (batch_size * 10) == 0:
                        self.env.clear()

            return self._success_notification(
                'Update Products Complete',
                f'Updated prices/barcode for {updated} products.')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Update products failed: {str(e)}')

    # ── SQL Queries ─────────────────────────────────────────────────────

    def _query_products_with_prices(self, cursor):
        """Fetch products with current prices from tblItemsUnits (StockUnit=1)."""
        cursor.execute("""
            SELECT
                i.ItemID,
                i.ItemName,
                i.EnglishName,
                i.Tax,
                i.SupplierID,
                i.CatID,
                i.DepartmentID,
                i.VatCat,
                iu.PurchasePrice,
                iu.SellPrice,
                iu.UnitName,
                iu.BarCode
            FROM [dbo].[tblItems] i
            LEFT JOIN [dbo].[tblItemsUnits] iu
                ON i.ItemID = iu.ItemID AND iu.StockUnit = 1
        """)
        return cursor.fetchall()

    def _query_current_prices(self, cursor):
        """Fetch current prices, barcode for all products from tblItemsUnits (StockUnit=1)."""
        cursor.execute("""
            SELECT
                iu.ItemID,
                iu.PurchasePrice,
                iu.SellPrice,
                iu.UnitName,
                iu.BarCode
            FROM [dbo].[tblItemsUnits] iu
            WHERE iu.StockUnit = 1
        """)
        return cursor.fetchall()
