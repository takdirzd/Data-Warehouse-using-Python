import psycopg2
import datetime

current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


##################################
##          REGION MGR          ##
##################################
def region_mgr(cursor_stg = None,
               cursor_dwh = None,
                DB_SCHEMA_STG = None,
                DB_SCHEMA_DWH = None
                ):
    
    proc_region_mgr = "SYN_REGION_MGR"
    sql_select_region = f"""SELECT region, person from {DB_SCHEMA_STG}.people"""
    cursor_stg.execute(sql_select_region)
    data_rgm = cursor_stg.fetchall()

    for row in data_rgm:
        sql_insert_rgm = f"""
                INSERT INTO {DB_SCHEMA_DWH}.region_mgr VALUES (%s, %s)
                ON CONFLICT (region) DO NOTHING
                """
        cursor_dwh.execute(sql_insert_rgm, row)

         
    print("Imported to REGION_MGR ✅")


##################################
##          LOCATION            ##
##################################
def location(cursor_stg = None,
               cursor_dwh = None,
                DB_SCHEMA_STG = None,
                DB_SCHEMA_DWH = None
                ):
    
    proc_location = "SYN_LOCATION"
    sql_select_location = f"""SELECT postal_code, country, region, state, city from {DB_SCHEMA_STG}.superstore"""
    cursor_stg.execute(sql_select_location)
    data_location = cursor_stg.fetchall()

    for row in data_location:
        sql_insert_location = f"""
            INSERT INTO {DB_SCHEMA_DWH}.location
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (zipcode) DO NOTHING
        """
        cursor_dwh.execute(sql_insert_location, row)


    print("Imported to LOCATION ✅")



##################################
##           PRODUCT            ##
##################################
def product(cursor_stg = None,
               cursor_dwh = None,
                DB_SCHEMA_STG = None,
                DB_SCHEMA_DWH = None
                ):
    

    sql_select_product= f"""SELECT product_id, category, sub_category, product_name from {DB_SCHEMA_STG}.superstore"""
    cursor_stg.execute(sql_select_product)
    data_product = cursor_stg.fetchall()

    for row in data_product:
        sql_insert_product = f"""
            INSERT INTO {DB_SCHEMA_DWH}.product 
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (product_id) DO NOTHING
        """
        cursor_dwh.execute(sql_insert_product, row)


    print("Imported to PRODUCT ✅")


##################################
##           CUSTOMER           ##
##################################
def customer(cursor_stg = None,
               cursor_dwh = None,
                DB_SCHEMA_STG = None,
                DB_SCHEMA_DWH = None
                ):
    

    sql_select_customer= f"""SELECT customer_id, customer_name, segment from {DB_SCHEMA_STG}.superstore"""
    cursor_stg.execute(sql_select_customer)
    data_customer= cursor_stg.fetchall()

    for row in data_customer:
        sql_insert_customer = f"""
            INSERT INTO {DB_SCHEMA_DWH}.customer 
            VALUES (%s, %s, %s) 
            ON CONFLICT (customer_id) DO NOTHING
        """
        cursor_dwh.execute(sql_insert_customer, row)


    print("Imported to CUSTOMER ✅")


##################################
##           ORDERS             ##
##################################
def orders(cursor_stg = None,
               cursor_dwh = None,
                DB_SCHEMA_STG = None,
                DB_SCHEMA_DWH = None
                ):
    
    proc_orders =" SYN_ORDERS"

    sql_select_orders = f"""SELECT a.Order_ID, a.Order_Date, 
                a.Product_ID, a.Customer_ID, 
                a.Ship_Date, a.Postal_Code, 
                a.Sales, a.Quantity, 
                a.Discount, a.Profit, COALESCE(b.Returned, 'No') as RETURNED 
                FROM {DB_SCHEMA_STG}.superstore a
                LEFT JOIN {DB_SCHEMA_STG}.returns b on a.Order_ID = b.Order_ID
                """
    cursor_stg.execute(sql_select_orders)
    data_orders = cursor_stg.fetchall()

    for row in data_orders:
        try:
            sql_insert_orders = f"""INSERT INTO {DB_SCHEMA_DWH}.orders (ORDER_DATE, ORDER_ID, PRODUCT_ID, CUSTOMER_ID, 
                                    SHIP_DATE, ZIPCODE, SALES, QUANTITY, DISCOUNT, PROFIT, RETURNED) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (order_date, order_id, product_id) DO NOTHING"""


            cursor_dwh.execute(sql_insert_orders, (
                row[1], row[0], row[2], row[3], 
                row[4], row[5], row[6], row[7], 
                row[8], row[9], row[10]
            ))

            # Cek duplikasi
            if cursor_dwh.rowcount == 0:
                error_message_orders = str(f"ORDER_ID={row[0]} ORDER_DATE={row[1]} PRODUCT_ID={row[2]} SHIP_DATE={row[4]} : unique constraint (orders.XPKORDERS) violated")
                raise psycopg2.IntegrityError(error_message_orders)


        except psycopg2.Error as err:
            # Masukkan error (baik duplikat atau error lain) ke err_log
            sql_insert_error_location = f"""
                INSERT INTO {DB_SCHEMA_DWH}.err_log (proc_name, log_date, err_msg) 
                VALUES (%s, %s, %s)
            """
            cursor_dwh.execute(sql_insert_error_location, (proc_orders, current_date, str(err)))




    print("Imported to ORDERS ✅")