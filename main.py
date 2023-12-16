import pandas as pd
import json
import mysql.connector
import datetime

current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

########################## ------- STAGING  ------- ##########################
def connect_to_staging():
        # Information of the database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="de_test"
        ) 
        return connection

def connect_to_dwh():
        # Information of the database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="dwh"
        ) 
        return connection
#--------------------------- Import excel file to MySQL
    
def source_to_staging():
    # Read file using pandas
    data_excel = pd.read_excel('E:\TES EDSEN\TEST TECHNICAL\Superstore.xlsx')
    data_excel['Order_Date'] = data_excel['Order_Date'].dt.strftime('%Y-%m-%d')
    data_excel['Ship_Date'] = data_excel['Ship_Date'].dt.strftime('%Y-%m-%d')
    
    # Hubungkan ke database MySQL
    conn_staging = connect_to_staging()
    cursor = conn_staging.cursor()

    # Menyimpan data dari Excel ke MySQL
    for row in data_excel.itertuples():
        sql_excel = """INSERT INTO Superstore (Row_ID, Order_ID, Order_Date, Ship_Date, Ship_Mode, 
                    Customer_ID, Customer_Name, Segment,
                    Country, City, State, Postal_Code, Region,
                    Product_ID, Category, Sub_Category, Product_Name,
                    Sales, Quantity, Discount, Profit) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"""
        cursor.execute(sql_excel, (row.Row_ID, row.Order_ID, row.Order_Date, row.Ship_Date, row.Ship_Mode,
                    row.Customer_ID, row.Customer_Name, row.Segment,
                    row.Country, row.City,  row.State, row.Postal_Code, row.Region,
                    row.Product_ID, row.Category, row.Sub_Category, row.Product_Name,
                    row.Sales, row.Quantity, row.Discount, row.Profit)) 
    print("Successfully import Excel file to STAGING!")
    conn_staging.commit()

#--------------------------- Import JSON file to MySQL

    with open('E:\TES EDSEN\TEST TECHNICAL\People.json') as f:
        data_json = json.load(f)
    
    conn_staging = connect_to_staging()
    cursor = conn_staging.cursor()
  
        # Store JSON data to MySQL
    for item_json in data_json:
        sql_json = "INSERT INTO People (Person, Region) VALUES (%s, %s)"
        cursor.execute(sql_json, (item_json['Person'], item_json['Region']))
    print("Successfully import JSON to STAGING")
    conn_staging.commit()

#--------------------------- Import XML file to MySQL


    file_path_xml = 'E:\TES EDSEN\TEST TECHNICAL\Returns.xml'

    # Read XML file
    with open(file_path_xml, 'r') as file_xml:
        xml_text = file_xml.read()

    # Find the <row> pattern to capture the information in it
    start_index = xml_text.find('<row>')
    end_index = xml_text.rfind('</row>')

    # Get the substring that contains the data to XML variable
    xml_data = xml_text[start_index:end_index + len('</row>')]

    # Make an empty variable to store the findings data in XML file 
    order_ids_xml = []
    returned_values_xml = []

    while '<data ss:type="String">' in xml_data:
        order_start = xml_data.find('<data ss:type="String">') + len('<data ss:type="String">')
        order_end = xml_data.find('</data>')
        order_ids_xml.append(xml_data[order_start:order_end])
        
        xml_data = xml_data[order_end + len('</data>'):]
        
        returned_start = xml_data.find('<data ss:type="String">') + len('<data ss:type="String">')
        returned_end = xml_data.find('</data>')
        returned_values_xml.append(xml_data[returned_start:returned_end])
        
        xml_data = xml_data[returned_end + len('</data>'):]

    # Make a dataframe to store the findings data to a dataframe, to make esier to read
    df_xml = pd.DataFrame({'Order_ID': order_ids_xml, 'Returned': returned_values_xml})
    df_xml = df_xml.drop(index=0)
    
    conn_staging = connect_to_staging()
    cursor = conn_staging.cursor()
        
    # Menyimpan data dari XML ke MySQL
    for index, row in df_xml.iterrows():
        sql_xml = "INSERT INTO returns (Order_ID, Returned) VALUES (%s, %s)"
        cursor.execute(sql_xml, (row['Order_ID'], row['Returned']))
    print("Successfully import XML to STAGING")

    print("Successfully store all the source data to STAGING!")
    conn_staging.commit()
    


########################## ------- Data Warehouse  ------- ########################## Data WarehouseData WarehouseData WarehouseData Warehouse
def staging_to_dwh(): 
    
    # CONNECT TO STAGING 
    conn_staging = connect_to_staging()
    cursor_staging = conn_staging.cursor()

    # CONNECT TO DWH
    conn_dwh = connect_to_dwh()
    cursor_dwh = conn_dwh.cursor()


#--------------------------------------------------------------REGION MANAGER REGION MANAGER REGION MANAGER REGION MANAGER REGION 
    # Get RAGION MANAGER DATA
    proc_region_mgr = "SYN_REGION_MGR"
    rgm = "SELECT Region, Person FROM people"
    cursor_staging.execute(rgm)
    data_rgm = cursor_staging.fetchall()
#--------------------------------------------------------------
    # Import REGION MANAGER data to data warehouse from Staging

    for row in data_rgm:
    # Query untuk memasukkan data ke database tujuan
        insert_query_rgm = "INSERT INTO region_mgr (REGION, PERSON) VALUES (%s, %s)"
        
        try:
            cursor_dwh.execute(insert_query_rgm, row)
            
        except mysql.connector.Error as err:
            # Store the error to the Error Log table
            error_message_rgm = str(f"REGION={['REGION']} : unique constraint (region_pkey) violated")
            insert_error_query_rgm = "INSERT IGNORE INTO err_log (PROC_NAME,LOG_DATE, ERR_MSG ) VALUES (%s, %s, %s)"
            cursor_dwh.execute(insert_error_query_rgm, ( proc_region_mgr, current_date, error_message_rgm))

    print("REGION MANAGER successfully imported to DATA WAREHOUSE!")
    conn_dwh.commit()

#--------------------------------------------------------------LOCATIONLOCATIONLOCATIONLOCATIONLOCATIONLOCATIONLOCATION
    # Get LOCATION DATA
    proc_loc = "SYN_LOCATION"
    loc = "SELECT Postal_Code, Country, Region, State, City FROM superstore"
    cursor_staging.execute(loc)
    data_loc = cursor_staging.fetchall()
#--------------------------------------------------------------
    # Import LOCATION data to data warehouse from Staging

    for row in data_loc:
    # Query untuk memasukkan data ke database tujuan
        insert_query_loc = "INSERT INTO location (ZIPCODE, COUNTRY, REGION, STATE, CITY) VALUES (%s, %s, %s, %s, %s)"
        
        try:
            cursor_dwh.execute(insert_query_loc, row)
            
        except mysql.connector.Error as err:
            # Store the error to the Error Log table
            error_message_loc = str(f"ZIPCODE={['ZIPCODE']} : unique constraint (zipcode_pkey) violated")
            insert_error_query_loc = "INSERT IGNORE INTO err_log (PROC_NAME,LOG_DATE, ERR_MSG ) VALUES (%s, %s, %s)"
            cursor_dwh.execute(insert_error_query_loc, (proc_loc, current_date, error_message_loc))

    print("LOCATION successfully imported to DATA WAREHOUSE!")
    conn_dwh.commit()
    
#--------------------------------------------------------------CUSTOMERCUSTOMERCUSTOMERCUSTOMERCUSTOMERCUSTOMERCUSTOMER
    # Get CUSTOMER DATA
    proc_loc = "SYN_CUSTOMER"
    cust = "SELECT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT FROM superstore"
    cursor_staging.execute(cust)
    data_cust = cursor_staging.fetchall()
#--------------------------------------------------------------
    # Import CUSTOMER data to data warehouse from Staging

    for row in data_cust:
    # Query untuk memasukkan data ke database tujuan
        insert_query_cust = "INSERT INTO customer (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT) VALUES (%s, %s, %s)"
        
        try:
            cursor_dwh.execute(insert_query_cust, row)
            
        except mysql.connector.Error as err:
            # Store the error to the Error Log table
            error_message_cust = str(f"CUSTOMER_ID={['CUSTOMER_ID']} : unique constraint (customer_pkey) violated")
            insert_error_query_cust = "INSERT IGNORE INTO err_log (PROC_NAME,LOG_DATE, ERR_MSG ) VALUES (%s, %s, %s)"
            cursor_dwh.execute(insert_error_query_cust, (proc_loc, current_date, error_message_cust))

    print("CUSTOMER successfully imported to DATA WAREHOUSE!")
    conn_dwh.commit()

#--------------------------------------------------------------PRODUCTPRODUCTPRODUCTPRODUCTPRODUCTPRODUCTPRODUCTPRODUCT
    # Get PRODUCT DATA
    proc_prod= "SYN_PRODUCT"
    prod = "SELECT Product_ID, Category, Sub_Category, Product_Name FROM superstore"
    cursor_staging.execute(prod)
    data_prod = cursor_staging.fetchall()
#--------------------------------------------------------------
    # Import PRODUCT data to data warehouse from Staging

    for row in data_prod:
    # Query untuk memasukkan data ke database tujuan
        insert_query_prod = "INSERT INTO product (PRODUCT_ID, CATEGORY, SUB_CATEGORY, PRODUCT_NAME) VALUES (%s, %s, %s, %s)"
        
        try:
            cursor_dwh.execute(insert_query_prod, row)
            
        except mysql.connector.Error as err:
            # Store the error to the Error Log table
            error_message_prod = str(f"PRODUCT_ID={['Product_ID']} : unique constraint (product_pkey) violated")
            insert_error_query_prod = "INSERT IGNORE INTO err_log (PROC_NAME,LOG_DATE, ERR_MSG ) VALUES (%s, %s, %s)"
            cursor_dwh.execute(insert_error_query_prod, (proc_prod, current_date, error_message_prod))

    print("PRODUCT successfully imported to DATA WAREHOUSE!")
    conn_dwh.commit()


#--------------------------------------------------------------ORDERSORDERSORDERSORDERSORDERSORDERSORDERSORDERSORDERSORDERS
    # Get ORDERS DATA
    proc_orders= "SYN_ORDERS"
    orders = """SELECT de_test.superstore.Order_ID, de_test.superstore.Order_Date, 
                de_test.superstore.Product_ID, de_test.superstore.Customer_ID, 
                de_test.superstore.Ship_Date, de_test.superstore.Postal_Code, 
                de_test.superstore.Sales, de_test.superstore.Quantity, 
                de_test.superstore.Discount, de_test.superstore.Profit, IFNULL(de_test.returns.Returned, "No") as RETURNED 
                FROM de_test.superstore 
                LEFT JOIN de_test.returns on de_test.superstore.Order_ID = de_test.returns.Order_ID
                """
    cursor_staging.execute(orders)
    data_orders = cursor_staging.fetchall()
    df_orders = pd.DataFrame(data_orders)
#--------------------------------------------------------------
    # Import ORDERS data to data warehouse from Staging

    for index, row in df_orders.iterrows():
        # Query untuk memasukkan data ke database tujuan
        insert_query_orders = """INSERT INTO orders (ORDER_DATE, ORDER_ID, PRODUCT_ID, CUSTOMER_ID, 
                                    SHIP_DATE, ZIPCODE, SALES, QUANTITY, DISCOUNT, PROFIT, RETURNED) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        try:
            cursor_dwh.execute(insert_query_orders, (
                row[1], row[0], row[2], row[3], 
                row[4], row[5], row[6], row[7], 
                row[8], row[9], row[10]
            ))
        except mysql.connector.Error as err:
            # Store the error to the Error Log table
            error_message_orders = str(f"ORDER_ID={row[0]} ORDER_DATE={row[1]} PRODUCT_ID={row[2]} SHIP_DATE={row[4]} : unique constraint (orders.XPKORDERS) violated")
            insert_error_query_orders = "INSERT IGNORE INTO err_log (PROC_NAME,LOG_DATE, ERR_MSG ) VALUES (%s, %s, %s)"
            cursor_dwh.execute(insert_error_query_orders, (proc_orders, current_date, error_message_orders))

    print("ORDERS successfully imported to DATA WAREHOUSE!")

    print("Successfully store all the STAGING data to DATA WAREHOUSE!")
    conn_dwh.commit()

# ------------------------------------------------------------MAIN PROCESS-----------------------------------------------------------
#--------------------------- Staging stage
connect_to_staging()
print("======================= SOURCE TO STAGING ==============================")
print(f"Successfully connected to database : '{connect_to_staging().database}' as STAGING")
source_to_staging()

#--------------------------- ETL to Data Warehouse stage
connect_to_dwh()
print("\n\n======================= STAGING TO DATA WAREHOUSE =======================")
print(f"Successfully connected to database : '{connect_to_dwh().database}' as DATA WAREHOUSE")
staging_to_dwh()

print("Process completed.")




