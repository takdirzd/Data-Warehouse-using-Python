import json
import pandas as pd

##################################
##           CSV FILE           ##
##           CSV FILE           ##
##           CSV FILE           ##
##################################

def import_excel_to_stg(prm_CURSOR = None,
                        prm_DB_SCHEMA = None,
                        prm_PATH_TO_EXCEL = None):
    # Baca file Exce
    data_excel = pd.read_excel(f'{prm_PATH_TO_EXCEL}')
    data_excel['Order_Date'] = data_excel['Order_Date'].dt.strftime('%Y-%m-%d')
    data_excel['Ship_Date'] = data_excel['Ship_Date'].dt.strftime('%Y-%m-%d')



    # Menyimpan data dari Excel ke MySQL
    for row in data_excel.itertuples():
        sql = f"""INSERT INTO {prm_DB_SCHEMA}.Superstore (
                    Row_ID,
                    Order_ID,
                    Order_Date,
                    Ship_Date,
                    Ship_Mode,
                    Customer_ID,
                    Customer_Name,
                    Segment,
                    Country,
                    City,
                    State,
                    Postal_Code,
                    Region,
                    Product_ID,
                    Category,
                    Sub_Category,
                    Product_Name,
                    Sales,
                    Quantity,
                    Discount,
                    Profit) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"""
        prm_CURSOR.execute(sql, (
                    row.Row_ID,
                    row.Order_ID,
                    row.Order_Date,
                    row.Ship_Date,
                    row.Ship_Mode,
                    row.Customer_ID,
                    row.Customer_Name,
                    row.Segment,
                    row.Country,
                    row.City,
                    row.State,
                    row.Postal_Code,
                    row.Region,
                    row.Product_ID,
                    row.Category,
                    row.Sub_Category,
                    row.Product_Name,
                    row.Sales,
                    row.Quantity,
                    row.Discount,
                    row.Profit))  # Sesuaikan dengan nama kolom
    

    print("XLSX file imported to SUPERSTORE ✅")


##################################
##          JSON FILE           ##
##          JSON FILE           ##
##          JSON FILE           ##
##################################

def import_json_to_stg(prm_CURSOR = None,
                        prm_DB_SCHEMA = None,
                        prm_PATH_TO_JSON = None):
    # Baca file JSON
    with open(f'{prm_PATH_TO_JSON}') as f:
        data_json = json.load(f)

    

    # Menyimpan data dari JSON ke MySQL
    for item in data_json:
        sql = f"INSERT INTO {prm_DB_SCHEMA}.people (person, region) VALUES (%s, %s)"
        prm_CURSOR.execute(sql, (item['Person'], item['Region']))  # Sesuaikan dengan nama kolom
    

    print("JSON file imported to PEOPLE ✅")


##################################
##           XML FILE           ##
##           XML FILE           ##
##           XML FILE           ##
##################################


def import_xml_to_stg(prm_CURSOR = None,
                        prm_DB_SCHEMA = None,
                        prm_PATH_TO_XML = None):

    # Parsing file XML
    file_path = f'{prm_PATH_TO_XML}'

    # Baca file XML sebagai string teks
    with open(file_path, 'r') as file:
        xml_text = file.read()

    # Mencari pola string untuk mengekstrak informasi
    start_index = xml_text.find('<row>')
    end_index = xml_text.rfind('</row>')

    # Mendapatkan substring yang berisi data XML
    xml_data = xml_text[start_index:end_index + len('</row>')]

    # Ekstrak nilai Order_ID dan Returned dari substring XML
    order_ids = []
    returned_values = []


    while '<data ss:type="String">' in xml_data:
        order_start = xml_data.find('<data ss:type="String">') + len('<data ss:type="String">')
        order_end = xml_data.find('</data>')
        order_ids.append(xml_data[order_start:order_end])
        
        xml_data = xml_data[order_end + len('</data>'):]
        
        returned_start = xml_data.find('<data ss:type="String">') + len('<data ss:type="String">')
        returned_end = xml_data.find('</data>')
        returned_values.append(xml_data[returned_start:returned_end])
        
        xml_data = xml_data[returned_end + len('</data>'):]
        
    # Membuat DataFrame dari data yang diekstrak
    df = pd.DataFrame({'Order_ID': order_ids, 'Returned': returned_values})
    # Hapus baris pertama (karena itu header duplikat)
    df_new = df.iloc[1:].reset_index(drop=True)
    
    

    # IMPORT DATA KE STAGING
    for index, row in df_new.iterrows():
        sql = f"INSERT INTO {prm_DB_SCHEMA}.returns (Order_ID, Returned) VALUES (%s, %s)"
        prm_CURSOR.execute(sql, (row['Order_ID'], row['Returned']))

    print("XML file imported to RETURNS ✅")
