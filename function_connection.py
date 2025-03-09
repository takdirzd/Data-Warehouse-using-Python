import pandas as pd
import json

import psycopg2

def func_db_connect(prm_db_ssh = None,
                    prm_db_type=None,
                    prm_db_host=None,
                    prm_db_port=None,
                    prm_db_name=None,
                    prm_user=None,
                    prm_pass=None,
                    prm_schema=None,
                    prm_prv_key = None,
                    prm_prv_pass = None,
                    prm_ssh_ip = None,
                    prm_ssh_port = None,
                    prm_ssh_user = None
                   ):
   
    data = [None, None, None]
   
    try:
        conn = psycopg2.connect(user=prm_user,
                                password=prm_pass,
                                database=prm_db_name,
                                port=prm_db_port,
                                host=prm_db_host,
                                options=f"-c search_path=dbo,{prm_schema}")
   
        data[0] = conn
        data[1] = True
        data[2] = "Connection Successfully"        
        return data
           
    except (Exception) as e:
        data[0] = None
        data[1] = False
        data[2] = e        
        return data
