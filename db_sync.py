import mysql.connector
import pyodbc
import pandas as pd
from sync_engine import dbEngine

MSSQL_SERVER = '127.0.0.1'
MSSQL_DB_NAME = 'mstestdb'
MSSQL_USER_ID = 'sa'
MSSQL_PWD = '12345'

MySQL_SERVER = '127.0.0.1'
MySQL_DB_NAME = 'testdb'
MySQL_USER_ID = 'root'
MySQL_PWD = '12345'

"""
    Main Function
"""
if __name__ == '__main__':

    try:
        # MSSQL Connection
        mssql_con = pyodbc.connect('Driver={SQL Server};'
                                  f'Server={MSSQL_SERVER};'
                                  f'Database={MSSQL_DB_NAME};'
                                  f'UID={MSSQL_USER_ID};'
                                  f'PWD={MSSQL_PWD};')
        ms_cursor = mssql_con.cursor()

        # MySQL Connection
        mysql_con = mysql.connector.connect(
                    host=MySQL_SERVER,
                    user=MySQL_USER_ID,
                    passwd=MySQL_PWD,
                    database=MySQL_DB_NAME)
        my_cursor = mysql_con.cursor()
    except:
        print("Error ... connection database")

    else:
        # Starting DB Sync
        try:
            # init
            db_sync_eng = dbEngine(mssql_con, mysql_con, ms_cursor, my_cursor)

            ms_table_list = db_sync_eng.get_taable_list(ms_cursor, 1)
            my_table_list = db_sync_eng.get_taable_list(my_cursor, 2)
            db_sync_eng.table_sync(ms_table_list, my_table_list)

            for table_name in ms_table_list:
                ms_pri_column, ms_column_list, ms_column_type_list = db_sync_eng.get_taable_column_info(table_name, 1)
                _, my_column_list, _ = db_sync_eng.get_taable_column_info(table_name, 2)

                if len(ms_column_list) > 0 and  len(my_column_list) > 0 and db_sync_eng.check_table_column_name(ms_column_list, my_column_list) == False :
                    db_sync_eng.table_del(table_name)
                    my_table_list.remove(table_name)

                if len(ms_pri_column) < 1 :
                    print(f"There is no default primary key in {table_name}")
                else:
                    # Create Table
                    if str(table_name) in my_table_list:
                        k = 0
                    else:
                        print(f"Creating table {table_name} into MYSQL DB ... ")
                        create_sql = db_sync_eng.get_taable_header_create_SQL(table_name)
                        print(f"Creating table {table_name}  ... OK")

                    print(f"Starting sync for {table_name} ... ")
                    db_sync_eng.table_record_sync(table_name, ms_pri_column, ms_column_list, ms_column_type_list)
                    print(f"Starting {table_name} sync ... OK")

            mysql_con.close()
            mssql_con.close()
        except:
            print("Error ... MSSQL -> MYSQL")
            mysql_con.close()
            mssql_con.close()

        else:
            print("OK ... MSSQL -> MYSQL")

