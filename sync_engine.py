import mysql.connector
import pyodbc
import pandas as pd

class dbEngine:

    def __init__(self, mssql_con, mysql_con, ms_cursor, my_cursor):
        self.mssql_con = mssql_con
        self.mysql_con = mysql_con
        self.ms_cursor = ms_cursor
        self.my_cursor = my_cursor

    def get_taable_list(self, cursor, opt):
        table_list = []
        if(opt == 1):
            cursor.execute("SELECT table_name FROM information_schema.tables")
        elif(opt == 2):
            cursor.execute("show tables;")

        for table_name in cursor:
            table_list.append(table_name[0].lower())
            # print(table_name[0])
        return table_list

    def change_MSSQL2MySQL_datatype(self, table_column_info):

        if str(table_column_info[1]) == "uniqueidentifier":
            table_column_info[1] = "char"
            table_column_info[2] = "36"
        elif str(table_column_info[1]) == "image":
            table_column_info[1] = "longblob"

        return table_column_info

    def get_taable_column_info(self, table_name, opt):
        SQL_SYNTAX = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'__MY_TABLE_NAME__'"
        SQL_SYNTAX = SQL_SYNTAX.replace("__MY_TABLE_NAME__", table_name);

        if(opt == 1) :
            self.ms_cursor.execute(SQL_SYNTAX)
        else:
            self.my_cursor.execute(SQL_SYNTAX)

        find_PriKey = False
        PriKey_ColumnName = ''
        column_list = []
        column_type_list = []

        if opt == 1 :
            for table_column_info in self.ms_cursor:
                if find_PriKey == False and str(table_column_info[1]) == "uniqueidentifier":
                    find_PriKey = True
                    PriKey_ColumnName = str(table_column_info[0])

            if(find_PriKey == True):
                column_list.append(PriKey_ColumnName)
                column_type_list.append("uniqueidentifier")
                self.ms_cursor.execute(SQL_SYNTAX)
                for table_column_info in self.ms_cursor:
                    if(str(table_column_info[0]) == PriKey_ColumnName):
                        k = 0
                    else:
                        column_list.append(table_column_info[0])
                        column_type_list.append(table_column_info[1])

                return PriKey_ColumnName, column_list, column_type_list
            else:
                return PriKey_ColumnName, column_list, column_type_list
        else:
            for table_column_info in self.my_cursor:
                column_list.append(table_column_info[0])
                column_type_list.append(table_column_info[1])
            return PriKey_ColumnName, column_list, column_type_list


    def get_taable_header_create_SQL(self, table_name):
        SQL_SYNTAX = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'__MY_TABLE_NAME__'"
        SQL_SYNTAX = SQL_SYNTAX.replace("__MY_TABLE_NAME__", table_name);
        self.ms_cursor.execute(SQL_SYNTAX)

        MYSQL_CREATE_SQL = "CREATE TABLE IF NOT EXISTS " + str(table_name) + " ( "

        for table_column_info in self.ms_cursor:
            mysql_table_col_list = self.change_MSSQL2MySQL_datatype(table_column_info)
            if mysql_table_col_list[2] == None:
                MYSQL_CREATE_SQL += str(mysql_table_col_list[0]) + " " +  str(mysql_table_col_list[1]) + " "
            else:
                MYSQL_CREATE_SQL += str(mysql_table_col_list[0]) + " " +  str(mysql_table_col_list[1]) + "(" + str(mysql_table_col_list[2]) + ")" + " "

            if str(mysql_table_col_list[3]) == "YES":
                MYSQL_CREATE_SQL += "DEFAULT NULL, "
            else:
                MYSQL_CREATE_SQL += "NOT NULL, "

        MYSQL_CREATE_SQL = MYSQL_CREATE_SQL[:len(MYSQL_CREATE_SQL) - 2]
        MYSQL_CREATE_SQL += ");"

        self.my_cursor.execute(MYSQL_CREATE_SQL)
        self.mysql_con.commit()

        return MYSQL_CREATE_SQL

    def find_symbol_order(self, data_list, symbol):
        for i, item in enumerate(data_list):
            if item == symbol:
                return i
        return -1  # Return -1 if symbol is not found in the data list


    def insert_record(self, table_name, column_list, ms_value_info):

        print("insert_record " + table_name + " record id_key = " + ms_value_info[0])

        column_cnt = len(column_list)
        insert_sql = "insert into " + table_name + " ( "
        for i in range(column_cnt):
            insert_sql += column_list[i] + ", "
        insert_sql = insert_sql[:len(insert_sql) - 2]

        valuse_data = []
        insert_sql += ") VALUES ( "
        for i in range(column_cnt):
            insert_sql += "%s" + ", "
            valuse_data.append(ms_value_info[i])
        insert_sql = insert_sql[:len(insert_sql) - 2]
        insert_sql += ")"

        self.my_cursor.execute(insert_sql, valuse_data)
        self.mysql_con.commit()
        return;

    def update_record(self, table_name, column_list, column_type_list, id_key, ms_value_info, my_value_info):
        char_type_list = ['char', 'nchar', 'varchar', 'nvarchar']

        column_cnt = len(column_list)
        update_data = ''
        for i in range(column_cnt):
            if column_type_list[i] in char_type_list:
                ms_data = ''
                my_data = ''

                ms_data = str(ms_value_info[i])
                my_data = str(my_value_info[i])

                ms_data = ms_data.strip()
                my_data = my_data.strip()

                if(ms_data != my_data):
                    update_data += str(column_list[i]) + "='" + str(ms_data) + "', "
            else:
                if(ms_value_info[i] != my_value_info[i]):
                    update_data += str(column_list[i]) + "='" + str(ms_value_info[i]) + "', "

        if(len(update_data) > 1):
            print("update_record " + table_name + " key = " + id_key)

            update_data = update_data[:len(update_data) - 2]
            update_data += " "

            update_sql = "update " + table_name + " SET " + update_data + "WHERE " + column_list[0] + "='" + id_key + "'"
            self.my_cursor.execute(update_sql)
            self.mysql_con.commit()

        return;

    def delete_record(self, table_name, column_list, del_id_key):

        print("delete_record " + table_name + " del_id_key = " + del_id_key)

        del_sql = "delete from " + table_name + " WHERE " + column_list[0] + "='" + del_id_key + "'"
        self.my_cursor.execute(del_sql)
        self.mysql_con.commit()
        return;


    def table_record_sync(self, table_name, prikey_name, column_list, column_type_list):
        mssql_record_list = []
        mysql_record_list = []

        mssql_prikey_list = []
        mysql_prikey_list = []

        # make SQL query (Select column_id, column_name from tableName order by column_id ASC)
        sql_query = f"select "
        for column_name in column_list:
            sql_query += str(column_name) + ", "

        sql_query = sql_query[:len(sql_query)-2]
        sql_query += f" from {table_name} order by {prikey_name} ASC"

        self.ms_cursor.execute(sql_query)
        result = self.ms_cursor.fetchall()
        for row_info in result:
            mssql_record_list.append(row_info)
            mssql_prikey_list.append(row_info[0].lower())
            # print(row_info)

        self.my_cursor.execute(sql_query)
        result = self.my_cursor.fetchall()
        for row_info in result:
            mysql_record_list.append(row_info)
            mysql_prikey_list.append(row_info[0].lower())
            # print(row_info)

        # INSERT, UPDATE Processing
        i = 0;
        for id_key in mssql_prikey_list:
            if str(id_key) in mysql_prikey_list:
                # check updating
                order_num = self.find_symbol_order(mysql_prikey_list, id_key)
                self.update_record(table_name, column_list, column_type_list, id_key, mssql_record_list[i], mysql_record_list[order_num])
            else:
                # Inserting
                self.insert_record(table_name, column_list, mssql_record_list[i])
            i += 1

        # DELETE Processing
        for id_key in mysql_prikey_list:
            if str(id_key) in mssql_prikey_list:
                k = 0
            else:
                self.delete_record(table_name, column_list, id_key)

    def table_sync(self, ms_table_list, my_table_list):

        for table_name in my_table_list:
            if str(table_name) in ms_table_list:
                k = 0
            else:
                del_table_sql = "drop table " + table_name + ";"
                self.my_cursor.execute(del_table_sql)
                self.mysql_con.commit()

    def table_del(self, table_name):
        del_table_sql = "drop table " + table_name + ";"
        self.my_cursor.execute(del_table_sql)
        self.mysql_con.commit()

    def check_table_column_name(self, ms_column_list, my_column_list):

        if(len(ms_column_list) != len(my_column_list)):
            return False

        column_cnt = len(ms_column_list)
        for i in range(column_cnt):
            if(ms_column_list[i] in my_column_list):
                t = 0
            else:
                return False

        return True

