import streamlit as st
from sqlalchemy import create_engine, inspect
from sqlalchemy.types import *

import cx_Oracle
import pandas as pd
from urllib.parse import quote_plus
import ast
import re
from pyhive import hive

type_mappings_path = '/Users/nachanon/projects/hive_datatype/type_mappings.txt'

def read_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        return ast.literal_eval(content)
#oracle+cx_oracle://SYS:1923@localhost:1521/FREE
def get_connection_string(db_type, host, port, username, password, database):
    if db_type == "oracle":
        return f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "mssql":
        return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    elif db_type == "postgres":
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "mysql":
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

def get_schemas(engine):
    inspector = inspect(engine)
    return inspector.get_schema_names()

def get_tables(engine, schema):
    inspector = inspect(engine)
    return inspector.get_table_names(schema=schema)

def get_columns(engine, schema, table):
    inspector = inspect(engine)
    return inspector.get_columns(table, schema=schema)



def get_hive_type(db_type , column_type):
    type_mappings = read_file(type_mappings_path)
    column_type = str(column_type.__repr__()).lower()
    match = re.match(r"\w+\(\s*[^)]+,\s*[^)]*\)",column_type)
    if match:
        dec_info = match.group().split('(')[1].split(',')
        precision = int(dec_info[0].split('=')[1])
        try: 
            scale = int(dec_info[1].split('=')[1].split(')')[0])
        except:
            print('scale is (x,)')
            scale = 0
        if precision > 38:
            precision = 38
        if (precision >0 and precision <= 38) and (scale >=0 and scale <= precision):
            return f"DECIMAL({precision},{scale})"
        
    if 'char' in column_type:
        char_info = column_type.split('(')
        try:
            str_len  = int(char_info[1].split(')')[0])
        except:
            str_len  = int(char_info[1].split(')')[0].split('=')[1])
        type_name = char_info[0]
        return(f"{type_name.upper()}({str_len})")  
      
    column_type = column_type.replace('()','')

    if db_type in type_mappings and column_type in type_mappings[db_type]:
        return type_mappings[db_type][column_type]
    
    
    return 'STRING'  

def convert_schema_to_hive(engine, inspector, db_schema, db_type):
    
    schema = {}
    
    for table_name in inspector.get_table_names(db_schema):
        columns = []
        for column in inspector.get_columns(table_name = table_name, schema = db_schema):
            hive_type = get_hive_type(db_type,column['type'])
            columns.append({
                'name':column['name'],
                'hive_type':hive_type,
                'comment':column['comment']}
              )
        schema[table_name] = columns

    return schema

def generate_sql_ddl(hive_schema, schema_name, table_name, table_comment, location = '/staging' , stored_as = 'PARQUET'):
    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS staging.{schema_name}_{table_name.lower()} (\n"
    cols= []
    for col in hive_schema[table_name]:
        comment = f"COMMENT '{col['comment']}'" if col['comment'] else ''
        cols.append(f"{col['name']} {col['hive_type']} {comment}")
        
    ddl += "    "
    ddl += ",\n    ".join(cols)
    ddl += "\n)\n"

    ddl += f"COMMENT '{table_comment['text']}'\n" if table_comment['text'] else ''
    ddl += f"STORED AS {stored_as}\n"
    ddl += f"LOCATION '{location}'"
    
    
    return ddl

def get_hive_conn(hive_host,hive_port,hive_username,hive_password \
                  ,hive_database, hive_auth):
    conn = hive.Connection(
        host=hive_host, 
        port=hive_port, 
        username=hive_username,
        password=hive_password, 
        database=hive_database,
        auth=hive_auth
    )
    return conn

def main():
    st.title("Hadoop Table Migration Tool")
    
    # Use session state to persist values across reruns
    if 'connection' not in st.session_state:
        st.session_state.connection = {
            'db_type': '',
            'host': '',
            'port': '',
            'username': '',
            'password': '',
            'database': '',
            'engine': None,
            'schemas': [],
            'selected_schema': '',
            'tables': [],
            'selected_table': '',
            'hive_host': '',
            'hive_port': '',
            'hive_username': '',
            'hive_password': '',
            'hive_database': '',
            'hive_auth':'',
            'hive_ddl': '',
            'hive_conn':''
        }

    db_type = st.selectbox("Select Database Type", ["oracle", "sqlserver", "postgres", "mysql"], 
                           key='db_type')

    col1, col2 = st.columns(2)
    with col1:
        host = st.text_input("Host", key='host')
        username = st.text_input("Username", key='username')
        database = st.text_input("Database", key='database')
    with col2:
        port = st.text_input("Port", key='port')
        password = st.text_input("Password", type="password", key='password')

    if st.button("Connect"):
        parsed_passwd  = quote_plus(password)
        connection_string = get_connection_string(db_type, host, port, username, parsed_passwd, database)
        
        try:
            engine = create_engine(connection_string)
            st.session_state.connection['engine'] = engine
            st.success("Connected successfully!")
            
            # Fetch schemas
            schemas = get_schemas(engine)
            st.session_state.connection['schemas'] = schemas
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            st.error(f"Connection string (password hidden): {connection_string.replace(password, '*****')}")

    # Display schema dropdown if connection is established
    if st.session_state.connection['engine'] is not None:
        selected_schema = st.selectbox("Select Schema", st.session_state.connection['schemas'], 
                                       key='selected_schema')
        
        if selected_schema:
            inspector = inspect(st.session_state.connection['engine'])
            hive_schema = convert_schema_to_hive(st.session_state.connection['engine'], inspector, selected_schema,db_type)

            # Fetch tables for the selected schema
            tables = get_tables(st.session_state.connection['engine'], selected_schema)
            st.session_state.connection['tables'] = tables
            
            selected_table = st.selectbox("Select Table", tables, key='selected_table')
            
            if selected_table:
                # columns = get_columns(st.session_state.connection['engine'], selected_schema, selected_table)
                
                hive_table_schema = hive_schema[selected_table]
                table_comment = inspector.get_table_comment(schema = selected_schema,table_name = selected_table)

                print('########################')
                print(selected_table)
                st.write("Table Structure:")
                df = pd.DataFrame(hive_table_schema)
                st.dataframe(df)
                
                # conn = hive.Connection(
                #     host='192.168.170.224', 
                #     port=10000, 
                #     username='hiveadmin',
                #     password='P@ssw0rdsit', 
                #     database='default',
                #     auth='LDAP'
                # )
                hive_col = st.columns(1)[0]
                with hive_col:
                    hive_host =st.text_input("hive_host", key='hive_host')
                    hive_port =st.text_input("hive_port", key='hive_port')
                    hive_username =st.text_input("hive_username", key='hive_username')
                    hive_password =st.text_input("hive_password", key='hive_password')
                    hive_database =st.text_input("hive_database", key='hive_database')
                    hive_auth =st.text_input("hive_auth", key='hive_auth')

                    print('##############')
                    print(hive_host)
                    print(hive_port)
                    print(hive_username)
                    print(hive_password)
                    print(hive_database)
                    print(hive_auth)
                if st.button("Connect to Hive."):
                    print(st.session_state.connection)
                    try:
                        hive_conn = get_hive_conn(hive_host,hive_port,hive_username,hive_password \
                            ,hive_database, hive_auth)
                    
                        hive_ddl = generate_sql_ddl(hive_schema, selected_schema.lower(), selected_table,table_comment, \
                                            location = f'/staging/{selected_schema.lower()}/{selected_table.lower()}' , stored_as = 'PARQUET')
                        
                        st.text_area("Hive DDL", hive_ddl, height=200)
                        print(hive_ddl)
                        st.session_state.connection['hive_ddl'] = hive_ddl
                        st.session_state.connection['hive_conn'] = hive_conn
                        # if st.button("Create Table in Hive"):
                        #     print(type(st.session_state.connection['hive_conn']))
                        #     cursor = st.session_state.connection['hive_conn'].cursor()

                        #     cursor.execute('show databases;')
                        #     res = cursor.fetchall()
                        #     print(res)
                        #     st.warning('executed')
                            
                    except Exception as e:
                        st.error(f"Connection failed: {str(e)}")
                try:
                    if st.button("Create Table in Hive"):
                            cursor = st.session_state.connection['hive_conn'].cursor()
                            
                            cursor.execute(st.session_state.connection['hive_ddl'])
                            
                            st.success('executed')
                except Exception as e:
                    st.error(f"button failed {str(e)}")
                    
                
                    

if __name__ == "__main__":
    main()