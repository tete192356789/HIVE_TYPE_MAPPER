import streamlit as st
from sqlalchemy import create_engine, inspect
from sqlalchemy.types import *

import cx_Oracle
import pandas as pd
from urllib.parse import quote_plus
import ast
import re
from pyhive import hive
import os
from ddl import select_ddl_from_source, generate_pdi_parquet_stg, select_ddl_stg
from io import BytesIO
from copy import deepcopy

type_mappings_path = f'{os.path.dirname(os.path.abspath(__file__))}/type_mappings.txt'
pdi_parquet_mapping_path = f'{os.path.dirname(os.path.abspath(__file__))}/parquet_mappings.txt'
def read_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        return ast.literal_eval(content)
#oracle+cx_oracle://SYS:1923@localhost:1521/FREE
def get_connection_string(db_type, host, port, username, password, database):
    if db_type == "oracle":
        return f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "sqlserver":
        return f"mssql+pymssql://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "postgres":
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "mysql":
        return f"mysql+pymssql://{username}:{password}@{host}:{port}/{database}"

def get_schemas(engine):
    inspector = inspect(engine)
    return [schema for schema in inspector.get_schema_names() if schema != 'information_schema']

def get_tables(engine, schema):
    inspector = inspect(engine)
    return inspector.get_table_names(schema=schema)

def get_columns(engine, schema, table):
    inspector = inspect(engine)
    return inspector.get_columns(table, schema=schema)

def download_hive_ddl(label, data, file_name):
    dl_btn = st.download_button(
        label=label,
        data=data,
        file_name=file_name)
    return dl_btn

def write_bytes_excel(schema_list:list):
    sheet_list = ['staging','landing','gold']
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',
            'border': 1
        })

        for count , schema in enumerate(schema_list):
            df = pd.DataFrame(schema)
            excel_table = df.rename(columns={'name': 'source_name', 'hive_type': 'target_type'})
            excel_table['target_name'] = excel_table['source_name']
            excel_table = excel_table[['source_name', 'source_type', 'target_name', 'target_type']]
            excel_table = excel_table.drop_duplicates()
            
            excel_table.to_excel(writer, sheet_name= sheet_list[count], index=False)
            
            worksheet = writer.sheets[sheet_list[count]]
            
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 15)
            worksheet.set_column('C:C', 20)
            worksheet.set_column('D:D', 15)
            
            last_row = len(excel_table) + 1
            worksheet.autofilter(0, 0, last_row - 1, len(excel_table.columns) - 1)
            
            
            
            for col_num, value in enumerate(excel_table.columns.values):
                worksheet.write(0, col_num, value, header_format)
    
    output.seek(0)
    
    return output.getvalue()




def download_schema_excel(schema_list:list, file_name:str, key:str):
   
    excel_bytes = write_bytes_excel(schema_list)
    return st.download_button(
        label="Download Excel file",
        data=excel_bytes,
        file_name= file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key= key
    )

def get_hive_type(db_type , column_type):

    type_mappings = read_file(type_mappings_path)
    column_type = str(column_type.__repr__()).lower()
    if 'char' in column_type:
        return "STRING"
    match = re.match(r"\w+\(\s*[^)]+,\s*[^)]*\)",column_type)
        
    if match:
        dec_info = match.group().split('(')[1].split(',')
        try:
            precision = int(dec_info[0])
        except:
            precision = int(dec_info[0].split('=')[1])
        
        try:
            try:
                scale = int(dec_info[1].split(')')[0])
            except:
                scale = int(dec_info[1].split('=')[1].split(')')[0])
        except:
            scale = 0
            
        if precision > 38:
            precision = 38
        if (precision >0 and precision <= 38) and (scale >=0 and scale <= precision):
            return f"DECIMAL({precision},{scale})"

    

    
    column_type = column_type.split('(')[0]
    
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
                'source_type':str(column['type']),
                'comment':column['comment']}
              )
        schema[table_name] = columns

    for table_name in inspector.get_view_names(db_schema):
        columns = []
        for column in inspector.get_columns(table_name = table_name, schema = db_schema):
            hive_type = get_hive_type(db_type,column['type'])
            columns.append({
                'name':column['name'],
                'hive_type':hive_type,
                'source_type':str(column['type']),
                'comment':column['comment']}
              )
        schema[table_name] = columns
 
 
    return schema

def generate_sql_ddl(db_zone ,hive_schema, source_input,schema_name, table_name, table_comment, location , stored_as = 'PARQUET'):
    date_cols = ['TIMESTAMP','DATE']
    edit_schema = deepcopy(hive_schema[table_name])
   
    list_cols = {'ingdte':{'type':'STRING','comment':'วันเวลาที่ถ่ายโอนข้อมูลสู่ Big Data Platform'},'ingyer':{'type':'DECIMAL(4,0)','comment':'ปีที่ถ่ายโอนข้อมูลสู่ Big Data Platform'},'ingmth': {'type':'DECIMAL(2,0)','comment':'เดือนที่ถ่ายโอนข้อมูลสู่ Big Data Platform'},'ingday': {'type':'DECIMAL(2,0)','comment':'วันที่ถ่ายโอนข้อมูลสู่ Big Data Platform'}}
    if db_zone == 'staging':
        ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS staging.{source_input.lower()}_{schema_name.lower()}_{table_name.lower()} (\n"
        cols= []
        
        for i , col in enumerate(hive_schema[table_name]):
            comment = f"COMMENT '{col['comment']}'" if col['comment'] else ''
            if col['hive_type'] in date_cols:
                cols.append(f"{col['name']} STRING {comment}")
                edit_schema[i]['hive_type'] = "STRING"
            else:
                cols.append(f"{col['name']} {col['hive_type']} {comment}")
        for col in list_cols:
            cols.append(f"{col} {list_cols[col]['type']} COMMENT '{list_cols[col]['comment']}'")
            edit_schema.append({
                'name':f"{col}",
                'hive_type':f"{list_cols[col]['type']}",
                "source_type":f"",
                'comment':f"{list_cols[col]['comment']}"
            })
        ddl += "    "
        ddl += ",\n    ".join(cols)
        ddl += "\n)\n"

        # ddl += f"COMMENT '{table_comment['text']}'\n" if table_comment['text'] else ''
        ddl += f"COMMENT 'ตารางพักข้อมูลของตาราง {source_input.lower()}_{schema_name.lower()}_{table_name.lower()} จากระบบ {source_input}'\n"
        ddl += f"STORED AS {stored_as}\n"
        ddl += f"LOCATION '{location}'"
        # print('/////////stg')
        # print(edit_schema)
        return ddl, edit_schema

    if db_zone == 'landing':
        ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS landing.{source_input.lower()}_{schema_name.lower()}_{table_name.lower()} (\n"
        cols= []
        partition_col_nm = ['ingdte','ingyer','ingmth','ingday']
        for i, col in enumerate(hive_schema[table_name]):
            comment = f"COMMENT '{col['comment']}'" if col['comment'] else ''
            if col['name'] not in partition_col_nm and col['hive_type'] not in date_cols:
                cols.append(f"{col['name']} {col['hive_type']} {comment}")
            if col['hive_type']  in date_cols:
                cols.append(f"{col['name']} STRING {comment}")
                edit_schema[i]['hive_type'] = "STRING"
        cols.append(f"{partition_col_nm[0]} STRING COMMENT 'วันเวลาที่ถ่ายโอนข้อมูลสู่ Big Data Platform'")
        ddl += "    "
        ddl += ",\n    ".join(cols)
        ddl += "\n)\n"

        partition_cols =[]
       
        # ddl += f"PARTITIONED BY (ingyer DECIMAL(4,0) COMMENT,ingmth DECIMAL(2,0),ingday DECIMAL(2,0))\n"
        for col in list_cols:
            edit_schema.append({
                'name':f"{col}",
                'hive_type':f"{list_cols[col]['type']}",
                "source_type":f"",
                'comment':f"{list_cols[col]['comment']}"
            })
            if col != 'ingdte':
                partition_cols.append(f"{col} {list_cols[col]['type']} COMMENT '{list_cols[col]['comment']}'")
        ddl += f"COMMENT 'ตารางจัดเก็บประวัติการนำเข้าข้อมูลของตาราง {source_input.lower()}_{schema_name.lower()}_{table_name.lower()} จากระบบ {source_input}'\n"
        ddl += f"PARTITIONED BY (\n"
        ddl += "    "
        ddl += ",\n     ".join(partition_cols)
        ddl += "\n)\n"
        # ddl += f"COMMENT '{table_comment['text']}'\n" if table_comment['text'] else ''
        ddl += f"STORED AS {stored_as}\n"
        ddl += f"LOCATION '{location}'"

        # print('/////////landing')
        # print(edit_schema)
        return ddl, edit_schema
    
    if db_zone == 'gold':
        ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {source_input.lower()}.{schema_name.lower()}_{table_name.lower()} (\n"
        cols= []
        partition_col_nm = ['ingdte','ingyer','ingmth','ingday']
        for col in hive_schema[table_name]:
            comment = f"COMMENT '{col['comment']}'" if col['comment'] else ''
            if col['name'] not in partition_col_nm :
                cols.append(f"{col['name']} {col['hive_type']} {comment}")

        
        for col in list_cols:
            
            if col != 'ingdte':
                edit_schema.append({
                'name':f"{col}",
                'hive_type':f"{list_cols[col]['type']}",
                "source_type":f"",
                'comment':f"{list_cols[col]['comment']}"
                })  
                cols.append(f"{col} {list_cols[col]['type']} COMMENT '{list_cols[col]['comment']}'")
            else:
                edit_schema.append({
                'name':f"{col}",
                'hive_type':"TIMESTAMP",
                "source_type":f"",
                'comment':f"{list_cols[col]['comment']}"
                })
                cols.append(f"{col} TIMESTAMP COMMENT '{list_cols[col]['comment']}'")
        ddl += "    "
        ddl += ",\n    ".join(cols)
        ddl += "\n)\n"

        ddl += f"COMMENT '{table_comment['text']}'\n" if table_comment['text'] else ''
        ddl += f"PARTITIONED BY ()\n"
        ddl += f"STORED AS {stored_as}\n"
        ddl += f"LOCATION '{location}'"
        return ddl , edit_schema

    
def generate_insert_sql_ddl(db_zone, hive_schema,source_input, schema_name, table_name,insert_method ='Full Refresh'):

    # if db_zone == 'staging':
    #     ddl = f"INSERT INTO staging.{schema_name}_{table_name.lower()} \n"
    #     cols= []
    #     list_cols = {'ingdte':'DECIMAL(2,0)','ingyer':'DECIMAL(2,0)','ingmth': 'DECIMAL(2,0)','ingday': 'DECIMAL(2,0)'}
    #     for col in hive_schema[table_name]:
    #         cols.append(f"{col['name']} {col['hive_type']} {comment}")
    #     for col in list_cols:
    #         cols.append(f"{col} {list_cols[col]}")
    #     ddl += "    "
    #     ddl += ",\n    ".join(cols)
    #     ddl += "\n)\n"
        
    #     ddl += "VALUES \n"

    ing_cols = ['ingdte','ingyer','ingmth','ingday']
    date_cols = ['TIMESTAMP','DATE']
    if db_zone == 'landing':
        ddl = "set hive.exec.dynamic.partition=true;\n"
        ddl += "set hive.exec.dynamic.partition.mode=nonstrict;\n"
        ddl += "set hive.merge.smallfiles.avgsize=1280000000;\n"
        ddl += f"INSERT INTO landing.{source_input.lower()}_{schema_name.lower()}_{table_name.lower()} PARTITION(ingyer, ingmth, ingday)\n"
        ddl += f"SELECT \n"
        cols= []
        for col in hive_schema[table_name]:
            cols.append(f"{col['name']}")
        for col in ing_cols:
            cols.append(col)    
        ddl += "    "
        ddl += ",\n    ".join(cols)
        ddl += "\n"
        ddl += f"FROM staging.{source_input.lower()}_{schema_name.lower()}_{table_name.lower()}"
        return ddl
    
    if db_zone == 'gold':
        if insert_method == 'Full Refresh':
            ddl = "set hive.exec.dynamic.partition=true; \n"
            ddl += "set hive.exec.dynamic.partition.mode=nonstrict; \n"
            ddl += "set hive.merge.mapredfiles = true; \n"
            ddl += "set hive.merge.smallfiles.avgsize=1280000000; \n"

            ddl += f"INSERT OVERWRITE TABLE {source_input.lower()}.{schema_name}_{table_name.lower()} PARTITION () \n"
            ddl += f"SELECT \n"

            cols= []
            for col in hive_schema[table_name]:
                if col['hive_type'] in date_cols:
                    cols.append(f"CAST({col['name']} AS TIMESTAMP)")
                else:
                    cols.append(f"{col['name']}")

            for col in ing_cols:
                if col == 'ingdte':
                    cols.append(f"CAST({col} AS TIMESTAMP)")
                else:
                    cols.append(col)    
            ddl += "    "
            ddl += ",\n    ".join(cols)
            ddl += "\n"
            ddl += f"FROM landing.{source_input.lower()}_{schema_name.lower()}_{table_name.lower()} \n"
            ddl += "WHERE (${LATEST_LANDING_PARTITION})"
            return ddl
        

        if insert_method == 'Incremental':
            ddl = "set mapred.reduce.tasks=-1; \n"
            ddl += "set hive.exec.dynamic.partition=true; \n"
            ddl += "set hive.exec.dynamic.partition.mode=nonstrict; \n"
            ddl += "set hive.exec.max.dynamic.partitions=2048; \n"
            ddl += "set hive.exec.max.dynamic.partitions.pernode=512; \n"
            ddl += "set mapreduce.map.memory.mb = 3072; \n"
            ddl += "set mapreduce.reduce.memory.mb = 3072; \n"
            ddl += "set hive.merge.mapredfiles = true; \n"
            ddl += "set hive.merge.smallfiles.avgsize=1280000000; \n"
            ddl += "set hive.exec.max.created.files=200000; \n"
            
            ddl += f"INSERT OVERWRITE TABLE {source_input.lower()}.{schema_name.lower()}_{table_name.lower()} PARTITION () \n"
            ddl += "SELECT \n"
            cols =[]
            for col in hive_schema[table_name]:
                cols.append(f"{col['name']}")
            for col in ing_cols:
                cols.append(col)  
            total = len(cols)
            first  = round(total *(1/3))
            second = round(total *(2/3))
            cols.insert(first, '\n')
            cols.insert(second+1,'\n')

            ddl += "    "
            ddl += ", ".join(cols)
            ddl += "\n"
            
            ddl += "FROM (\n"
            ddl += "    SELECT *, ROW_NUMBER() OVER (PARTITION BY  ORDER BY INGDTE DESC) rn"
            ddl += "    FROM (\n"
            ddl += "        SELECT \n"
            ddl += "            "
            ddl += ", ".join(cols)
            ddl += "\n"

            ddl += f"       FROM {source_input.lower()}.{schema_name.lower()}_{table_name.lower()} \n"
            ddl += "        WHERE ${IMPACTED_PARTITION}\n"
            ddl += "        UNION ALL\n"
            ddl += "        SELECT\n"
            ddl += "            "
            cols = []
            for col in hive_schema[table_name]:
                if col['hive_type'] in date_cols:
                    cols.append(f"CAST({col['name']} AS TIMESTAMP)")
                else:
                    cols.append(f"{col['name']}")
            for col in ing_cols:
                if col == 'ingdte':
                    cols.append(f"CAST({col} AS TIMESTAMP)")
                else:
                    cols.append(col)  
            total = len(cols)
            first  = round(total *(1/3))
            second = round(total *(2/3))
            cols.insert(first, '\n')
            cols.insert(second+1,'\n')
            ddl += ", ".join(cols)
            ddl += "\n"
            ddl += f"       FROM landing.{source_input.lower()}_{schema_name.lower()}_{table_name.lower()} \n"
            ddl += "        WHERE ${LANDING_PARTITION}\n"
            ddl += "    )t1\n"
            ddl += ")t2\n"
            ddl += "WHERE rn=1 \n"
            ddl += "DISTRIBUTE BY"
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



st.markdown(
    """
    <style>
        [data-testid="stSidebar"][aria-expanded="true"]{
            min-width: 1500;
            max-width: 1500;
        }
        [data-testid="stSidebar"][aria-expanded="false"]{
            min-width: 1500;
            max-width: 1500;
         
        }
    </style>
    """,
    unsafe_allow_html=True,
)

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

 
    sqlalchemy_tab, hive_tab = st.tabs(["SQLAlchemy Tab", "Hive Tab"])
    with sqlalchemy_tab:
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
    with hive_tab:
        hive_col = st.columns(1)[0]
        with hive_col:
            hive_host =st.text_input("hive_host", key='hive_host')
            hive_port =st.text_input("hive_port", key='hive_port')
            hive_username =st.text_input("hive_username", key='hive_username')
            hive_password =st.text_input("hive_password", key='hive_password')
            hive_database =st.text_input("hive_database", key='hive_database')
            hive_auth =st.text_input("hive_auth", key='hive_auth')
        if st.button("Connect to Hive."):
            try:
                hive_conn = get_hive_conn(hive_host,hive_port,hive_username,hive_password \
                    ,hive_database, hive_auth)
                st.session_state.connection['hive_conn'] = hive_conn        
            except Exception as e:
                st.error(f"Connection failed: {str(e)}")

                  

    # Display schema dropdown if connection is established
    if st.session_state.connection['engine'] is not None:
        with st.sidebar:
            selected_schema = st.selectbox("Select Schema", st.session_state.connection['schemas'], 
                                        key='selected_schema')
            
            if selected_schema:


                if 'hive_schema' in st.session_state:
                    del st.session_state.hive_schema
                
                inspector = inspect(st.session_state.connection['engine'])
                # Store schema in session state
                st.session_state.hive_schema = convert_schema_to_hive(
                    st.session_state.connection['engine'], 
                    inspector, 
                    selected_schema,
                    db_type
                )
                # inspector = inspect(st.session_state.connection['engine'])
                # hive_schema = convert_schema_to_hive(st.session_state.connection['engine'], inspector, selected_schema,db_type)
                tables = get_tables(st.session_state.connection['engine'], selected_schema)
                st.session_state.connection['tables'] = tables
                
                selected_table = st.selectbox("Select Table", tables, key='selected_table')
                
                if selected_table:
                    # columns = get_columns(st.session_state.connection['engine'], selected_schema, selected_table)
                    source_input =  st.text_input("Input Source Name","ois")
                    st.write("Source Input : ",source_input)

                    hive_table_schema = st.session_state.hive_schema[selected_table]
                    table_comment = inspector.get_table_comment(schema = selected_schema,table_name = selected_table)
             
                    
                    st.write("Table Structure:")
                    df = pd.DataFrame(hive_table_schema)
                    st.dataframe(df)

                    # if st.button("Download Schema Excel",key='Download_excel_schema'):
                    #     get_schema_excel(df, f'{selected_schema}_{selected_table}_schema.xlsx')
                    #     st.success('Download Schema Excel Complete.')

                    # download_schema_excel(df, f'{selected_schema}_{selected_table}_schema.xlsx','excel')

                    
                    
                    parquet_tab, select_tab, create_tab, insert_tab = st.tabs(["Parquet Tab","Select Tab","Create Tab", "Insert Tab"])
                    
                    with parquet_tab:
                        st.header("PDI Parquet Ouput Script Generator.")
                        try:
                            pdi_parquet_mapping = read_file(pdi_parquet_mapping_path)
                            parquet_ddl = generate_pdi_parquet_stg(db_type,st.session_state.hive_schema, pdi_parquet_mapping, selected_table)
                            st.code(parquet_ddl, language="sql")
                            st.session_state.connection['hive_ddl'] = parquet_ddl
                        except:
                            st.code('', language="sql")
                        try:
                            download_hive_ddl('Download DDL AS Text File',  parquet_ddl, 'parquet_ddl')
                        except Exception as e:
                            st.error(f"button failed {str(e)}")

                    with select_tab:
                        select_source , select_stg = st.tabs(['Select From Source','Select From Staging'])

                        with select_source:
                            st.header("Select Hive Query From Source")
                            try:
                                select_ddl = select_ddl_from_source(db_type,st.session_state.hive_schema, source_input, selected_schema.lower(), selected_table)
                                st.code(select_ddl, language="sql")
                                st.session_state.connection['hive_ddl'] = select_ddl
                            except:
                                st.code('', language="sql")
                            try:
                                download_hive_ddl('Download DDL AS Text File', select_ddl, 'select_ddl')
                            except Exception as e:
                                st.error(f"button failed {str(e)}")

                        with select_stg:
                            st.header("Select Hive Query From Staging")
                            try:
                                selected_stg = select_ddl_stg(db_type,st.session_state.hive_schema, source_input, selected_schema.lower(), selected_table)
                                st.code(selected_stg, language="sql")
                                st.session_state.connection['hive_ddl'] = selected_stg
                            except:
                                st.code('', language="sql")
                            try:
                                download_hive_ddl('Download DDL AS Text File', selected_stg, 'selected_stg')
                            except Exception as e:
                                st.error(f"button failed {str(e)}")


                    with create_tab:
                        staging_create_tab, landing_create_tab, gold_create_tab = st.tabs(["staging Tab", "landing Tab", "gold Tab"])
                        with staging_create_tab:
                            st.subheader("Create Staging Hive Query")
                        
                            staging_create_ddl, stg_create_edit_schema = generate_sql_ddl('staging',st.session_state.hive_schema,source_input, selected_schema, selected_table,table_comment, \
                                        location = f'/staging/{source_input.lower()}/{selected_schema.lower()}/{selected_table.lower()}' , stored_as = 'PARQUET')
                            print('########')
                            print(stg_create_edit_schema)
                            st.code(staging_create_ddl, language="sql")
                            st.session_state.connection['hive_ddl'] = staging_create_ddl
                            download_hive_ddl('Download DDL AS Text File', staging_create_ddl, 'staging_create_ddl')
                            try:
                                if st.session_state.connection['hive_conn'] != '':
                                    if st.button("Create Table in Hive",key = 'staging_create_ddl_button'):
                                        cursor = st.session_state.connection['hive_conn'].cursor()
                                        
                                        cursor.execute(staging_create_ddl)
                                        
                                        st.success('executed')
                            except Exception as e:
                                st.error(f"button failed {str(e)}")

                            # download_schema_excel(pd.DataFrame(stg_create_edit_schema), f'staging_{selected_schema}_{selected_table}_schema.xlsx','excel_staging')
                        with landing_create_tab:
                            st.subheader("Create Landing Hive Query")

                            landing_create_ddl , landing_create_edit_schema = generate_sql_ddl('landing', st.session_state.hive_schema,source_input, selected_schema.lower(), selected_table,table_comment, \
                                        location = f'/landing/{source_input.lower()}/{selected_schema.lower()}/{selected_table.lower()}' , stored_as = 'PARQUET')
                            print('########')
                            print(landing_create_edit_schema)
                            st.code(landing_create_ddl, language="sql")
                            st.session_state.connection['hive_ddl'] = landing_create_ddl
                            download_hive_ddl('Download DDL AS Text File', landing_create_ddl, 'landing_create_ddl')
                            try:
                                if st.session_state.connection['hive_conn'] != '':
                                    if st.button("Create Table in Hive",key ='landing_create_ddl_button'):
                                        cursor = st.session_state.connection['hive_conn'].cursor()
                                        
                                        cursor.execute(landing_create_ddl)
                                        
                                        st.success('executed')
                            except Exception as e:
                                st.error(f"button failed {str(e)}")

                            # download_schema_excel(pd.DataFrame(landing_create_edit_schema), f'landing_{selected_schema}_{selected_table}_schema.xlsx','excel_landing')
                        with gold_create_tab:
                            st.subheader("Create Gold Hive Query")
                        
                            gold_create_ddl ,gold_create_edit_schema= generate_sql_ddl('gold',st.session_state.hive_schema, source_input, selected_schema.lower(), selected_table,table_comment, \
                                        location = f'/gold/{source_input.lower()}/{selected_schema.lower()}_{selected_table.lower()}' , stored_as = 'PARQUET')
                            print('GOLD------------')
                            print(gold_create_edit_schema)
                            # s= st.text_area("Hive DDL", staging_create_ddl, height=200,key='create_staging')
                            st.code(gold_create_ddl, language="sql")
                            st.session_state.connection['hive_ddl'] = gold_create_ddl
                            download_hive_ddl('Download DDL AS Text File', gold_create_ddl, 'gold_create_ddl')
                            try:
                                if st.session_state.connection['hive_conn'] != '':
                                    if st.button("Create Table in Hive",key = 'gold_create_ddl_button'):
                                        cursor = st.session_state.connection['hive_conn'].cursor()
                                        
                                        cursor.execute(gold_create_ddl)
                                        
                                        st.success('executed')
                            except Exception as e:
                                st.error(f"button failed {str(e)}")

                            # download_schema_excel(pd.DataFrame(landing_create_edit_schema), f'gold_{selected_schema}_{selected_table}_schema.xlsx','excel_gold')
                        schema_list = [stg_create_edit_schema,landing_create_edit_schema,gold_create_edit_schema]
                        print('$$$$$$$$$$$$$$$$$$')
                        print(schema_list)
                        download_schema_excel(schema_list, f'{source_input}_{selected_schema}_{selected_table}_schema.xlsx','download_excel')
        
                    
                    with insert_tab:
                        pdi_staging_tab,landing_insert_tab , gold_insert_tab= st.tabs(["PDI Staging Tab","Landing Insert Tab", "Gold Insert Tab"])
                        with pdi_staging_tab:
                            st.header("Pentaho Data Integration(PDI) Staging Insert Script.")

                        with landing_insert_tab:
                            st.header("Insert Hive Query")

                            insert_ddl = generate_insert_sql_ddl('landing',st.session_state.hive_schema, source_input,selected_schema.lower(), selected_table)
                            # st.text_area("Hive DDL", insert_ddl, height=200)
                            st.code(insert_ddl, language="sql")
                            st.session_state.connection['hive_ddl'] = insert_ddl
                            download_hive_ddl('Download DDL AS Text File', insert_ddl, 'insert_ddl')
                            try:
                                if st.session_state.connection['hive_conn'] != '':
                                    if st.button("Create Table in Hive",key ='insert_ddl_button'):
                                        cursor = st.session_state.connection['hive_conn'].cursor()
                                        
                                        cursor.execute(insert_ddl)
                                        
                                        st.success('executed')
                            except Exception as e:
                                st.error(f"button failed {str(e)}")
                        
                        with gold_insert_tab:
                            radio = st.radio('Select zone to insert.',['Full Refresh','Incremental'],horizontal = True)
                            if radio == 'Full Refresh':
                                st.subheader("Insert Full Refresh Gold Hive Query")
                            
                                gold_full_insert_ddl = generate_insert_sql_ddl('gold',st.session_state.hive_schema, source_input, selected_schema.lower(), selected_table,insert_method="Full Refresh")
                                
                                # s= st.text_area("Hive DDL", staging_create_ddl, height=200,key='create_staging')
                                st.code(gold_full_insert_ddl, language="sql")
                                st.session_state.connection['hive_ddl'] = gold_full_insert_ddl
                                download_hive_ddl('Download DDL AS Text File', gold_full_insert_ddl, 'gold_full_insert_ddl')
                                try:
                                    if st.session_state.connection['hive_conn'] != '':
                                        if st.button("Create Table in Hive",key = 'gold_full_insert_ddl_button'):
                                            cursor = st.session_state.connection['hive_conn'].cursor()
                                            
                                            cursor.execute(gold_full_insert_ddl)
                                            
                                            st.success('executed')
                                except Exception as e:
                                    st.error(f"button failed {str(e)}")
                            else:
                                st.subheader("Insert Incremental Gold Hive Query")
                            
                                gold_incremental_insert_ddl = generate_insert_sql_ddl('gold',st.session_state.hive_schema, source_input, selected_schema.lower(), selected_table,insert_method="Incremental")
                                
                                # s= st.text_area("Hive DDL", staging_create_ddl, height=200,key='create_staging')
                                st.code(gold_incremental_insert_ddl, language="sql")
                                st.session_state.connection['hive_ddl'] = gold_incremental_insert_ddl
                                download_hive_ddl('Download DDL AS Text File', gold_incremental_insert_ddl, 'gold_incremental_insert_ddl')

                                try:
                                    if st.session_state.connection['hive_conn'] != '':
                                        if st.button("Create Table in Hive",key = 'gold_incremental_insert_ddl_button'):
                                            cursor = st.session_state.connection['hive_conn'].cursor()
                                            
                                            cursor.execute(gold_incremental_insert_ddl)
                                            
                                            st.success('executed')
                                except Exception as e:
                                    st.error(f"button failed {str(e)}")
                    # df_list = [pd.DataFrame(stg_create_edit_schema), pd.DataFrame(landing_create_edit_schema), pd.DataFrame(gold_create_edit_schema)]
                    # schema_list = [stg_create_edit_schema,landing_create_edit_schema,gold_create_edit_schema]
                    # print('$$$$$$$$$$$$$$$$$$')
                    # print(schema_list)
                    # download_schema_excel(schema_list, f'{select_source}_{selected_schema}_{selected_table}_schema.xlsx','download_excel')
    
                    

if __name__ == "__main__":
    main()