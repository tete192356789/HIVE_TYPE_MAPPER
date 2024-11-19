import re

def generate_select_sql_ddl(db_type  ,hive_schema, schema_name, table_name):
    list_cols = {'ingdte':{'type':'STRING','comment':'วันเวลาที่ถ่ายโอนข้อมูลสู่ Big Data Platform'},'ingyer':{'type':'DECIMAL(4,0)','comment':'ปีที่ถ่ายโอนข้อมูลสู่ Big Data Platform'},'ingmth': {'type':'DECIMAL(2,0)','comment':'เดือนที่ถ่ายโอนข้อมูลสู่ Big Data Platform'},'ingday': {'type':'DECIMAL(2,0)','comment':'วันที่ถ่ายโอนข้อมูลสู่ Big Data Platform'}}
    date_cols = ['DATETIME','DATE','TIMESTAMP']
    ddl = f"SELECT \n"
    cols= []
    
    if db_type == 'sqlserver':
        for col in hive_schema[table_name]:
            if col['hive_type'] in date_cols:
                if (col['hive_type'] == 'DATETIME') | (col['hive_type'] == 'TIMESTAMP'):
                    cols.append(f"FORMAT({col['name']}, 'yyyy-MM-dd HH:mm:ss')")
                else:
                    cols.append(f"FORMAT({col['name']}, 'yyyy-MM-dd')")
            else:
                cols.append(f"{col['name']}")
        cols.append('CAST(YEAR(CURRENT_TIMESTAMP) AS INT) INGYER')
        cols.append('CAST(MONTH(CURRENT_TIMESTAMP) AS INT) INGMTH')
        cols.append('CAST(DAY(CURRENT_TIMESTAMP) AS INT) INGDAY')
        cols.append("FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') INGDTE")
        
    # for col in list_cols:
    #     cols.append(f"{col} {list_cols[col]['type']} COMMENT '{list_cols[col]['comment']}'")
    ddl += "    "
    ddl += ",\n    ".join(cols)
    ddl += "\n"

    ddl += f"FROM {schema_name}.{table_name}"

    return ddl

def generate_pdi_parquet_stg(db_type, hive_schema, pdi_parquet_mapping, table_name ):
    base_header = "Parquet Path	Name	Parquet Type	Precision	Scale	Default value	Null\n"
    script = base_header
    pattern = r"\w+\(\s*[^)]+,\s*[^)]*\)"
    pdi_parquet_mapping = pdi_parquet_mapping[db_type]
    for col in hive_schema[table_name]:
        match = re.match(pattern,col['hive_type'])
        row = f"{col['name']}\t{col['name']}\t"
        if match:
            splited = match.group().split('(')
            base = splited[0].lower()
            precision = splited[1].split(',')[0]
            scale = splited[1].split(',')[1].split(')')[0]
            row += f"{pdi_parquet_mapping[base]}\t{precision}\t{scale}\t\tYes\n"
        else:
            #this try for ex. char collate...
            try:
                row += f"{pdi_parquet_mapping[col['source_type'].lower()]}\t\t\t\tYes\n"
            except:
                row += f"UTF8\t\t\t\tYes\n"
        script += row    
    return script
        