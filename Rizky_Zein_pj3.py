import json
from unittest import result
#import logging
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine


schema_json ='user_address.json'
create_schema_sql = """Create Table if not exists user_address_2018_snapshoot {};"""
zip_small_file = 'temp/dataset-small.zip'
small_file_name = 'dataset-small.csv'
database='postgres'
user = 'postgres'
password = 'changeme'
host = '0.0.0.0'
port = 5432
table_name = 'user_address_2018_snapshoot'
result_test_sql = 'resul.sql'

with open(schema_json,'r') as schema:
    content =  json.loads(schema.read())

list_schema=[]
for c in content:
    col_name = c['column_name']
    col_type = c['column_type']
    constraint = c['is_null_able']
    ddl_list = [col_name,col_type,constraint]
    list_schema.append(ddl_list)


list_schema2 = []
for l in list_schema:
    s = ' '.join(l)
    list_schema2.append(s)


create_schema_sql_final = create_schema_sql.format(tuple(list_schema2)).replace("'","")
print(create_schema_sql_final)


##init connection
con = pg.connect(database=database,
                user = user,
                password = password,
                host = host,
                port = port
                )
con.autocommit=True
cursor=con.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL Schema Created Sucess....")
except pg.errors.DuplicateTable:
    print("Table already Created...")


# cursor.execute(create_schema_sql_final)
# print("DDL Schema Created Sucess....")

##Load File From ZipFile to DF
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name))

#print(df.head())

col_name_df = [c['column_name'] for c in content]
df.columns = col_name_df

#print(col_name_df)

df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

##insert to Postgress
df_filtered.to_sql(table_name,engine, if_exists='append',index=False)
print(f"Total Insert Row :{len(df_filtered)}")
print(f'Last Created at :{df_filtered.created_at.min()}')
print(f'Last Created at :{df_filtered.created_at.max()}')

cursor.execute(open(result_test_sql,'r').read())
result = cursor.fetchall()
print(result)