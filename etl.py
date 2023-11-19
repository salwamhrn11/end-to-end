from sqlalchemy import create_engine
import pandas as pd
import os
import pyodbc
from dotenv import load_dotenv
import pdb


load_dotenv()
#get password from environment var
pwd = os.getenv('PGPASS')
uid = os.getenv('PGUID')

#sql db details
driver = "{SQL Server Native Client 11.0}"
server = "localhost"
database = "dataRekdat"


#extract data from sql server
def extract():
    try:
        src_conn = pyodbc.connect(r'DRIVER=' + driver + ';SERVER=' + server + r'\SQLEXPRESS' + ';DATABASE=' + database + ';Trusted_Connection=yes')
        src_cursor = src_conn.cursor()
        breakpoint()
        #execute query
        src_cursor.execute("""SELECT t.name as table_name FROM sys.tables t WHERE t.name IN ('endtoend.fixData') """)
        src_tables = src_cursor.fetchall()

        for tbl in src_tables:
            #query and load save data to dataframe
            df = pd.read_sql_query(f'SELECT * FROM [endtoend].{tbl[0]}', src_conn)
            breakpoint()
            df.to_csv(f'fixData.csv', index=False)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        if src_conn is not None:
            src_conn.close()

#load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://etl:endtoend@localhost:5432/dataRekdat')
        breakpoint()
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')

        #save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        breakpoint()

        #add elapsed time to final print out
        print(f"Data imported successfully!")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))