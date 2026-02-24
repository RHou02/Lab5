import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv() 

DATA_PATH = 'data/docs.csv'
DATA_CLAUSES_PATH = 'data/master_clauses.csv'


conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database='CS5542_DB',
    role=os.environ.get('SNOWFLAKE_ROLE')
)


select_col = ['Filename','Parties-Answer','Agreement Date-Answer','Effective Date-Answer','Expiration Date-Answer', 'Renewal Term-Answer','Notice Period To Terminate Renewal- Answer', 'Governing Law-Answer','Competitive Restriction Exception-Answer','Non-Compete-Answer' ]
mapping = {
    'Filename': 'FILENAME',
    'Parties-Answer': 'PARTIES',
    'Agreement Date-Answer': 'AGREEMENT_DATE',
    'Effective Date-Answer': 'EFFECTIVE_DATE',
    'Expiration Date-Answer': 'EXPIRATION_DATE',
    'Renewal Term-Answer': 'RENEWAL_TERM',
    'Notice Period To Terminate Renewal- Answer': 'NOTICE_PERIOD_TO_TERMINATE_RENEWAL',
    'Governing Law-Answer': 'GOVERNING_LAW',
    'Competitive Restriction Exception-Answer': 'COMPETITIVE_RESTRICTION',
    'Non-Compete-Answer': 'NON_COMPETE'
}

df_clauses = pd.read_csv(DATA_CLAUSES_PATH, usecols=select_col)
df_clauses.rename(columns=mapping, inplace=True)


for col in ['AGREEMENT_DATE', 'EFFECTIVE_DATE', 'EXPIRATION_DATE']:
    df_clauses[col] = pd.to_datetime(df_clauses[col], errors='coerce').dt.date
df_clauses.columns = [c.upper() for c in df_clauses.columns]


df_docs = pd.read_csv(DATA_PATH)
df_docs.columns = [c.upper() for c in df_docs.columns] 


ok1, n1, r1, _ = write_pandas(conn, df_clauses, 'MASTER_CLAUSE', schema='APP', auto_create_table=False)
print(f'MASTER_CLAUSE loaded: {ok1}, rows: {r1}')

ok2, n2, r2, _ = write_pandas(conn, df_docs, 'DOCS', schema='RAW', auto_create_table=False)
print(f'DOCS loaded: {ok2}, rows: {r2}')

conn.close()