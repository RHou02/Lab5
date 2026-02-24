import os
import pandas as pd
import streamlit as st
import snowflake.connector 
import time
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
def get_conn():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="CS5542_DB",
        schema="APP",
        role=os.environ.get("SNOWFLAKE_ROLE"),

    )

st.title('CS5542 Lab 5 - Snowflake-backed Dashboard')
q = st.text_input('Search keyword', 'risk')

sql = f"""
SELECT DOC_ID, TITLE, SOURCE, PREVIEW
FROM APP.V_APP_DATA
WHERE PREVIEW ILIKE '%{q}%'
LIMIT 50;
"""
sql_detail = f"""
SELECT *
FROM APP.MASTER_CLAUSE_DATA
WHERE PREVIEW ILIKE '%{q}%'
LIMIT 50;
"""
t0 = time.time()
conn = get_conn()
df = pd.read_sql(sql,conn)
df_detailed = pd.read_sql(sql_detail, conn)
latency = time.time() - t0

st.metric("Latency (sec)", round(latency, 3))
tab1, tab2 = st.tabs(["📄 Basic Search", "📑 Detailed Clauses"])

with tab1:
    st.subheader("Results from V_APP_DATA")
    st.metric("Rows found", len(df))
    st.dataframe(df)
            
with tab2:
    st.subheader("Results from MASTER_CLAUSE_DATA")
    st.metric("Rows found", len(df_detailed))
    st.dataframe(df_detailed)




# ---- logging to pipeline_logs.csv ----
log_row = {
    "timestamp": datetime.utcnow().isoformat(),
    "user": os.environ["SNOWFLAKE_USER"], # EXTENSION 3: extra logging fields
    "feature_or_query": "keyword_search",
    "latency_sec": latency,
    "returned_count": len(df) + len(df_detailed)
}
st.write("log:", log_row)


def log_transaction(path: str, row_data: dict):
    """Appends a new record to the CSV log."""
    try:
        df = pd.read_csv(path)
        new_row = pd.DataFrame([row_data])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(path, index=False)
        return True
    except Exception as e:
        st.error(f"⚠️ Logging failed: {e}")
        return False

log_transaction('logs/pipeline_logs.csv', log_row)