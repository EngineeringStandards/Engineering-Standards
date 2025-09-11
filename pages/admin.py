# ============================================================================
# Admin DASHBOARD - with AgGrid
# ============================================================================
import streamlit as st
import pandas as pd
from databricks import sql
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# ============================================================================
# CONFIGURATION
# ============================================================================
warehouse_id = "f50df4c3b0b8cb91"
DATABRICKS_SERVER = "adb-4151713458336319.19.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi97bfcf4f2625d2d7d1c1982bcee6cf8d-3"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
@st.cache_data
def sqlQuery(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()
        

menu_options = [
    "All Records",
    "Records by Analyst",
    "Pending Updates",
    "Custom Query"
]

selection = st.sidebar("Select a query:", menu_options)