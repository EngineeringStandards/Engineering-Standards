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

st.sidebar.title("Query Menu")

if st.sidebar.button("All Records"):
    selection = "All Records"
elif st.sidebar.button("Records by Analyst"):
    selection = "Records by Analyst"
elif st.sidebar.button("Pending Updates"):
    selection = "Pending Updates"
elif st.sidebar.button("Custom Query"):
    selection = "Custom Query"
else:
    selection = None

st.write(f"You selected: {selection}")