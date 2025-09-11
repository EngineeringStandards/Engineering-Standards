import streamlit as st
import pandas as pd
from databricks import sql
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# ====================================================================
# CONFIGURATION
# ====================================================================
warehouse_id = "f50df4c3b0b8cb91"
DATABRICKS_SERVER = "adb-4151713458336319.19.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi97bfcf4f2625d2d7d1c1982bcee6cf8d-3"

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================
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

# ====================================================================
# QUERY MENU
# ====================================================================
queries = [
    "001-CG1594 - WIP TAB",
    "002-CG1594 - PUBLISHED TAB",
    "2025 Workload",
    "Add Duplicate GMW to WIP",
    "Additional Documents",
    "All Active Standards",
    "Assign Multiple GMWs",
    "Body",
    "Body Changes",
    "CSV01 for Accuris - EXTERNAL RESTRICTED",
    "CSV02 for Accuris - HIGHLY RESTRICTED",
    "CSV03 for Accuris - EXTERNAL",
    "CSV04 for CG2569 - All",
    "CSV05 for CG2569 - Eng Stds Pub",
    "DNG",
    "Editables",
    "Fix Ownerships",
    "ILS: 001-WIP",
    "ILS: 002-OOPS - Ready to Publish",
    "ILS: 002-Ready to Publish",
    "ILS: 003-Submitted to Publisher",
    "ILS: 004-Published",
    "ILS: Batch Sheet - Additional Documents Folder",
    "ILS: Batch Sheet - ES Published Folder",
    "RECONCILE: Mark Published",
    "RECONCILE: Published Documents",
    "RECONCILE: TCIMM Notification",
    "Regionals",
    "Send to ILM folders-Remove from All Data",
    "SPCs",
    "Special Collection Honda Electrification (BEV)",
    "Special Collections - Monthly Publishing",
    "Update from CSV",
    "Year End Cleanup - Remove Duplicates",
    "Year End Cleanup - Remove Published",
    "Yearly ILM Review-Inactive",
    "Yearly ILM Review-LU/SUPER",
    "z0-Judy Tracking for Analysts - Status Report",
    "z0-Judy's Nagging List",
    "z1-Judy's Tracking List-WIP"
]


st.sidebar.title("Queries")
selection = None
for q in queries:
    if st.sidebar.button(q):
        selection = q
        break

# ====================================================================
# DISPLAY DATA
# ====================================================================
if selection:
    st.write(f"**Selected Query:** {selection}")
    
    # Example: replace this with actual SQL queries per selection
    sql_map = {
        "001-CG1594 - WIP TAB": "SELECT * FROM table_wip LIMIT 100",
        "002-CG1594 - PUBLISHED TAB": "SELECT * FROM table_published LIMIT 100",
        # add mapping for other queries...
    }
    
    query_string = sql_map.get(selection, f"SELECT * FROM {selection.replace(' ', '_')} LIMIT 100")
    df = sqlQuery(query_string)
    
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination()
    gb.configure_default_column(editable=True)
    grid_options = gb.build()
    
    AgGrid(df, gridOptions=grid_options, update_mode=GridUpdateMode.NO_UPDATE)
