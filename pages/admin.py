# ============================================================================ 
# ENGINEERING STANDARDS DASHBOARD - Admin Page
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
            df = cursor.fetchall_arrow().to_pandas()
    # Convert problematic date columns to string
    for col in ["submit_date", "current_step_date", "final_date", "ils_submit_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

def get_query_results(query_type):
    if query_type == "All Records":
        query = "SELECT * FROM maxis_sandbox.engineering_standards.all_data_cleaned"
    elif query_type == "By Analyst":
        analyst = st.text_input("Enter analyst name:").strip()
        query = f"""
            SELECT * 
            FROM maxis_sandbox.engineering_standards.all_data_cleaned
            WHERE analyst = '{analyst}'
        """
    elif query_type == "Active Projects":
        query = """
            SELECT * 
            FROM maxis_sandbox.engineering_standards.all_data_cleaned
            WHERE action = 'Active'
        """
    else:
        query = "SELECT * FROM maxis_sandbox.engineering_standards.all_data_cleaned LIMIT 0"
    return sqlQuery(query)

def update_records(original_df, edited_df):
    diffs = []
    for idx, row in edited_df.iterrows():
        orig_row = original_df.loc[idx]
        for col in edited_df.columns:
            if pd.notna(row[col]) and row[col] != orig_row[col]:
                diffs.append({
                    "Record ID": row["Record ID"],
                    "Column": col,
                    "Old Value": orig_row[col],
                    "New Value": row[col]
                })
    if diffs:
        st.warning("Detected changes:")
        st.dataframe(pd.DataFrame(diffs))
    else:
        st.success("No changes detected.")

# ============================================================================ 
# STREAMLIT APP
# ============================================================================ 
st.set_page_config(layout="wide")
st.title("Engineering Standards Admin Dashboard")

# Sidebar menu
query_type = st.sidebar.selectbox("Select query", ["All Records", "By Analyst", "Active Projects"])

# Fetch data based on selection
data = get_query_results(query_type)
st.session_state.admin_data = data

if data.empty:
    st.warning("No data to display")
else:
    # ============================= 
    # AgGrid setup
    # ============================= 
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_default_column(editable=True, filter="agTextColumnFilter", sortable=True, resizable=True, minWidth=200)
    gb.configure_column("Record ID", editable=False, width=200)
    gb.configure_column("WIP Title", width=600)
    gb.configure_column("Key Contact", width=400)
    gb.configure_column("Process Step", width=600)
    gb.configure_column("History", width=500)
    gb.configure_column("Ownership", width=300)
    gb.configure_column("Analyst", width=300)
    gb.configure_column("Project", width=300)
    gb.configure_column("Submit Date", width=300)
    gb.configure_column("Days in Process", width=300)
    gb.configure_column("Action", width=300)

    grid_options = gb.build()

    grid_response = AgGrid(
        data,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        theme="balham",
        height=600
    )

    edited_data = pd.DataFrame(grid_response["data"])

    if st.button("💾 Save changes"):
        update_records(data, edited_data)
        # Refresh after save
        st.experimental_rerun()
