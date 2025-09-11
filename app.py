# ============================================================================
# ENGINEERING STANDARDS DASHBOARD - SIMPLIFIED VERSION
# ============================================================================
import os
from databricks import sql
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# ============================================================================
# CONFIGURATION AND CONSTANTS
# ============================================================================
warehouse_id = "f50df4c3b0b8cb91"
DATABRICKS_SERVER = "adb-4151713458336319.19.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi97bfcf4f2625d2d7d1c1982bcee6cf8d-3"  # Don't touch token

# ============================================================================
# DATABASE CONNECTION AND QUERY FUNCTIONS
# ============================================================================
def sqlQuery(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

def get_analyst_data(record_ids=None):
    if record_ids:
        record_ids_str = ",".join([f"'{rid.upper().strip()}'" for rid in record_ids])
        query = f"""
            SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process,
                   key_contact, action, local_standards_replaced, replaced_by, ownership, process_step,
                   location, current_step_date, days_in_step, num_pages, history,
                   final_disposition_action, final_date, distribution_year, update_csv,
                   ils_published, ils_submit_date
            FROM maxis_sandbox.engineering_standards.all_data_cleaned
            WHERE UPPER(TRIM(record_id)) IN ({record_ids_str})
        """
    else:
        query = """
            SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process,
                   key_contact, action, local_standards_replaced, replaced_by, ownership, process_step,
                   location, current_step_date, days_in_step, num_pages, history,
                   final_disposition_action, final_date, distribution_year, update_csv,
                   ils_published, ils_submit_date
            FROM maxis_sandbox.engineering_standards.all_data_cleaned
        """

    df = sqlQuery(query)
    df = df.rename(columns={
        "record_id": "Record ID",
        "wip_title": "WIP Title",
        "project": "Project",
        "submit_date": "Submit Date",
        "days_in_process": "Days In Process",
        "key_contact": "Key Contact",
        "action": "Action",
        "local_standards_replaced": "Local Standards Replaced",
        "replaced_by": "Replaced By",
        "ownership": "Ownership",
        "process_step": "Process Step",
        "location": "Location",
        "current_step_date": "Current Step Date",
        "days_in_step": "Days in Step",
        "num_pages": "Pages",
        "history": "History",
        "wip_tab": "WIP Tab",
        "published_tab": "Published Tab",
        "final_disposition_action": "Final Disposition Action",
        "final_date": "Final Date",
        "distribution_year": "Distribution Year",
        "update_csv": "Update CSV",
        "ils_published": "ILS Published",
        "ils_submit_date": "ILS Submit Date"
    })
    return df

# ============================================================================
# STREAMLIT APP CONFIGURATION
# ============================================================================
st.set_page_config(layout="wide")
st.markdown("<h1 style='color:#1F2937;'>Engineering Standards Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<hr style='border:2px solid #3B82F6'>", unsafe_allow_html=True)

# ============================================================================
# SEARCH BAR ONLY
# ============================================================================
record_ids_input = st.text_input("Search Record IDs (comma separated):")
record_ids = [rid.strip().upper() for rid in record_ids_input.split(",") if rid.strip()] if record_ids_input else None

# ============================================================================
# FETCH DATA
# ============================================================================
analyst_data = get_analyst_data(record_ids)

if not analyst_data.empty:
    gb = GridOptionsBuilder.from_dataframe(analyst_data)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    gb.configure_default_column(editable=True, groupable=True, filter=True, sortable=True, resizable=True)
    gb.configure_selection("single")
    gb.configure_column("WIP Title", width=700)
    gb.configure_column("Key Contact", width=400)
    gb.configure_column("Process Step", width=600)
    gb.configure_column("History", width=400)
    gb.configure_column("Record ID", width=300)

    gridOptions = gb.build()

    custom_css = {
        ".ag-header-cell-label": {
            "background-color": "#d9edf7",
            "color": "black",
            "font-weight": "bold",
            "font-size": "16px"
        }
    }

    AgGrid(
        analyst_data,
        gridOptions=gridOptions,
        enable_enterprise_modules=False,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="balham",
        height=600,
        custom_css=custom_css
    )
else:
    st.warning("No data to display")
