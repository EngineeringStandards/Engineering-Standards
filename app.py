# ============================================================================
# ENGINEERING STANDARDS DASHBOARD - WITH SAVE BUTTON
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
# UPDATE RECORDS (similar to your CG Dashboard)
# ============================================================================
def update_records(original_df, edited_df):
    # detect diffs (basic example)
    original_aligned = original_df.reset_index(drop=True)
    edited_aligned = edited_df.reset_index(drop=True)
    diffs = []
    for idx, row in edited_aligned.iterrows():
        orig_row = original_aligned.loc[idx]
        for col in edited_aligned.columns:
            if pd.notna(row[col]) and row[col] != orig_row[col]:
                diffs.append({
                    "Record ID": row["Record ID"],
                    "Column": col,
                    "Old Value": orig_row[col],
                    "New Value": row[col]
                })

    if diffs:
        st.write("Detected changes:")
        st.dataframe(pd.DataFrame(diffs))

        # 👉 Here you’d loop over diffs and execute UPDATE queries in Databricks
        # Example:
        # for diff in diffs:
        #     query = f"""
        #     UPDATE maxis_sandbox.engineering_standards.all_data_cleaned
        #     SET {diff['Column']} = '{diff['New Value']}'
        #     WHERE record_id = '{diff['Record ID']}'
        #     """
        #     sqlQuery(query)
    else:
        st.info("No changes detected.")

# ============================================================================
# STREAMLIT APP
# ============================================================================
st.set_page_config(layout="wide")
st.markdown("<h1 style='color:#1F2937;'>Engineering Standards Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<hr style='border:2px solid #3B82F6'>", unsafe_allow_html=True)

# Search bar
record_ids_input = st.text_input("Search Record IDs (comma separated):")
record_ids = [rid.strip().upper() for rid in record_ids_input.split(",") if rid.strip()] if record_ids_input else None

# Fetch data
data = get_analyst_data(record_ids)
st.session_state.analyst_data = data

if data.empty:
    st.warning("No data to display")
else:
    gb = GridOptionsBuilder.from_dataframe(data)
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

    grid_response = AgGrid(
        data,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        theme="balham",
        height=600,
    )
    edited_data = pd.DataFrame(grid_response["data"])

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Save changes"):
            update_records(data, edited_data)

            # Refresh data after save
            if record_ids:
                st.session_state.analyst_data = get_analyst_data(record_ids)
            else:
                st.session_state.analyst_data = get_analyst_data()

            st.rerun()
