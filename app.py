# ============================================================================
# ENGINEERING STANDARDS DASHBOARD - with st.data_editor
# ============================================================================
import streamlit as st
import pandas as pd
from databricks import sql

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

@st.cache_data
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
    return df.rename(columns={
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

def update_records(original_df, edited_df):
    """Compare original vs edited and push updates."""
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

        # Example update loop (commented until ready for live updates):
        # for diff in diffs:
        #     query = f"""
        #         UPDATE maxis_sandbox.engineering_standards.all_data_cleaned
        #         SET {diff['Column']} = '{diff['New Value']}'
        #         WHERE record_id = '{diff['Record ID']}'
        #     """
        #     sqlQuery(query)
    else:
        st.success("No changes detected.")

# ============================================================================
# STREAMLIT APP
# ============================================================================
st.set_page_config(layout="wide")
st.title("Engineering Standards Dashboard")

record_ids_input = st.text_input("Search Record IDs (comma separated):")
record_ids = [rid.strip().upper() for rid in record_ids_input.split(",") if rid.strip()] if record_ids_input else None

data = get_analyst_data(record_ids)
st.session_state.analyst_data = data

if data.empty:
    st.warning("No data to display")
else:
    edited_data = st.data_editor(
        data,
        key="standards_editor",
        hide_index=True,
        num_rows="dynamic",
        use_container_width=True,
        disabled=["Record ID"]  # Lock primary key
    )

    if st.button("💾 Save changes"):
        update_records(data, edited_data)

        # Refresh after save
        if record_ids:
            st.session_state.analyst_data = get_analyst_data(record_ids)
        else:
            st.session_state.analyst_data = get_analyst_data()
        st.rerun()
