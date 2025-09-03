import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# Ensure environment variable is set correctly
warehouse_id = "f50df4c3b0b8cb91" #os.getenv("DATABRICKS_WAREHOUSE_ID")
assert warehouse_id, "DATABRICKS_WAREHOUSE_ID environment variable not set"




# Initialize Config once for efficiency
# Removed unused Config() instantiation
# Use as sql query runner
def sqlQuery(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname = "adb-4151713458336319.19.azuredatabricks.net",
        http_path = f"/sql/1.0/warehouses/{warehouse_id}",
        # Don't touch token
        access_token = "dapi97bfcf4f2625d2d7d1c1982bcee6cf8d-3"
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()
        
def get_analyst_data(analyst, data_view, record_ids=None):
    if record_ids:
        record_ids_str = ",".join([f"'{rid}'" for rid in record_ids])
        if analyst == "Lisa Coppola" and data_view == "WIP":
            query = f"""SELECT record_id, wip_tab, published_tab, final_disposition_action, final_date, distribution_year, update_csv, ils_published, ils_submit_date, published_tab
                        FROM maxis_sandbox.engineering_standards.all_data_cleaned
                        WHERE UPPER(TRIM(record_id)) IN ({record_ids_str})"""
        else:
            query = f"""SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action, 
                               local_standards_replaced, replaced_by, ownership, process_step, location, 
                               current_step_date, days_in_step, num_pages, history
                        FROM maxis_sandbox.engineering_standards.all_data_cleaned
                        WHERE UPPER(TRIM(record_id)) IN ({record_ids_str})"""
    else:
        # Handle Lisa Coppola separately
        if analyst == "Lisa Coppola":
            if data_view == "WIP":
                query = """SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action,
                                  local_standards_replaced, replaced_by, ownership, process_step, location, 
                                  current_step_date, days_in_step, num_pages, history
                           FROM maxis_sandbox.engineering_standards.all_data_cleaned
                           WHERE wip_tab = TRUE"""
            elif data_view == "Published":
                query = """SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action,
                                  local_standards_replaced, replaced_by, ownership, process_step, location, 
                                  current_step_date, days_in_step, num_pages, history
                           FROM maxis_sandbox.engineering_standards.all_data_cleaned
                           WHERE published_tab = TRUE"""
            else:  # Both
                query = """SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action,
                                  local_standards_replaced, replaced_by, ownership, process_step, location, 
                                  current_step_date, days_in_step, num_pages, history
                           FROM maxis_sandbox.engineering_standards.all_data_cleaned"""
        else:
            # Other analysts
            if data_view == "WIP":
                query = f"""SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action,
                                  local_standards_replaced, replaced_by, ownership, process_step, location,
                                  current_step_date, days_in_step, num_pages, history
                           FROM maxis_sandbox.engineering_standards.all_data_cleaned
                           WHERE analyst = '{analyst}' AND wip_tab = TRUE"""
            elif data_view == "Published":
                query = f"""SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action,
                                  local_standards_replaced, replaced_by, ownership, process_step, location,
                                  current_step_date, days_in_step, num_pages, history
                           FROM maxis_sandbox.engineering_standards.all_data_cleaned
                           WHERE analyst = '{analyst}' AND published_tab = TRUE"""
            else:  # Both
                query = f"""SELECT record_id, wip_title, wip_tab, published_tab, project, submit_date, days_in_process, key_contact, action,
                                  local_standards_replaced, replaced_by, ownership, process_step, location,
                                  current_step_date, days_in_step, num_pages, history
                           FROM maxis_sandbox.engineering_standards.all_data_cleaned
                           WHERE analyst = '{analyst}'"""

    df = sqlQuery(query)

    # rename columns
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

def get_metrics(df):
    wip_count = df[df["WIP Tab"] == True].shape[0] if "WIP Tab" in df.columns else 0
    published_count = df[df["Published Tab"] == True].shape[0] if "Published Tab" in df.columns else 0
    return wip_count, published_count



st.set_page_config(layout="wide")
st.markdown("<h1 style='color:#1F2937;'>Engineering Standards Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<hr style='border:2px solid #3B82F6'>", unsafe_allow_html=True)
#st.image("gm-logo-solid-blue-sm.png", width=150)

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def getData():
    # This example query depends on the nyctaxi data set in Unity Catalog, see https://docs.databricks.com/en/discover/databricks-datasets.html for details
    return sqlQuery("select * from samples.nyctaxi.trips limit 5000")

data = getData()

# test

st.header("Engineering Standards GMW Tracking Sheet")

analyst = st.selectbox("Analyst:", ["Judy Brombach", "Stacy Weegman", "Greg Scofield", "Dave Haas", "Kim Thompson", "Rodger Mertz", "Greg Rushlow", "Lisa Coppola"])
st.write(f"Looking at {analyst}'s view")



col2, = st.columns(1)
with col2:
    st.subheader("Select desired information")
    data_view = st.radio("Select View:", ['WIP', 'Published', 'Both'])

    # Display the selected option using success message
    if data_view == 'WIP':
        st.success("WIP")
    elif data_view == 'Published':
        st.success("Published")
    elif data_view == 'Both':
        st.success("Both")

   
record_ids_input = st.text_input("Search Record IDs:")

# 1. Get the record IDs from input
record_ids = [rid.strip().upper() for rid in record_ids_input.split(",") if rid.strip()] if record_ids_input else None

# 2. Fetch all the analyst data
analyst_data = get_analyst_data(analyst, data_view)

# 3. Filter by record_ids if provided
if record_ids:
    analyst_data = analyst_data[analyst_data["Record ID"].isin(record_ids)]

# 4. Calculate metrics
wip_count, published_count = get_metrics(analyst_data)


def metric_box(label, value, bg_color="#f0f2f6", label_color="#333", value_color="#333"):
    st.markdown(
        f"""
        <div style="
            background-color: {bg_color};
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            font-size: 24px;
            font-weight: bold;
        ">
            <span style="color: {label_color};">{label}</span><br>
            <span style="font-size: 32px; color: {value_color};">{value}</span>
        </div>
        """,
        unsafe_allow_html=True
    )

if not analyst_data.empty:
    if data_view == "Both":
        col1, col2, col3 = st.columns(3)
        with col1:
            metric_box("WIP Records", wip_count, "#fcf8e3", label_color="#d9534f", value_color="#d9534f")
        with col2:
            metric_box("Published Records", published_count, "#dff0d8")
        with col3:
            metric_box("Total Records", wip_count + published_count, "#d9edf7")
        st.markdown("<br>", unsafe_allow_html=True)
    else:
        col = st.columns(1)[0]
        with col:
            if data_view == "WIP":
                metric_box(f"WIP Records for {analyst}", wip_count, "#fcf8e3", label_color="#d9534f", value_color="#d9534f")
            elif data_view == "Published":
                metric_box(f"Published Records for {analyst}", published_count, "#dff0d8")
        st.markdown("<br>", unsafe_allow_html=True)


    # Display the data in an editable grid
    header_class = "custom-header"
   
    gb = GridOptionsBuilder.from_dataframe(analyst_data)
    gb.configure_pagination(paginationAutoPageSize=True)  # pagination
    gb.configure_side_bar()  # enable columns panel
    gb.configure_default_column(editable=False, groupable=True, filter=True, sortable=True, resizable=True, headerClass=header_class)
    gb.configure_selection("single")
    gb.configure_column("WIP Title", width=700, headerClass=header_class)
    gb.configure_column("Key Contact", width=400, headerClass=header_class)
    gb.configure_column("Process Step", width=600, headerClass=header_class)
    gb.configure_column("History", width=400, headerClass=header_class)
    gb.configure_column("Record ID", width=300, headerClass=header_class)
    gb.configure_column("Days In Process", headerClass=header_class)
    gb.configure_column("Days in Step", headerClass=header_class)
    gb.configure_column("Pages", headerClass=header_class)
   
    
    gridOptions = gb.build()
    
    
    custom_css = {
    ".custom-header": {
        "background-color":"#d9edf7",
        "color": "black",
        "font-weight": "bold",
        "font-size": "16px"
    }
}

    grid_response = AgGrid(
        analyst_data,
        gridOptions=gridOptions,
        enable_enterprise_modules=False,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="balham",  # themes: "streamlit", "light", "dark", "blue", "fresh", "balham"
        height=600,
        custom_css=custom_css
 )
    

    # Track if popup should be shown
    if "show_popup" not in st.session_state:
     st.session_state.show_popup = False
    if "selected_row" not in st.session_state:
     st.session_state.selected_row = None

# 1️⃣ Get selected row from AgGrid
selected = grid_response["selected_rows"]

if selected and len(selected) > 0:
    st.session_state.selected_row = selected[0]  # store in session
    st.session_state.show_popup = True

# 2️⃣ Only render popup if a row is selected
if st.session_state.show_popup and st.session_state.selected_row:
    row = st.session_state.selected_row  # always safe

    st.markdown(
        """
        <style>
        .popup {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0px 0px 20px rgba(0,0,0,0.4);
            z-index: 9999;
            width: 400px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown("<div class='popup'>", unsafe_allow_html=True)
    st.subheader(f"Editing Record ID: {row['Record ID']}")

    with st.form("edit_row_form", clear_on_submit=True):
        updated_wip_title = st.text_input("WIP Title", row["WIP Title"])
        updated_key_contact = st.text_input("Key Contact", row["Key Contact"])
        updated_process_step = st.text_input("Process Step", row["Process Step"])

        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("💾 Save")
        with col2:
            cancel = st.form_submit_button("❌ Cancel")

        if submitted:
            rid = row["Record ID"]
            analyst_data.loc[analyst_data["Record ID"] == rid, ["WIP Title", "Key Contact", "Process Step"]] = [
                updated_wip_title, updated_key_contact, updated_process_step
            ]
            st.success("Row updated successfully!")
            st.session_state.show_popup = False
            st.session_state.selected_row = None
            st.experimental_rerun()

        if cancel:
            st.session_state.show_popup = False
            st.session_state.selected_row = None
            st.experimental_rerun()

    st.markdown("</div>", unsafe_allow_html=True)

else:
    st.warning("No data to display")