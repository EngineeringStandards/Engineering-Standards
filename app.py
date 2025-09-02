import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

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

st.set_page_config(layout="wide")

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

if record_ids_input:
    # Split on commas only, strip spaces
    record_ids = [rid.strip().upper() for rid in record_ids_input.split(",") if rid.strip()]

    if record_ids:
        # Build SQL-friendly string
        record_ids_str = ",".join([f"'{rid}'" for rid in record_ids])

        # Use different queries depending on analyst
        if analyst == "Lisa Coppola" and data_view == "WIP":
            analyst_data = sqlQuery(f"""
                SELECT record_id, wip_tab, final_disposition_action, final_date, distribution_year, update_csv, ils_published, ils_submit_date, published_tab  
                FROM maxis_sandbox.engineering_standards.all_data_cleaned
                WHERE UPPER(TRIM(record_id)) IN ({record_ids_str})
            """)
        else:
            analyst_data = sqlQuery(f"""
                SELECT record_id, wip_title, project, submit_date, days_in_process, key_contact, action, local_standards_replaced,replaced_by, ownership, process_step,location, current_step_date, days_in_step, num_pages, history
                FROM maxis_sandbox.engineering_standards.all_data_cleaned
                WHERE UPPER(TRIM(record_id)) IN ({record_ids_str})
            """)
    else:
        st.warning("Please enter at least one valid record_id.")
        analyst_data = pd.DataFrame()  # empty dataframe to avoid errors

else:
    # No search input, default analyst/data_view logic
    if analyst == "Lisa Coppola" and data_view == "WIP":
        analyst_data = sqlQuery("""
           SELECT record_id, wip_title, project, submit_date, days_in_process, key_contact, action, local_standards_replaced,replaced_by, ownership, process_step,location, current_step_date, days_in_step, num_pages, history
            FROM maxis_sandbox.engineering_standards.all_data_cleaned
            WHERE wip_tab = TRUE
        """)
    else:
        analyst_data = sqlQuery(f"""
           SELECT record_id, wip_title, project, submit_date, days_in_process, key_contact, action, local_standards_replaced,replaced_by, ownership, process_step,location, current_step_date, days_in_step, num_pages, history 
            FROM maxis_sandbox.engineering_standards.all_data_cleaned
            WHERE analyst = '{analyst}';
        """)


if not analyst_data.empty:
    gb = GridOptionsBuilder.from_dataframe(analyst_data)
    gb.configure_pagination(paginationAutoPageSize=True)  # pagination
    gb.configure_side_bar()  # enable columns panel
    gb.configure_default_column(editable=False, groupable=True, filter=True, sortable=True, resizable=True)
    gb.configure_column("wip_title", width=600)
    gb.configure_column("key_contact", width=200)
    gb.configure_grid_options(domLayout='normal')
    gridOptions = gb.build()


    AgGrid(
        analyst_data,
        gridOptions=gridOptions,
        enable_enterprise_modules=False,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="balham",  # themes: "streamlit", "light", "dark", "blue", "fresh", "balham"
        height=600,
        fit_columns_on_grid_load=True
    )
else:
    st.warning("No data to display")