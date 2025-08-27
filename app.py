import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

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

    analyst = st.selectbox("Analyst:", ["Judy Brombach", "Stacy Weegman", "Greg Scofield", "Dave Haas", "Kim Thompson", "Rodger Mertz", "Greg Rushlow", "Lisa Coppola"])
    st.write(f"Looking at {analyst}'s view")


if analyst == "Lisa Smith" and data_view == "WIP":
    # Lisa's restricted WIP view
    analyst_data = sqlQuery("""
        SELECT * 
        FROM maxis_sandbox.engineering_standards.all_data_cleaned
        WHERE wip_tab = TRUE
    """)

   
else:
    # Default view for other analysts
    analyst_data = sqlQuery(f"SELECT * FROM maxis_sandbox.engineering_standards.all_data_cleaned WHERE analyst = '{analyst}';")

st.dataframe(data=analyst_data, height=600, use_container_width=True)
