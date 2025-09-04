import streamlit as st
import pandas as pd
from scripts.cg_processing import base_cg_query, find_CG_by_tracking_id, update_records

st.title("CG Dashboard")
st.sidebar.success("You are currently viewing the CG Dashboard")

"""
Ask Jim exactly what he wants on this page.

Make it possible to create a new CG record from this page?
"""
# Search box for CG Tracking ID
cg_search = st.text_input("CG Tracking ID", "Enter CG tracking ID to search")
st.write(f"Searching for CG tracking ID: {cg_search}")

# If no search term, show all records
if not cg_search or cg_search == "Enter CG tracking ID to search":
    data = base_cg_query()
else:
    data = find_CG_by_tracking_id(cg_search)

# Show the data in a table and an error message if no records are found with the search criteria
if data.empty:
    st.write("No records found.")
else:
    edited_data = st.data_editor(data, 
                                use_container_width=True, 
                                hide_index=True, 
                                num_rows="dynamic", 
                                disabled=["Tracking ID"]
                                )

    if st.button("Save changes"):
        update_records(data, edited_data)

