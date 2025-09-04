import streamlit as st
import pandas as pd
from scripts.cg_processing import base_cg_query, find_CG_by_tracking_id

st.title("CG Dashboard")
st.sidebar.success("You are currently viewing the CG Dashboard")

"""
Add an editable table to the page to show all CG records with a search box.
    call a function from cg_processing.py to get the data

Try to make all sql calls determined in the processing scripts to keep this page clean.

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
    st.data_editor(data, use_container_width=True, hide_index=True, num_rows="dynamic")
