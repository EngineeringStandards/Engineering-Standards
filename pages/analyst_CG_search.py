import streamlit as st
import pandas as pd
from scripts.cg_processing import base_cg_query, find_CG_by_tracking_id, update_records, create_new_cg_record

st.title("CG Dashboard")
st.sidebar.success("You are currently viewing the CG Dashboard")

# Search box for CG Tracking ID
cg_search = st.text_input("CG Tracking ID", "Enter CG tracking ID to search")
st.write(f"Searching for CG tracking ID: {cg_search}")

# Always refresh data based on search input
if not cg_search or cg_search == "Enter CG tracking ID to search":
    data = base_cg_query()
else:
    data = find_CG_by_tracking_id(cg_search)

st.session_state.cg_data = data  

# Show the data in a table and an error message if no records are found with the search criteria
if data.empty:
    st.write("No records found.")
else:
    edited_data = st.data_editor(
        data,
        key="cg_editor", 
        width="content",
        hide_index=True,
        num_rows="dynamic",
        disabled=["Tracking ID"]
    )

    if st.button("Save changes"):
        # Call function to process and save the changes made to the data
        update_records(data, edited_data)

        # Refresh data again after saving
        if not cg_search or cg_search == "Enter CG tracking ID to search":
            st.session_state.cg_data = base_cg_query()
        else:
            st.session_state.cg_data = find_CG_by_tracking_id(cg_search)

        st.rerun()

if "show_form" not in st.session_state:
    st.session_state.show_form = False

# Button that makes the form visibile
if st.button("Add CG record"):
    st.session_state.show_form = True

# Only render form if button was pressed
if st.session_state.show_form:
    with st.form("new_cg_form",enter_to_submit=False, clear_on_submit=True):
        st.write("New CG record form")

        # Form fields for CG record creation
        tracking_id = st.text_input("Tracking ID", key="form_tracking_id")
        title = st.text_input("Title", key="form_title")
        author = st.text_input("Author", key="form_author")
        status = st.selectbox("Status", ["Active", "Inactive", "Draft"], key="form_status")
        notes = st.text_area("Notes", key="form_notes")
        submitted = st.form_submit_button("Submit")

        if submitted:
            
            # When the form is submitted, pull all form values into a dictionary to create a new record
            values_dict = {k: st.session_state[k] for k in st.session_state if k.startswith("form_")}
            create_new_cg_record(values_dict)
            st.session_state.show_form = False 
            st.rerun()
