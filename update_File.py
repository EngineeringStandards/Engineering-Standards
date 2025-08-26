import streamlit as st
import pandas as pd

st.title("This is the page to update reports for GMW's.")
st.sidebar.success("You are currently viewing the Update page")

record_id = st.text_input("Record ID")

#Autopopulate other fields based on the current data in the database

#Add a button to update the database with the new data
if st.button(f"Update record"):
  st.write(f"Record {record_id} updated successfully!")