import streamlit as st
import pandas as pd
from scripts.cg_processing import base_cg_query

st.title("CG Search Page")
st.sidebar.success("You are currently viewing the CG search page")

"""
Add an editable table to the page to show all CG records with a search box.
    call a function from cg_processing.py to get the data

Use the cleaned headers from cg_processing.py

Try to make all sql calls determined in the processing scripts to keep this page clean.

Ask Jim exactly what he wants on this page.

Make it possible to create a new CG record from this page?
"""
data = base_cg_query()
st.dataframe(data)
