import streamlit as st
import pandas as pd
from databricks import sql
from other_modules import sqlQuery  # reuse your existing helper

# =========================
# PAGE SETUP
# =========================
st.set_page_config(page_title="Admin Dashboard", layout="wide")
st.title("Admin Dashboard")

# =========================
# SIDEBAR MENU
# =========================
menu_options = [
    "All Records",
    "Records by Analyst",
    "Pending Updates",
    "Custom Query"
]
selection = st.sidebar.selectbox("Select a query:", menu_options)

# =========================
# QUERY LOGIC
# =========================
def get_all_records():
    query = "SELECT * FROM maxis_sandbox.engineering_standards.all_data_cleaned"
    return sqlQuery(query)

def get_records_by_analyst(analyst_name):
    query = f"""
        SELECT * FROM maxis_sandbox.engineering_standards.all_data_cleaned
        WHERE analyst = '{analyst_name}'
    """
    return sqlQuery(query)

def get_pending_updates():
    query = """
        SELECT * FROM maxis_sandbox.engineering_standards.all_data_cleaned
        WHERE final_date IS NULL
    """
    return sqlQuery(query)

# =========================
# RUN QUERY BASED ON SELECTION
# =========================
if selection == "All Records":
    df = get_all_records()

elif selection == "Records by Analyst":
    analyst_name = st.sidebar.text_input("Enter Analyst Name:")
    if analyst_name:
        df = get_records_by_analyst(analyst_name)
    else:
        df = pd.DataFrame()
        st.warning("Please enter an analyst name")

elif selection == "Pending Updates":
    df = get_pending_updates()

elif selection == "Custom Query":
    custom_query = st.text_area("Enter SQL query:")
    if st.button("Run Query"):
        if custom_query.strip():
            df = sqlQuery(custom_query)
        else:
            df = pd.DataFrame()
            st.warning("Enter a valid SQL query")
    else:
        df = pd.DataFrame()

# =========================
# DISPLAY RESULTS
# =========================
if df.empty:
    st.info("No data to display")
else:
    st.data_editor(
        df,
        key="admin_editor",
        hide_index=True,
        use_container_width=True
    )

# =========================
# OPTIONAL: Save changes
# =========================
if st.button("💾 Save changes"):
    edited_data = st.session_state.admin_editor
    # call your update function
    # update_records(df, edited_data)
    st.success("Changes saved!")
