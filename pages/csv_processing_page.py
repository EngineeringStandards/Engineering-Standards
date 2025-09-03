import streamlit as st
import pandas as pd
from pathlib import Path
from scripts.csv_processing import process_excel

st.title("Create Monthly Excel Reports to be Shared")
st.sidebar.success("You are currently viewing the Reports page")

uploaded_file = st.file_uploader("Upload your Excel document", type=["xlsx"])

if uploaded_file is not None:
    st.write(f"Uploaded file: {uploaded_file.name}")

    # Save uploaded file temporarily
    temp_file = Path(f"./{uploaded_file.name}")
    with open(temp_file, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # Process the Excel file
    output_files = process_excel(temp_file)

    st.success("Reports created successfully")
    for f in output_files:
        st.write(f"{f.name}")
        with open(f, "rb") as file_bytes:
            st.download_button(
                label=f"Download {f.name}",
                data=file_bytes,
                file_name=f.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
else:
    st.write("Please upload an Excel file to proceed.")