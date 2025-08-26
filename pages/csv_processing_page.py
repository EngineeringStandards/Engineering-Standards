import streamlit as st
import pandas as pd

st.title("This is the page to create reports for GMW's.")
st.sidebar.success("You are currently viewing the Reports page")

# File uploader widget
uploaded_file = st.file_uploader("Upload your document", type=["txt", "pdf", "docx", "xlsx"])

if uploaded_file is not None:
    st.write(f"Uploaded file: {uploaded_file.name}")

    # Display file content for text files
    if uploaded_file.type == "text/plain":
        content = uploaded_file.read().decode("utf-8")
        st.text_area("File Content", content, height=300)
    elif uploaded_file.type == "xlsx":
        df = pd.read_excel(uploaded_file)
        st.write(df)
    else:
        st.info("File preview is only available for text files.")
else:
    st.warning("Please upload a file to proceed.")
    st.write("You can upload documents such as text files, PDFs, Word documents, or Excel files for processing.")