import pandas as pd
from pathlib import Path
from app import sqlQuery

'''
List of cleaned headers to replace the messy original headers from the Excel file.
'''
def rename_headers():
    script_dir = Path(__file__).parent
    input_file = script_dir / "Table_ Master_List_of_Data.xlsx"  # file must be named input.xlsx
    output_excel = script_dir / "output_cleaned.xlsx"
    output_csv = script_dir / "output_cleaned.csv"

    # Read Excel
    df = pd.read_excel(input_file, dtype=str)

    if len(df.columns) != len(CLEAN_HEADERS):
        raise ValueError(
            f"Header count mismatch: file has {len(df.columns)} columns, "
            f"but expected {len(CLEAN_HEADERS)}"
        )

    # Replace headers
    df.columns = CLEAN_HEADERS

    # Save outputs in same folder
    df.to_excel(output_excel, index=False)
    df.to_csv(output_csv, index=False)

    print(f"✅ Saved cleaned Excel: {output_excel}")
    print(f"✅ Saved cleaned CSV: {output_csv}")

"""
List of basic columns to be used in SQL queries.
"""
base_columns = """tracking_id AS `Tracking ID`, 
                    record_id AS `Record ID`, 
                    record_id_num AS `Record ID Number`, 
                    title AS `Title`, 
                    filename AS `Filename`, 
                    team_name AS `Team Name`, 
                    author AS `Author`, 
                    owner AS `Owner`, 
                    owner_gmin AS `Owner GMIN`, 
                    gmws AS `GMWs`, 
                    status AS `Status`,
                    notes AS `Notes`"""

"""
Populate the dataframe with all CG records from the database.

Returns:
    pd.DataFrame: DataFrame containing all CG records.
"""
def base_cg_query():
    query = f"SELECT {base_columns} FROM maxis_sandbox.engineering_standards.cg_cleaned_data"
    df = sqlQuery(query)
    return df

"""
Create a new CG record in the database from a dictionary of data values.

Parameters:
    data (dict): Dictionary containing CG record data with keys matching database columns.
"""
def create_new_cg_record(data: dict):
    # Query to insert a new CG record into the database with base columns needed
    create_query = """
        INSERT INTO maxis_sandbox.engineering_standards.cg_cleaned_data 
        (tracking_id, title, author, status, notes)
        VALUES (?,?,?,?,?)  
        """

    # Tuple of values to insert into the database
    values = (data["form_tracking_id"], data["form_title"], data["form_author"], data["form_status"], data["form_notes"])
    sqlQuery(create_query, values)


"""
Find a specific CG record by using its Tracking ID.

Parameters:
    tracking_id (str): The CG Tracking ID to search for.
"""
def find_CG_by_tracking_id(tracking_id: str):
    # Prepare and execute SQL query to find the CG record
    search_query = f"SELECT {base_columns} FROM maxis_sandbox.engineering_standards.cg_cleaned_data WHERE tracking_id = '{tracking_id}'"
    df = sqlQuery(search_query)
    return df

"""
Update CG records in the database based on changes made in the Streamlit data editor.

Parameters:
    data (pd.DataFrame): The original dataframe containing CG records.
    updated_data (pd.DataFrame): The dataframe after edits made in the Streamlit editor.
"""
def update_records(data, updated_data):
    # Align indexes and columns before comparing
    aligned_data = data.reindex_like(updated_data)
    changes = updated_data.compare(aligned_data)

    # If no changes, exit early
    if changes.empty:
        return "No changes detected."

    # Goes through each changed row and updates the database
    for i in changes.index.get_level_values(0).unique():
        row = updated_data.loc[i]

        # Prepare SQL update statement
        query = """
            UPDATE maxis_sandbox.engineering_standards.cg_cleaned_data
            SET title = ?, author = ?, status = ?, notes = ?
            WHERE tracking_id = ?
        """
        values = (row["Title"], row["Author"], row["Status"], row["Notes"], row["Tracking ID"])
        
        # Execute SQL update
        sqlQuery(query, values)

"""
Delete a CG record from the database using its Tracking ID.

Parameters:
    tracking_id (str): The CG Tracking ID of the record to delete.
"""
def delete_cg_record(tracking_id: str):
    # Prepare and execute SQL delete statement
    delete_query = f"DELETE FROM maxis_sandbox.engineering_standards.cg_cleaned_data WHERE tracking_id = {tracking_id}"
    sqlQuery(delete_query)