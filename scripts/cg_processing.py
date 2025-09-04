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
                    status AS `Status`"""

'''
Function to populate the dataframe with all CG records from the database.
'''
def base_cg_query():
    query = f"SELECT {base_columns} FROM maxis_sandbox.engineering_standards.cg_cleaned_data"
    df = sqlQuery(query)
    return df

'''
Create a new CG record in the database from a dictionary of data values.
'''
def create_new_cg_record(data: dict):
    # Example function to create a new CG record in the database
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    query = f"INSERT INTO maxis_sandbox.engineering_standards.cg_cleaned_data ({columns}) VALUES ({placeholders})"
    values = list(data.values())
    # Execute the query using your database connection (not implemented here)
    sqlQuery(query)
    print("New CG record created successfully.")

'''
Edit an existing CG record in the database identified by Tracking ID, updating with values from updates dictionary.
'''
def edit_cg(tracking_id: str, updates: dict):
    # Example function to edit an existing CG record in the database
    set_clause = ', '.join([f"{col} = %s" for col in updates.keys()])
    query = f"UPDATE maxis_sandbox.engineering_standards.cg_cleaned_data SET {set_clause} WHERE tracking_id = %s"
    values = list(updates.values()) + [tracking_id]
    # Execute the query using your database connection (not implemented here)
    sqlQuery(query)
    print(f"CG record {tracking_id} updated successfully.")

'''
Function to get the next available tracking_id for a new CG record which will help so determine the next Record ID and Record ID Number.

Currently disables because Record ID and Record ID Number are not being auto-generated.
'''
"""
def get_next_tracking_id():
    query = "SELECT MAX(tracking_id) AS max_id FROM maxis_sandbox.engineering_standards.cg_cleaned_data"
    df = sqlQuery(query)
    max_id = df.at[0, 'max_id']
    next_id = (int(max_id) + 1) if max_id is not None else 1
    return str(next_id).zfill(6)  # Zero-pad to 6 digits
"""

'''
Function to find a CG record by its Tracking ID.
'''
def find_CG_by_tracking_id(tracking_id: str):
    search_query = f"SELECT {base_columns} FROM maxis_sandbox.engineering_standards.cg_cleaned_data WHERE tracking_id = '{tracking_id}'"
    df = sqlQuery(search_query)
    return df

"""
Update CG records in the database based on changes made in the Streamlit data editor.

Parameters:
    data (pd.DataFrame): The original dataframe containing CG records.
    updated_data (pd.DataFrame): The dataframe after edits made in the Streamlit editor.

Returns:
    str: A message indicating whether changes were detected and processed.
"""
def update_records(data, updated_data):
    # Align indexes and columns before comparing
    aligned_data = data.reindex_like(updated_data)
    changes = updated_data.compare(aligned_data)

    if changes.empty:
        return "No changes detected."

    for idx in changes.index.get_level_values(0).unique():
        row = updated_data.loc[idx]
        tracking_id = row["Tracking ID"]
        title = row["Title"]
        author = row["Author"] 
        status = row["Status"]

        query = (
            f"""UPDATE maxis_sandbox.engineering_standards.cg_cleaned_data 
            SET title = `{title}`, author = `{author}`, status = `{status}`
            WHERE tracking_id = `{tracking_id}`"""
        )
        sqlQuery(query)
