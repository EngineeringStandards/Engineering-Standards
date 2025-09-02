import pandas as pd
from pathlib import Path
from openpyxl import load_workbook

"""
A step to make the columns auto-sized based on content.
"""
def autosize_excel_columns(file_path: Path):
    wb = load_workbook(file_path)
    ws = wb.active

    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        ws.column_dimensions[col_letter].width = max_length + 2

    wb.save(file_path)

"""
Takes an Excel file path, processes it, and generates 4 reports.
"""
def process_excel(file_path: Path):
    df = pd.read_excel(file_path)
    output_folder = Path(file_path).parent

    current_month = df['Distribution (YYYYMM)'].max()
    current_month_for_title = str(current_month)
    final_current_month_for_title = current_month_for_title[:4] + "-" + current_month_for_title[4:]

    # === COPY 1: CG2569 All Published Standards YYYY-MM-01 including subfolders ===
    columns_to_drop = [
        'VPPS/VIA Version', 'Country', 'Referenced Records', 'Personal Information Included',
        'Non-Disclosure Agreement Applies', 'Export Control Number', 'Source Rec ID',
        'Source Rec Info', 'Source Rec URL', 'Record Status', 'Format'
    ]
    copy1_df = df.drop(columns=columns_to_drop)
    copy1_file = output_folder / f"CG2569 All Published Standards {final_current_month_for_title}-01 including subfolders.xlsx"
    copy1_df.to_excel(copy1_file, index=False)
    autosize_excel_columns(copy1_file)

    # === COPY 2: For smartsearch ===
    copy2_df = df[(df['Engineering Standards Status'] == "Active") & (df['Distribution (YYYYMM)'] == current_month)]
    copy2_file = output_folder / f"CG2569 All Published Standards {final_current_month_for_title}-01 including subfolders - for smartsearch.xlsx"
    copy2_df.to_excel(copy2_file, index=False)
    autosize_excel_columns(copy2_file)

    # === COPY 3: All Published Standards YYYY-MM-01 with urls ===
    copy3_df = df[df['Folder'] == 'Engineering Standards Published'].sort_values(by='Record ID', ascending=True)
    copy3_file = output_folder / f"CG2569 All Published Standards {final_current_month_for_title}-01 with urls.xlsx"
    copy3_df.to_excel(copy3_file, index=False)
    autosize_excel_columns(copy3_file)

    # === COPY 4: Published Standards for current month ===
    copy4_df = df[df['Distribution (YYYYMM)'] == current_month][
        ["Record ID", "Name", "Title", "Engineering Standards Status", "Distribution (YYYYMM)", "Record Subtype", "Description"]
    ].sort_values(by='Record ID', ascending=True)
    copy4_file = output_folder / f"{final_current_month_for_title}_Published.xlsx"
    copy4_df.to_excel(copy4_file, index=False)
    autosize_excel_columns(copy4_file)

    return [copy1_file, copy2_file, copy3_file, copy4_file]
