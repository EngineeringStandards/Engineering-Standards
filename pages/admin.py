import streamlit as st
import pandas as pd
from databricks import sql
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# ====================================================================
# CONFIGURATION
# ====================================================================
warehouse_id = "f50df4c3b0b8cb91"
DATABRICKS_SERVER = "adb-4151713458336319.19.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi97bfcf4f2625d2d7d1c1982bcee6cf8d-3"

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================
@st.cache_data
def sqlQuery(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# ====================================================================
# QUERY MENU
# ====================================================================
queries = [
    "001-CG1594 - WIP TAB",
    "002-CG1594 - PUBLISHED TAB",
    "2025 Workload",
    "Add Duplicate GMW to WIP",
    "Additional Documents",
    "All Active Standards",
    "Assign Multiple GMWs",
    "Body",
    "Body Changes",
    "CSV01 for Accuris - EXTERNAL RESTRICTED",
    "CSV02 for Accuris - HIGHLY RESTRICTED",
    "CSV03 for Accuris - EXTERNAL",
    "CSV04 for CG2569 - All",
    "CSV05 for CG2569 - Eng Stds Pub",
    "DNG",
    "Editables",
    "Fix Ownerships",
    "ILS: 001-WIP",
    "ILS: 002-OOPS - Ready to Publish",
    "ILS: 002-Ready to Publish",
    "ILS: 003-Submitted to Publisher",
    "ILS: 004-Published",
    "ILS: Batch Sheet - Additional Documents Folder",
    "ILS: Batch Sheet - ES Published Folder",
    "RECONCILE: Mark Published",
    "RECONCILE: Published Documents",
    "RECONCILE: TCIMM Notification",
    "Regionals",
    "Send to ILM folders-Remove from All Data",
    "SPCs",
    "Special Collection Honda Electrification (BEV)",
    "Special Collections - Monthly Publishing",
    "Update from CSV",
    "Year End Cleanup - Remove Duplicates",
    "Year End Cleanup - Remove Published",
    "Yearly ILM Review-Inactive",
    "Yearly ILM Review-LU/SUPER",
    "z0-Judy Tracking for Analysts - Status Report",
    "z0-Judy's Nagging List",
    "z1-Judy's Tracking List-WIP"
]


st.sidebar.title("Queries")
selection = None
for q in queries:
    if st.sidebar.button(q):
        selection = q
        break

# ====================================================================
# DISPLAY DATA
# ====================================================================
if selection:
    st.write(f"**Selected Query:** {selection}")
    
    sql_map = {
    "001-CG1594 - WIP TAB": """SELECT 
    Record_ID, 
    WIP_Title, 
    Project, 
    Submit_Date, 
    Days_in_Process, 
    Key_Contact, 
    Analyst, 
    Action, 
    Local_Standards_Replaced, 
    Replaced_By, 
    Checked_for_Regulatory, 
    CG_Tracking_ID, 
    WIP_TAB, 
    PUBLISHED_TAB, 
    Ownership, 
    Final_Date, 
    ILS_Ready_to_Publish, 
    team_name_gdm, 
    Distribution_YYYYMM, 
    Distribution_YYYYMM_GDM, 
    Single_Point_Contact, 
    Num_Pages, 
    Team_Name, 
    Engineering_Standards_Status_GDM, 
    History, 
    Comments_to_Retain, 
    ILS_Comments, 
    ILS_Status, 
    Whats_the_Plan, 
    ILS_WIP, 
    Distribution_Type_Permitted_GDM, 
    Information_Security_Classification_GDM, 
    wl_2023_transfers, 
    Judy_Tracking_for_Analyst, 
    show_on_judy_tracking, 
    workload_2025_comments,
    KC_GMIN, 
    KC_Email
FROM maxis_sandbox.engineering_standards.all_data_cleaned
WHERE WIP_TAB = True
ORDER BY Record_ID, WIP_Title
""",
    "002-CG1594 - PUBLISHED TAB": """select record_id,
wip_title,
project,
submit_date,
days_in_process,
key_contact,
analyst,
action,
local_standards_replaced,
replaced_by,
checked_for_regulatory,
distribution_year,
final_disposition_action,
final_date,
ils_published,
duplicates_for_published_tab_only,
process_step,
current_step_date,
num_pages,
ownership,
location,
days_in_step,
distribution_yyyymm_gdm,
wl_2023_transfers,
workload_2024_comments
from maxis_sandbox.engineering_standards.all_data_cleaned
where published_tab = true or duplicates_for_published_tab_only = true
order by record_id,final_date""",
    "2025 Workload": """select 
workload_2025_comments,
regional_elimination_planned_year,
team_name,
ownership,
single_point_contact,
key_contact,
date_editable_requested,
requestor_of_editable,
record_id,
hyperlink_latest_version_gdm,
name_gdm,
title_gdm,
engineering_standards_status_gdm,
native_language_title_gdm,
distribution_yyyymm_gdm,
distribution_type_permitted_gdm,
information_security_classification_gdm,
record_subtype_gdm,
standards_validation_area_gdm,
team_name_gdm,
stakeholders_gdm,
authors_gdm,
vpps_via_version,
vpps_via_level_1_gdm,
vpps_via_level_2_gdm,
vpps_via_level_3_gdm,
vpps_via_level_4_gdm,
standards_category_gdm,
standards_subcategory_gdm,
version_gdm,
user_comments_gdm,
description_gdm,
keyword_gdm,
alias_gdm,
region_gdm,
language_gdm,
electronic_collection_gdm,
regulatory_date_gdm,
regulatory_standards_gdm,
owner_gdm,
modifier_gdm,
source_system_gdm,
source_sys_load_date_gdm,
modifier_2_gdm,
object_id_gdm,
record_type_gdm,
global_process_area_gdm,
rim_category_gdm,
applicable_business_processes_gdm,
folder_gdm,
wip_tab,
published_tab,
analyst,
action,
project,
submit_date,
final_disposition_action,
final_date,
once,
wip_title,
do_not_include,
kc_gmin,
kc_email,
ils_ready_to_publish
from maxis_sandbox.engineering_standards.all_data_cleaned
WHERE Do_not_include = False AND Duplicates_for_Published_Tab_Only = False
ORDER BY Record_ID""",
"Add Duplicate GMW to WIP": "SELECT * FROM table_all_active_standards LIMIT 100",
"Additional Documents": """ SELECT
  Record_ID,
  WIP_Title,
  Team_Name,
  Key_Contact,
  Ownership,
  Single_Point_Contact,
  Distribution_YYYYMM,
  WIP_TAB,
  PUBLISHED_TAB,
  Engineering_Standards_Status_GDM,
  Distribution_Type_Permitted_GDM,
  Analyst,
  Distribution_YYYYMM_GDM,
  Comments_to_Retain,
  team_name_gdm,
  Folder_GDM,
  hyperlink_latest_version_gdm,
  KC_GMIN,
  KC_Email,
  Last_Analyst,
  Final_Disposition_Action,
  Final_Date
FROM maxis_sandbox.engineering_standards.all_data_cleaned
WHERE Folder_GDM = 'Additional Documents'
ORDER BY Record_ID;

""",
    "All Active Standards": """SELECT 
    record_id,
    wip_title,
    team_name,
    key_contact,
    ownership,
    single_point_contact,
    distribution_yyyymm,
    wip_tab,
    published_tab,
    engineering_standards_status_gdm,
    distribution_type_permitted_gdm,
    analyst,
    distribution_yyyymm_gdm,
    comments_to_retain,
    team_name_gdm,
    folder_gdm,
    hyperlink_latest_version_gdm,
    kc_gmin,
    kc_email,
    last_analyst,
    final_disposition_action,
    final_date
FROM maxis_sandbox.engineering_standards.all_data_cleaned
WHERE engineering_standards_status_gdm = 'Active'
ORDER BY record_id""",
    "Assign Multiple GMWs": """SELECT 
    record_id,
    wip_title,
    team_name,
    key_contact,
    ownership,
    single_point_contact,
    distribution_yyyymm,
    wip_tab,
    published_tab,
    engineering_standards_status_gdm,
    distribution_type_permitted_gdm,
    analyst,
    distribution_yyyymm_gdm,
    comments_to_retain,
    team_name_gdm,
    folder_gdm,
    hyperlink_latest_version_gdm,
    kc_gmin,
    kc_email,
    last_analyst,
    project,
    rfi_for_collaboration,
    dng,
    ils_wip,
    action,
    submit_date
FROM maxis_sandbox.engineering_standards.all_data_cleaned
ORDER BY record_id"""
}

    
    query_string = sql_map.get(selection, f"SELECT * FROM {selection.replace(' ', '_')} LIMIT 100")
    df = sqlQuery(query_string)
    
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination()
    gb.configure_default_column(editable=True)
    grid_options = gb.build()
    
    AgGrid(df, gridOptions=grid_options, update_mode=GridUpdateMode.NO_UPDATE)
