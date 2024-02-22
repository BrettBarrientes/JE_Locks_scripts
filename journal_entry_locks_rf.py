import gspread
import redis
from gspread_formatting import set_column_width
import pandas as pd
from time import sleep
import data

def connect_to_google_sheets(creds_file, spreadsheet_name, sheet_name):
    gc = gspread.service_account(filename=creds_file)
    ws = gc.open(spreadsheet_name)
    gsheet_name = ws.worksheet(sheet_name)
    return gsheet_name

def connect_to_redis(redis_host):
    return redis.Redis(host=redis_host)

def update_google_sheet(gsheet_name, data_df, column_index, sleep_time=1):
    strings_list = data_df['Journal_Entry_Locks'].tolist()
    prefix_to_remove = "Edit_"
    cleaned_strings = [record.replace(prefix_to_remove, "") for record in strings_list]

    for row, cleaned_string in enumerate(cleaned_strings, start=2):
        sleep(sleep_time)
        gsheet_name.update_cell(row, column_index, cleaned_string)

def main():
    # Constants
    creds_file = "journalentrylocks-credentials.json"
    redis_host = data.host
    spreadsheet_name = "Journal_Entry_Locks"
    sheet_name = "Journal_Entry_Locks"

    # Connect to Google Sheets
    gsheet_name = connect_to_google_sheets(creds_file, spreadsheet_name, sheet_name)

    # Clear sheet
    gsheet_name.clear()

    # Update header
    gsheet_name.update('A1', 'JE Operating Leases')
    gsheet_name.update('C1', 'JE Policy Regeneration Operating Leases')
    gsheet_name.update('E1', 'JE Capital Leases')

    # Connect to Redis
    r = connect_to_redis(redis_host)

    # Get data from Redis
    journal_entry_locks = list(r.scan_iter("Edit_*"))
    journal_entry_locks_list_str = [item.decode('utf-8') for item in journal_entry_locks]
    df = pd.DataFrame(journal_entry_locks_list_str, columns=['Journal_Entry_Locks'])

    # Filter data
    filtered_df_Cap = df[df['Journal_Entry_Locks'].str.contains("JournalEntry_Cap")]
    filtered_df_policy_Op = df[df['Journal_Entry_Locks'].str.contains("JournalEntry_IsPolicy_Op")]
    filtered_df_Op = df[df['Journal_Entry_Locks'].str.contains("JournalEntry_Op")]

    # Update Google Sheets
    update_google_sheet(gsheet_name, filtered_df_Cap, column_index=5)
    update_google_sheet(gsheet_name, filtered_df_policy_Op, column_index=3)
    update_google_sheet(gsheet_name, filtered_df_Op, column_index=1)

if __name__ == "__main__":
    main()