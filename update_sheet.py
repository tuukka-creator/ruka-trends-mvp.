# update_sheet.py
import os, json, gspread
from google.oauth2.service_account import Credentials

def df_to_sheet(df, sheet_name: str, worksheet: str):
    creds_json = os.environ["GOOGLE_SA_JSON"]  # repository secret
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)

    sheet_id = os.environ.get("SHEET_ID")  # optional but recommended
    if sheet_id:
        sh = gc.open_by_key(sheet_id)
        print(f"Using SHEET_ID: {sheet_id}")
    else:
        sh = gc.open(sheet_name)  # falls back to name (less reliable)
        print(f"Using SHEET_NAME: {sheet_name}")

    try:
        ws = sh.worksheet(worksheet)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet, rows="2000", cols="30")

    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
    print(f"Updated worksheet: {worksheet} (rows={len(df)})")
