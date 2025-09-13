
# update_sheet.py
import os, json, gspread
from google.oauth2.service_account import Credentials

def df_to_sheet(df, sheet_name: str, worksheet: str):
    creds_json = os.environ["GOOGLE_SA_JSON"]
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open(sheet_name)
    try:
        ws = sh.worksheet(worksheet)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet, rows="1000", cols="20")
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
