import json
import pandas as pd
import time
from datetime import datetime
from pytrends.request import TrendReq
import gspread
from google.oauth2.service_account import Credentials

# Google Trends asetukset
pytrends = TrendReq(hl="en-US", tz=360)

# --- Lue hakusanat ---
with open("keywords_ruka.json", "r") as f:
    keywords = json.load(f)

# Luo dataframe kaikista hauista
all_results = []

for entry in keywords:
    term = entry["term"]
    geo = entry.get("geo", "")
    language = entry.get("language", "unknown")

    try:
        # Käytä aina viimeiset 12 kk viikkotasolla
        timeframe = "today 12-m"

        pytrends.build_payload([term], cat=0, timeframe=timeframe, geo=geo, gprop="")
        data = pytrends.interest_over_time()

        if not data.empty:
            data = data.reset_index()
            data["week_start"] = data["date"].dt.strftime("%Y-%m-%d")
            data["market"] = geo if geo else "global"
            data["language"] = language
            data["term"] = term
            data["source"] = "Google Trends"
            data.rename(columns={term: "trend_index_0_100"}, inplace=True)

            all_results.append(
                data[["week_start", "market", "language", "term", "source", "trend_index_0_100"]]
            )

        # odota 2 sekuntia, ettei Google Trends throttlaa
        time.sleep(2)

    except Exception as e:
        print(f"Error fetching {term}: {e}")

# Yhdistä tulokset
if all_results:
    df = pd.concat(all_results, ignore_index=True)
else:
    df = pd.DataFrame(columns=[
        "week_start", "market", "language", "term", "source", "trend_index_0_100"
    ])

# Placeholder-sarakkeet (jos myöhemmin lisätään Ads-data)
df["avg_monthly_searches"] = None
df["cpc"] = None
df["competition"] = None

# --- Tallenna CSV ---
df.to_csv("weekly_trends_output.csv", index=False)

# --- Päivitä Google Sheetiin ---
try:
    import os
    service_account_info = os.getenv("GOOGLE_SA_JSON")

    if service_account_info:
        creds = Credentials.from_service_account_info(
            json.loads(service_account_info),
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)

        # Vaihda tähän oma sheet-ID
        SHEET_ID = "YOUR_SHEET_ID"
        sh = client.open_by_key(SHEET_ID)

        # Luo tai ylikirjoita "weekly_trends" välilehti
        try:
            worksheet = sh.worksheet("weekly_trends")
            sh.del_worksheet(worksheet)
        except:
            pass

        worksheet = sh.add_worksheet(title="weekly_trends", rows="1000", cols="20")
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

        print("✅ Data updated in Google Sheet")
    else:
        print("⚠️ GOOGLE_SA_JSON secret not found")

except Exception as e:
    print(f"Error updating Google Sheet: {e}")
