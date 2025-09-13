
# Ruka Demand Trends — GitHub Actions
Weekly Google Trends → Google Sheets (Ruka/Kuusamo).

## Steps
1) Create a Google Sheet named **Ruka Demand Trends** (add tabs later if you want).
2) Create a **Service Account** (GCP → IAM → Service Accounts → JSON key).
3) Share the Sheet with the service account email as **Editor**.
4) In GitHub repo: Settings → Secrets and variables → Actions → New secret:
   - Name: `GOOGLE_SA_JSON`
   - Value: paste the downloaded JSON (entire content).
5) Push these files to your repo. The workflow runs on Mondays 06:05 UTC and on manual dispatch.

## Local run (optional)
```bash
python -m venv .venv && source .venv/bin/activate
pip install pytrends pandas numpy python-dateutil gspread google-auth
python ruka_trends_mvp.py --terms keywords_ruka.json --months 12 --tz 180 --out weekly_trends_output.csv
```
