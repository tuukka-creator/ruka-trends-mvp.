# ruka_trends_mvp.py
# Ruka/Kuusamo Trends → weekly series → Google Sheets (GitHub Actions)

import argparse, json, os, sys, time
from pathlib import Path
import pandas as pd

try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None


def _maybe_sheet_update(df: pd.DataFrame):
    sa_json = os.environ.get("GOOGLE_SA_JSON")
    sheet_name = os.environ.get("SHEET_NAME")
    if not sa_json or not sheet_name:
        print("No GOOGLE_SA_JSON or SHEET_NAME env found -> skipping sheet update.")
        return
    try:
        from update_sheet import df_to_sheet
        df_to_sheet(df, sheet_name, "weekly_trends")
        print(f"✅ Sheet updated: {sheet_name} / weekly_trends")
    except Exception as e:
        print(f"[WARN] Failed to update Google Sheet: {e}", file=sys.stderr)


def load_terms(json_path: Path) -> pd.DataFrame:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    # odotetaan rakennetta {"destination": "...", "terms":[{market,language,intent,term}, ...]}
    return pd.DataFrame(data["terms"])


def fetch_trends(df_terms: pd.DataFrame, tz: int = 180) -> pd.DataFrame:
    if TrendReq is None:
        raise RuntimeError("pytrends not installed. Run: pip install pytrends")

    pytrends = TrendReq(hl="en-US", tz=tz)
    timeframe = "today 12-m"  # 12 kk viikkotasolla
    results = []

    for market, group in df_terms.groupby("market"):
        geo = market  # FI/SE/DE/GB/NO
        terms = group["term"].tolist()
        for i in range(0, len(terms), 5):  # max 5 per pyyntö
            kw_batch = terms[i : i + 5]
            try:
                pytrends.build_payload(kw_batch, cat=0, timeframe=timeframe, geo=geo, gprop="")
                df = pytrends.interest_over_time()
                time.sleep(2)  # kevyt backoff

                if df.empty:
                    continue

                df = df.drop(columns=["isPartial"], errors="ignore")
                df.reset_index(names=["date"], inplace=True)
                df["market"] = market
                df_long = df.melt(
                    id_vars=["date", "market"],
                    var_name="term",
                    value_name="trend_index_0_100",
                )
                results.append(df_long)
            except Exception as e:
                print(f"[WARN] Failed batch {kw_batch} for geo {geo}: {e}", file=sys.stderr)
                time.sleep(3)
                continue

    if not results:
        return pd.DataFrame(
            columns=[
                "week_start","market","language","term","source",
                "trend_index_0_100","avg_monthly_searches","cpc_eur","competition_index"
            ]
        )

    all_trends = pd.concat(results, ignore_index=True)
    # liitä meta (language, intent)
    all_trends = all_trends.merge(
        df_terms[["market", "term", "language", "intent"]],
        on=["market", "term"],
        how="left",
    )

    all_trends["date"] = pd.to_datetime(all_trends["date"])
    all_trends["week_start"] = all_trends["date"] - pd.to_timedelta(all_trends["date"].dt.weekday, unit="D")
    weekly = (
        all_trends.groupby(["week_start", "market", "language", "term"], as_index=False)
        .agg(trend_index_0_100=("trend_index_0_100", "mean"))
        .sort_values(["week_start", "market", "term"])
        .reset_index(drop=True)
    )

    weekly["source"] = "GoogleTrends"
    weekly["avg_monthly_searches"] = pd.NA
    weekly["cpc_eur"] = pd.NA
    weekly["competition_index"] = pd.NA

    return weekly[
        ["week_start","market","language","term","source",
         "trend_index_0_100","avg_monthly_searches","cpc_eur","competition_index"]
    ]


def main():
    p = argparse.ArgumentParser(description="Ruka Demand Trends MVP (weekly series)")
    p.add_argument("--terms", type=str, default="keywords_ruka.json", help="Path to keywords JSON")
    p.add_argument("--tz", type=int, default=180, help="Timezone offset minutes (Helsinki = 180)")
    p.add_argument("--kp_csv", type=str, default=None, help="Optional Google Ads Keyword Planner CSV to merge")
    p.add_argument("--out", type=str, default="weekly_trends_output.csv", help="Output CSV path")
    args = p.parse_args()

    terms_df = load_terms(Path(args.terms))
    weekly = fetch_trends(terms_df, tz=args.tz)

    # (valinnainen) jos haluat myöhemmin yhdistää Keyword Planner CSV:n, lisää merge-funktio tähän

    weekly.to_csv(args.out, index=False)
    print(f"Saved: {args.out} (rows={len(weekly)})")
    _maybe_sheet_update(weekly)


if __name__ == "__main__":
    main()
