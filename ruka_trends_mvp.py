# ruka_trends_mvp.py
# Ruka/Kuusamo Google Trends → weekly series → Google Sheets (GitHub Actions ready)
# Requirements: pytrends pandas numpy python-dateutil gspread google-auth

import argparse, json, os, sys, time
from pathlib import Path
import pandas as pd

try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None


def _maybe_sheet_update(df: pd.DataFrame):
    """Push DataFrame to Google Sheets if env vars are present."""
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
    return pd.DataFrame(data["terms"])


def fetch_trends(df_terms: pd.DataFrame, tz: int = 180) -> pd.DataFrame:
    """
    Fetch 12 months of weekly Google Trends for all terms, grouped by market.
    Uses 'today 12-m' timeframe to ensure weekly granularity.
    """
    if TrendReq is None:
        raise RuntimeError("pytrends not installed. Run: pip install pytrends")

    pytrends = TrendReq(hl="en-US", tz=tz)
    timeframe = "today 12-m"  # <-- forces weekly series for last 12 months
    results = []

    # group by market to set geo (FI, SE, DE, GB, NO)
    for market, group in df_terms.groupby("market"):
        geo = market
        terms = group["term"].tolist()

        # pytrends supports up to 5 keywords per request
        for i in range(0, len(terms), 5):
            kw_batch = terms[i : i + 5]
            try:
                pytrends.build_payload(
                    kw_batch, cat=0, timeframe=timeframe, geo=geo, gprop=""
                )
                df = pytrends.interest_over_time()
                # simple backoff—helps on throttling
                time.sleep(2)

                if df.empty:
                    continue

                df = df.drop(columns=["isPartial"], errors="ignore")
                df.reset_index(names=["date"], inplace=True)
                df["market"] = market

                # long format
                df_long = df.melt(
                    id_vars=["date", "market"],
                    var_name="term",
                    value_name="trend_index_0_100",
                )
                results.append(df_long)

            except Exception as e:
                print(f"[WARN] Failed batch {kw_batch} for geo {geo}: {e}", file=sys.stderr)
                # small extra delay after an error
                time.sleep(3)
                continue

    if not results:
        return pd.DataFrame(
            columns=[
                "week_start",
                "market",
                "language",
                "term",
                "source",
                "trend_index_0_100",
                "avg_monthly_searches",
                "cpc_eur",
                "competition_index",
            ]
        )

    all_trends = pd.concat(results, ignore_index=True)

    # join language/intent meta back
    all_trends = all_trends.merge(
        df_terms[["market", "term", "language", "intent"]],
        on=["market", "term"],
        how="left",
    )

    # to weeks (Mon as start)
    all_trends["date"] = pd.to_datetime(all_trends["date"])
    all_trends["week_start"] = all_trends["date"] - pd.to_timedelta(
        all_trends["date"].dt.weekday, unit="D"
    )
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

    # final column order
    weekly = weekly[
        [
            "week_start",
            "market",
            "language",
            "term",
            "source",
            "trend_index_0_100",
            "avg_monthly_searches",
            "cpc_eur",
            "competition_index",
        ]
    ]
    return weekly


def merge_keyword_planner(weekly_df: pd.DataFrame, kp_csv_path: Path) -> pd.DataFrame:
    """
    Optional: merge Google Ads Keyword Planner CSV export.
    Expected headers (any casing works; we rename commonly exported ones):
      Keyword | Avg. monthly searches | CPC (EUR) | Competition | [Market] | [Language]
    """
    kp = pd.read_csv(kp_csv_path)
    rename_map = {
        "Keyword": "term",
        "keyword": "term",
        "Avg. monthly searches": "avg_monthly_searches",
        "CPC (EUR)": "cpc_eur",
        "CPC": "cpc_eur",
        "Competition": "competition_index",
        "Market": "market",
        "Language": "language",
    }
    kp = kp.rename(columns=rename_map)

    # ensure required columns exist
    for col in ["term", "avg_monthly_searches", "cpc_eur", "competition_index"]:
        if col not in kp.columns:
            kp[col] = pd.NA

    join_cols = ["term"]
    if "market" in kp.columns and kp["market"].notna().any():
        join_cols.append("market")

    out = weekly_df.merge(
        kp[join_cols + ["avg_monthly_searches", "cpc_eur", "competition_index"]],
        on=join_cols,
        how="left",
        suffixes=("", "_kp"),
    )

    # prefer KP values when present
    for col in ["avg_monthly_searches", "cpc_eur", "competition_index"]:
        if f"{col}_kp" in out.columns:
            out[col] = out[f"{col}_kp"].combine_first(out[col])
            out = out.drop(columns=[f"{col}_kp"])

    return out


def main():
    p = argparse.ArgumentParser(description="Ruka Demand Trends MVP (weekly series)")
    p.add_argument("--terms", type=str, default="keywords_ruka.json", help="Path to keywords JSON")
    p.add_argument("--tz", type=int, default=180, help="Timezone offset minutes (Helsinki = 180)")
    p.add_argument("--kp_csv", type=str, default=None, help="Optional Google Ads Keyword Planner CSV to merge")
    p.add_argument("--out", type=str, default="weekly_trends_output.csv", help="Output CSV path")
    args = p.parse_args()

    terms_df = load_terms(Path(args.terms))
    weekly = fetch_trends(terms_df, tz=args.tz)
    if args.kp_csv:
        weekly = merge_keyword_planner(weekly, Path(args.kp_csv))

    weekly.to_csv(args.out, index=False)
    print(f"Saved: {args.out} (rows={len(weekly)})")
    _maybe_sheet_update(weekly)


if __name__ == "__main__":
    main()
