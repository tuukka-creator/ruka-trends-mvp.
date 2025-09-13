# ruka_trends_mvp.py
# Google Trends → weekly + precomputed KPIs + recommendations → Google Sheets

import argparse, json, os, sys, time
from pathlib import Path
import pandas as pd

try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None


def _maybe_sheet_update(df: pd.DataFrame, worksheet: str):
    sa_json = os.environ.get("GOOGLE_SA_JSON")
    sheet_name = os.environ.get("SHEET_NAME")
    sheet_id = os.environ.get("SHEET_ID")
    if not sa_json or not (sheet_name or sheet_id):
        print(f"No GOOGLE_SA_JSON or (SHEET_NAME/SHEET_ID) → skip '{worksheet}'")
        return
    try:
        from update_sheet import df_to_sheet
        df_to_sheet(df, sheet_name or "", worksheet)
        print(f"✅ Sheet updated: {(sheet_name or sheet_id)} / {worksheet} (rows={len(df)})")
    except Exception as e:
        print(f"[WARN] Failed to update '{worksheet}': {e}", file=sys.stderr)


def load_terms(json_path: Path) -> pd.DataFrame:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    return pd.DataFrame(data["terms"])  # columns: market, language, intent, term


def fetch_trends(df_terms: pd.DataFrame, tz: int = 180) -> pd.DataFrame:
    if TrendReq is None:
        raise RuntimeError("pytrends not installed. pip install pytrends")

    pytrends = TrendReq(hl="en-US", tz=tz)
    timeframe = "today 12-m"
    results = []

    for market, group in df_terms.groupby("market"):
        geo = market
        terms = group["term"].tolist()
        for i in range(0, len(terms), 5):
            kw_batch = terms[i : i + 5]
            try:
                pytrends.build_payload(kw_batch, cat=0, timeframe=timeframe, geo=geo, gprop="")
                df = pytrends.interest_over_time()
                time.sleep(2)
                if df.empty:
                    continue
                df = df.drop(columns=["isPartial"], errors="ignore").reset_index(names=["date"])
                df["market"] = market
                df_long = df.melt(
                    id_vars=["date", "market"],
                    var_name="term",
                    value_name="trend_index_0_100",
                )
                results.append(df_long)
            except Exception as e:
                print(f"[WARN] Failed batch {kw_batch} ({geo}): {e}", file=sys.stderr)
                time.sleep(3)

    if not results:
        return pd.DataFrame(
            columns=["week_start","market","language","term","source",
                     "trend_index_0_100","avg_monthly_searches","cpc_eur","competition_index"]
        )

    all_trends = pd.concat(results, ignore_index=True)
    all_trends = all_trends.merge(
        df_terms[["market","language","term","intent"]],
        on=["market","term"], how="left"
    )
    all_trends["date"] = pd.to_datetime(all_trends["date"])
    all_trends["week_start"] = all_trends["date"] - pd.to_timedelta(all_trends["date"].dt.weekday, unit="D")

    weekly = (all_trends.groupby(["week_start","market","language","term"], as_index=False)
              .agg(trend_index_0_100=("trend_index_0_100","mean"))
              .sort_values(["week_start","market","term"]))
    weekly["source"] = "GoogleTrends"
    weekly["avg_monthly_searches"] = pd.NA
    weekly["cpc_eur"] = pd.NA
    weekly["competition_index"] = pd.NA
    return weekly[["week_start","market","language","term","source",
                   "trend_index_0_100","avg_monthly_searches","cpc_eur","competition_index"]]


# --------- KPI FEATS + RECS ---------

def compute_kpis(weekly: pd.DataFrame, terms_df: pd.DataFrame) -> pd.DataFrame:
    df = weekly.merge(terms_df[["market","language","term","intent"]],
                      on=["market","language","term"], how="left").copy()
    df = df.sort_values(["market","term","week_start"])
    latest_week = df["week_start"].max()

    last4_mask = df["week_start"] > (latest_week - pd.Timedelta(days=28))
    prev4_mask = (df["week_start"] <= (latest_week - pd.Timedelta(days=28))) & \
                 (df["week_start"] >  (latest_week - pd.Timedelta(days=56)))

    latest = df[df["week_start"] == latest_week].rename(columns={"trend_index_0_100":"latest"})[
        ["market","language","term","latest","intent"]
    ]
    last4 = (df[last4_mask].groupby(["market","language","term"], as_index=False)
             .agg(last4=("trend_index_0_100","mean")))
    prev4 = (df[prev4_mask].groupby(["market","language","term"], as_index=False)
             .agg(prev4=("trend_index_0_100","mean")))

    out = latest.merge(last4, on=["market","language","term"], how="left") \
                .merge(prev4, on=["market","language","term"], how="left")
    out["growth_vs_prev4"] = (out["last4"] - out["prev4"]).fillna(0)
    out["score"] = out["latest"].fillna(0)*0.7 + out["growth_vs_prev4"]*0.3
    out["week"] = latest_week
    return out.sort_values(["market","score"], ascending=[True,False])


def _channel_rules(row):
    term = (row["term"] or "").lower()
    intent = (row.get("intent") or "").lower()
    market = row["market"]
    funnel = "Mid → Lower"
    channels = ["Google Search (Exact/PH)", "Meta (Prospecting/RT)"]
    audience = "Travel intenders; site visitors; lookalikes"
    budget = "60% Search, 30% Meta, 10% Test"
    if any(x in term for x in ["flight","lento","flüge","flyg"]) or intent == "flights":
        funnel = "Lower + Mid"
        channels = ["Google Search (Exact/BMM)", "Meta Video", "Performance Max (test)"]
        audience = "O&D lookups (LON/STO/MUC), remarketing"
        budget = "70% Search, 20% Meta, 10% PMax"
    if intent in ("accommodation","brand"):
        channels = ["Google Search (Brand/Generic)", "Meta RT", "YouTube Shorts (test)"]
        funnel = "Lower (brand/generic) + Mid"
        audience = "Brand engagers; room viewers; LAL"
        budget = "65% Search, 25% Meta, 10% YouTube"
    if intent in ("family","seasonal","activity"):
        channels = ["Meta Reels/Stories", "TikTok Spark", "Google Discovery"]
        funnel = "Upper → Mid"
        audience = "Families; winter sports; aurora"
        budget = "20% Search, 60% Social, 20% Discovery"
    if market in ("GB","DE"):
        channels = list(dict.fromkeys(channels + ["Pinterest (test)"]))
    return funnel, ", ".join(channels), audience, budget


def build_recommendations(kpis: pd.DataFrame, top_n_per_market: int = 5) -> pd.DataFrame:
    recs = (kpis.sort_values(["market","score"], ascending=[True,False])
                 .groupby("market").head(top_n_per_market).reset_index(drop=True))
    rows = []
    for _, r in recs.iterrows():
        funnel, channels, audience, budget = _channel_rules(r)
        why = f"latest={round(r['latest'],1)}, last4={round(r['last4'],1)}, prev4={round(r['prev4'],1)}, Δ4w={round(r['growth_vs_prev4'],1)}, score={round(r['score'],1)}"
        rows.append({
            "week": r["week"].date(),
            "market": r["market"],
            "language": r["language"],
            "term": r["term"],
            "intent": r.get("intent",""),
            "latest": round(r["latest"],1),
            "last4": round(r["last4"],1) if pd.notna(r["last4"]) else None,
            "prev4": round(r["prev4"],1) if pd.notna(r["prev4"]) else None,
            "growth_vs_prev4": round(r["growth_vs_prev4"],1),
            "score": round(r["score"],1),
            "funnel": funnel,
            "suggested_channels": channels,
            "audience_hint": audience,
            "budget_split": budget,
            "why_now": why
        })
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser(description="Ruka Trends → KPIs + Recs")
    p.add_argument("--terms", type=str, default="keywords_ruka.json")
    p.add_argument("--tz", type=int, default=180)
    p.add_argument("--out", type=str, default="weekly_trends_output.csv")
    args = p.parse_args()

    terms_df = load_terms(Path(args.terms))
    weekly = fetch_trends(terms_df, tz=args.tz)
    weekly.to_csv(args.out, index=False)
    print(f"Saved: {args.out} (rows={len(weekly)})")

    # 1) Raakadata (kuten ennen)
    _maybe_sheet_update(weekly, "weekly_trends")

    # 2) Esilasketut KPI:t
    kpis = compute_kpis(weekly, terms_df)
    _maybe_sheet_update(kpis, "metrics_weekly")

    # 3) Top suositukset
    recs = build_recommendations(kpis, top_n_per_market=5)
    if not recs.empty:
        _maybe_sheet_update(recs, "recommendations")


if __name__ == "__main__":
    main()
