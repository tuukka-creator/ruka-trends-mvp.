# ruka_trends_mvp.py
# Ruka/Kuusamo Google Trends → weekly series + recommendations + one-pager → Google Sheets
# Requires: pytrends pandas numpy python-dateutil gspread google-auth

import argparse, json, os, sys, time
from pathlib import Path
import pandas as pd

try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None


def _maybe_sheet_update(df: pd.DataFrame, worksheet: str):
    """Push DataFrame to Google Sheets if env vars are present."""
    sa_json = os.environ.get("GOOGLE_SA_JSON")
    sheet_name = os.environ.get("SHEET_NAME")
    sheet_id = os.environ.get("SHEET_ID")
    if not sa_json or not (sheet_name or sheet_id):
        print("No GOOGLE_SA_JSON or (SHEET_NAME/SHEET_ID) env found -> skipping sheet update for", worksheet)
        return
    try:
        from update_sheet import df_to_sheet
        # df_to_sheet osaa käyttää SHEET_ID:tä envistä, jos sellainen on
        df_to_sheet(df, sheet_name or "", worksheet)
        print(f"✅ Sheet updated: {(sheet_name or sheet_id)} / {worksheet} (rows={len(df)})")
    except Exception as e:
        print(f"[WARN] Failed to update Google Sheet for {worksheet}: {e}", file=sys.stderr)


def load_terms(json_path: Path) -> pd.DataFrame:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    # odotetaan: {"destination":"Ruka", "terms":[{market,language,intent,term}, ...]}
    return pd.DataFrame(data["terms"])


def fetch_trends(df_terms: pd.DataFrame, tz: int = 180) -> pd.DataFrame:
    """Fetch 12 months of weekly Google Trends for all terms, grouped by market."""
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

    weekly = weekly[
        ["week_start","market","language","term","source",
         "trend_index_0_100","avg_monthly_searches","cpc_eur","competition_index"]
    ]
    return weekly


# ---------- RECOMMENDATIONS & ONE-PAGER ----------

def _calc_growth_feats(weekly: pd.DataFrame) -> pd.DataFrame:
    """Laskee viimeisen viikon tason + 4vko/trendi-kasvun per termi."""
    df = weekly.copy()
    df = df.sort_values(["market","term","week_start"])
    # moving averages: viimeisin viikko, edelliset 4 viikkoa, sitä edeltävät 4 viikkoa
    latest_week = df["week_start"].max()
    df_last = df[df["week_start"] == latest_week].rename(columns={"trend_index_0_100":"latest"})
    # viimeiset 4 viikkoa
    last4_mask = df["week_start"] > (latest_week - pd.Timedelta(days=28))
    prev4_mask = (df["week_start"] <= (latest_week - pd.Timedelta(days=28))) & (df["week_start"] > (latest_week - pd.Timedelta(days=56)))
    last4 = (df[last4_mask].groupby(["market","language","term"], as_index=False)
             .agg(last4=("trend_index_0_100","mean")))
    prev4 = (df[prev4_mask].groupby(["market","language","term"], as_index=False)
             .agg(prev4=("trend_index_0_100","mean")))

    out = df_last.merge(last4, on=["market","language","term"], how="left") \
                 .merge(prev4, on=["market","language","term"], how="left")
    out["growth_vs_prev4"] = (out["last4"] - out["prev4"]).fillna(0)
    # yksinkertainen pisteytys: 70% taso, 30% kasvu
    out["score"] = out["latest"].fillna(0)*0.7 + out["growth_vs_prev4"]*0.3
    out["week_start"] = latest_week
    return out


def _channel_rules(row):
    term = row["term"].lower()
    intent = row.get("intent","") if isinstance(row.get("intent",""), str) else ""
    market = row["market"]

    # oletus
    funnel = "Mid → Lower"
    channels = ["Google Search (Exact/PH)", "Meta (Prospecting/RT)"]
    audience = "Travel intenders; site visitors; lookalikes"
    budget = "60% Search, 30% Meta, 10% Test"

    if "flight" in term or "lento" in term or "flüge" in term or "flyg" in term or intent == "flights":
        funnel = "Lower (intent) + Mid (consideration)"
        channels = ["Google Search (Exact/BMM)", "Meta (Video/Traffic)", "Performance Max (test)"]
        audience = "City O&D lookups (LON/STO/MUC), airline engagers, remarketing"
        budget = "70% Search, 20% Meta, 10% PMax"

    if intent in ("accommodation","brand"):
        channels = ["Google Search (Brand/Generic)", "Meta (RT/Catalogue)", "YouTube Shorts (test)"]
        funnel = "Lower (brand/generic) + Mid"
        audience = "Brand engagers; cart/room viewers; lookalikes"
        budget = "65% Search, 25% Meta, 10% YouTube"

    if intent in ("family","seasonal","activity"):
        channels = ["Meta (Reels/Stories)", "TikTok (Spark Ads)", "Google Discovery"]
        funnel = "Upper → Mid"
        audience = "Families; winter sports; northern lights; interest stacks"
        budget = "20% Search, 60% Social, 20% Discovery"

    # markkinakohtainen vivahde
    if market in ("GB","DE"):
        channels = list(dict.fromkeys(channels + ["Pinterest (test)"]))

    return funnel, ", ".join(channels), audience, budget


def build_recommendations(weekly: pd.DataFrame, terms_df: pd.DataFrame, top_n_per_market: int = 5) -> pd.DataFrame:
    feats = _calc_growth_feats(weekly.merge(terms_df[["market","language","term","intent"]], on=["market","language","term"], how="left"))
    # top N / market
    recs = (feats.sort_values(["market","score"], ascending=[True,False])
                 .groupby("market")
                 .head(top_n_per_market)
                 .reset_index(drop=True))

    rows = []
    for _, r in recs.iterrows():
        funnel, channels, audience, budget = _channel_rules(r)
        why = f"Latest={round(r['latest'],1)}, Δ4w={round(r['growth_vs_prev4'],1)}, score={round(r['score'],1)}"
        rows.append({
            "market": r["market"],
            "language": r["language"],
            "term": r["term"],
            "intent": r.get("intent",""),
            "funnel": funnel,
            "suggested_channels": channels,
            "audience_hint": audience,
            "budget_split": budget,
            "why_now": why
        })
    return pd.DataFrame(rows)


def build_one_pager(weekly: pd.DataFrame, recs: pd.DataFrame) -> pd.DataFrame:
    latest_week = weekly["week_start"].max()
    # Top opportunities: 5 parasta scorea
    top_ops = (recs.assign(score=recs["why_now"].str.extract(r"score=([\d\.]+)").astype(float))
                    .sort_values("score", ascending=False)
                    .head(5))
    # Watchouts: matala taso ja negatiivinen kasvu
    # Lasketaan nopeasti per term viimeisin taso ja kasvu
    feats = _calc_growth_feats(weekly.merge(recs[["market","language","term","intent"]].drop_duplicates(),
                                            on=["market","language","term"], how="right"))
    watch = feats[(feats["latest"] < 15) | (feats["growth_vs_prev4"] < -5)].sort_values(["latest"]).head(5)

    # Creative hooks: yksinkertainen lista intenttien mukaan
    hooks = [
        "‘Direct to Kuusamo’ + exact O&D copy (STO/LON/MUC)",
        "Family angle: ‘Lapland with kids’ + sledging/reindeer/Northern Lights",
        "Short videos (10–15s): first 1s hook with snow + price/availability",
        "Meta Advantage+ RT: dynamic hotel/chalet creatives",
        "GB/DE test: Pinterest travel boards"
    ]
    actions = [
        "Activate brand & generic Search + exact ‘Kuusamo flights’ per market",
        "Prospecting reels for family/seasonal in GB/DE; retarget site visitors",
        "Set up UTM & sheet dashboard to monitor week-over-week",
        "Pilot Performance Max against ‘flights/accommodation’ clusters",
        "Add remarketing lists from site (GA4 audiences) to Search"
    ]

    rows = []
    rows.append({"section":"Top opportunities", "content":"; ".join([f"{r['market']} – {r['term']} ({r['why_now']})" for _, r in top_ops.iterrows()])})
    rows.append({"section":"Watchouts", "content":"; ".join([f"{r['market']} – {r['term']} (latest={round(r['latest'],1)}, Δ4w={round(r['growth_vs_prev4'],1)})" for _, r in watch.iterrows()])})
    rows.append({"section":"Creative hooks", "content":"; ".join(hooks)})
    rows.append({"section":"Next actions", "content":"; ".join(actions)})
    rows.append({"section":"week", "content": str(latest_week.date())})
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser(description="Ruka Demand Trends MVP (weekly series + recs + one-pager)")
    p.add_argument("--terms", type=str, default="keywords_ruka.json", help="Path to keywords JSON")
    p.add_argument("--tz", type=int, default=180, help="Timezone offset minutes (Helsinki = 180)")
    p.add_argument("--kp_csv", type=str, default=None, help="Optional Google Ads Keyword Planner CSV to merge")
    p.add_argument("--out", type=str, default="weekly_trends_output.csv", help="Output CSV path")
    args = p.parse_args()

    terms_df = load_terms(Path(args.terms))
    weekly = fetch_trends(terms_df, tz=args.tz)

    weekly.to_csv(args.out, index=False)
    print(f"Saved: {args.out} (rows={len(weekly)})")

    # --- write weekly_trends ---
    _maybe_sheet_update(weekly, "weekly_trends")

    # --- build & write recommendations ---
    try:
        recs = build_recommendations(weekly, terms_df, top_n_per_market=5)
        _maybe_sheet_update(recs, "recommendations")
    except Exception as e:
        print(f"[WARN] Could not build recommendations: {e}", file=sys.stderr)
        recs = pd.DataFrame()

    # --- build & write one_pager ---
    try:
        if not recs.empty:
            one = build_one_pager(weekly, recs)
        else:
            # fallback jos recsit tyhjiä
            one = pd.DataFrame([{"section":"Note","content":"No recommendations generated this run."}])
        _maybe_sheet_update(one, "one_pager")
    except Exception as e:
        print(f"[WARN] Could not build one-pager: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
