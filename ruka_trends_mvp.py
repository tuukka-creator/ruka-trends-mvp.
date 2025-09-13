
# ruka_trends_mvp.py
import argparse, json, os, sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None

def _maybe_sheet_update(df):
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
    data = json.loads(json_path.read_text(encoding='utf-8'))
    return pd.DataFrame(data['terms'])

def fetch_trends(df_terms: pd.DataFrame, start_months_ago: int = 12, tz: int = 180) -> pd.DataFrame:
    if TrendReq is None:
        raise RuntimeError("pytrends not installed. Run: pip install pytrends")
    pytrends = TrendReq(hl='en-US', tz=tz)
    results = []
   timeframe = "today 12-m"  # pakottaa viikkotason pisteet viimeisen 12 kk ajalta
    for market, group in df_terms.groupby('market'):
        geo = market
        terms = group['term'].tolist()
        for i in range(0, len(terms), 5):
            kw_batch = terms[i:i+5]
            try:
                pytrends.build_payload(kw_batch, timeframe=timeframe, geo=geo)
                df = pytrends.interest_over_time()
                if df.empty:
                    continue
                df = df.drop(columns=['isPartial'], errors='ignore')
                df.reset_index(names=['date'], inplace=True)
                df['market'] = market
                df_long = df.melt(id_vars=['date','market'], var_name='term', value_name='trend_index_0_100')
                results.append(df_long)
            except Exception as e:
                print(f"[WARN] Failed batch {kw_batch} for geo {geo}: {e}", file=sys.stderr)
                continue
    if not results:
        return pd.DataFrame(columns=['date','market','term','trend_index_0_100','language','intent'])
    all_trends = pd.concat(results, ignore_index=True)
    all_trends = all_trends.merge(df_terms[['market','term','language','intent']], on=['market','term'], how='left')
    all_trends['date'] = pd.to_datetime(all_trends['date'])
    all_trends['week_start'] = all_trends['date'] - pd.to_timedelta(all_trends['date'].dt.weekday, unit='D')
    weekly = (all_trends.groupby(['week_start','market','language','term'], as_index=False)
              .agg(trend_index_0_100=('trend_index_0_100','mean')))
    weekly['source'] = 'GoogleTrends'
    weekly['avg_monthly_searches'] = pd.NA
    weekly['cpc_eur'] = pd.NA
    weekly['competition_index'] = pd.NA
    weekly = weekly[['week_start','market','language','term','source',
                     'trend_index_0_100','avg_monthly_searches','cpc_eur','competition_index']]
    return weekly

def merge_keyword_planner(weekly_df: pd.DataFrame, kp_csv_path: Path) -> pd.DataFrame:
    kp = pd.read_csv(kp_csv_path)
    rename_map = {
        'Keyword': 'term', 'keyword':'term',
        'Avg. monthly searches':'avg_monthly_searches',
        'CPC (EUR)':'cpc_eur', 'CPC':'cpc_eur',
        'Competition':'competition_index',
        'Market':'market','Language':'language'
    }
    kp = kp.rename(columns=rename_map)
    for col in ['term','avg_monthly_searches','cpc_eur','competition_index']:
        if col not in kp.columns:
            kp[col] = pd.NA
    join_cols = ['term']
    if 'market' in kp.columns and kp['market'].notna().any():
        join_cols.append('market')
    out = weekly_df.merge(kp[join_cols+['avg_monthly_searches','cpc_eur','competition_index']],
                          on=join_cols, how='left', suffixes=('','_kp'))
    for col in ['avg_monthly_searches','cpc_eur','competition_index']:
        out[col] = out[col+'_kp'].combine_first(out[col])
        if col+'_kp' in out.columns:
            out = out.drop(columns=[col+'_kp'])
    return out

def main():
    p = argparse.ArgumentParser(description="Ruka Demand Trends MVP (GitHub Actions ready)")
    p.add_argument('--terms', type=str, default='keywords_ruka.json')
    p.add_argument('--months', type=int, default=12)
    p.add_argument('--tz', type=int, default=180)
    p.add_argument('--kp_csv', type=str, default=None)
    p.add_argument('--out', type=str, default='weekly_trends_output.csv')
    args = p.parse_args()

    terms_df = load_terms(Path(args.terms))
    weekly = fetch_trends(terms_df, start_months_ago=args.months, tz=args.tz)
    if args.kp_csv:
        weekly = merge_keyword_planner(weekly, Path(args.kp_csv))
    weekly.to_csv(args.out, index=False)
    print(f"Saved: {args.out} (rows={len(weekly)})")
    _maybe_sheet_update(weekly)

if __name__ == '__main__':
    main()
