import pandas as pd
import json
import logging
import sys

from quant_value_vn.pipeline.features import compute_features

logging.basicConfig(level=logging.ERROR)

def run():
    import glob
    import os
    
    cache_dir = os.path.expanduser("~/.quant_value_vn/cache")
    data = []
    try:
        for f in glob.glob(f"{cache_dir}/*.json"):
            with open(f) as fh:
                data.append(json.load(fh))
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)
        
    if not data:
        print("No cached data found.")
        sys.exit(0)
        
    df = pd.DataFrame(data)
    
    # Calculate features
    feat_df = compute_features(df)
    
    metrics = ['ROA_5yr_avg', 'ROC_5yr_avg', 'FCF_assets_5yr_avg', 'GM_stability', 'enterprise_value', 'invested_capital', 'working_capital', 'cfo_to_assets', 'roic']
    
    print("----- NaN Counts -----")
    for m in metrics:
        if m in feat_df.columns:
            nan_cnt = feat_df[m].isna().sum()
            print(f"{m}: {nan_cnt} missing out of {len(feat_df)}")
        else:
            print(f"{m}: Column entirely missing")
            
    print("\n----- Missing Fields from Ingestion -----")
    missing = []
    for col in ['total_assets', 'net_income_parent', 'revenue', 'gross_profit', 'net_working_capital', 'ppe', 'interest_expense', 'operating_profit', 'pretax_profit', 'current_assets', 'current_liabilities', 'operating_cash_flow', 'capex']:
        for year in range(5):
            suffix = "" if year == 0 else f"_y{year}"
            col_yr = f"{col}{suffix}"
            if col_yr not in df.columns:
                missing.append(col_yr)
            else:
                missing_cnt = df[col_yr].isna().sum()
                if missing_cnt > 0:
                    print(f"{col_yr}: {missing_cnt} missing out of {len(df)}")
    
    print(f"\nTotal rows processed: {len(feat_df)}")
    
if __name__ == '__main__':
    run()
