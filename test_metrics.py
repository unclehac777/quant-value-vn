import sys
import pandas as pd
import asyncio
import logging

from quant_value_vn.pipeline.ingest import ingest_all
from quant_value_vn.pipeline.features import compute_features

logging.basicConfig(level=logging.INFO)

def main():
    tickers = ["FPT", "HPG", "VNM"]
    print(f"Scraping {tickers}...")
    
    # Run the ingestion script without cache so it hits CafeF
    raw_data, failed = ingest_all(tickers, use_cache=False)
    
    if failed:
        print(f"Failed to scrape: {failed}")
    
    if not raw_data:
        print("No data retrieved.")
        return

    df = pd.DataFrame(raw_data)
    print("\n[Raw Columns]:")
    print(sorted(df.columns))

    print("\n[Computing Features...]")
    df_feat = compute_features(df)
    
    print("\n[Quality Metrics (5-year)]:")
    cols = ["ticker", "ROA_5yr_avg", "ROC_5yr_avg", "FCF_assets_5yr_avg", "GM_stability"]
    print(df_feat[cols].to_string())

if __name__ == "__main__":
    main()
