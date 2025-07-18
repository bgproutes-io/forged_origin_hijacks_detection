import pandas as pd
import requests
import lzma
from io import BytesIO

def collect_roas_snapshot(year, month, day, outfile):
    all_dfs = []
    for rir in ["afrinic", "apnic", "arin", "lacnic", "ripencc"]:
        tmp_url = f"https://ftp.ripe.net/rpki/{rir}.tal/{year}/{month}/{day}/roas.csv.xz"
        try:
            response = requests.get(tmp_url)
            response.raise_for_status()  # Raise an error for bad responses
            with lzma.open(BytesIO(response.content)) as f:
                df = pd.read_csv(f, dtype=str)
                df.drop(columns=['URI', "Not Before", "Not After"], inplace=True) # Columns not needed
                df["ASN"] = df["ASN"].str.replace("AS", "", regex=False) # Remove 'AS' prefix
                df["Max Length"] = df["Max Length"].astype(int)
                all_dfs.append(df)
        except requests.RequestException as e:
            print(f"Error fetching data from {tmp_url}: {e}")
            continue

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Remove duplicates based on ASN and Prefix, keeping the one with the longest Max Length
    combined_df = combined_df.sort_values("Max Length", ascending=False)
    combined_df = combined_df.drop_duplicates(subset=["ASN", "IP Prefix"], keep="first")

    combined_df.to_csv(outfile, index=False, sep=" ")