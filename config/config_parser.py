from __future__ import annotations
import argparse

def parse_args():
    p = argparse.ArgumentParser(description="Fetch intraday data and/or ingest CSV to Mongo.")
    p.add_argument("--config", default="config.ini", help="Path to config.ini")
    p.add_argument("--symbol", help="Override symbol (default from config)")
    p.add_argument("--days", type=int, help="Override days (default from config)")
    p.add_argument("--function", help="Override AV function (unused for intraday)")
    p.add_argument("--outfile", default="out.csv", help="Output CSV path (default: out.csv)")
    return p.parse_args()
