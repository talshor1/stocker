from __future__ import annotations

import logging
import os
from typing import Any
from google.genai import Client
import pandas as pd

from models.task import Task

logger = logging.getLogger(__name__)

def load_stock_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df

def df_to_summary_text(df: pd.DataFrame) -> str:
    last_rows = df.tail(100).to_dict(orient="records")

    summary = f"""
The file contains stock historical data with {len(df)} rows.
Columns: {', '.join(df.columns)}

Here are the last 100 rows of data:
{last_rows}

Now analyze the stock performance, volatility, trend direction,
volume strength, and produce a clear summary.
"""
    return summary


def run_prompt(cfg: Any, task: Task):
    logger.info("Creating prompt for Gemini AI")
    client = Client(api_key="AIzaSyD969fP535GiAEDFW5JcVxbhFy4mqR-iZI")
    # Load CSV data
    df = pd.read_csv(task.file_name)
    # Convert into text the model can understand
    prompt = df_to_summary_text(df)
    # Ask Gemini to analyze the stock
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    logger.info("Got response from Gemini AI: %s", response.text)

    csv_path = task.file_name
    directory = os.path.dirname(csv_path)
    output_path = os.path.join(directory, "ai_response.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(response.text)
