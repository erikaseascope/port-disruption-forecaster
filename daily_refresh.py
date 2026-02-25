#!/usr/bin/env python3
"""
Daily refresh script for Port & Logistics Disruption Forecaster API.

Fetches:
- Yesterday's GDELT events CSV (global events, protests, strikes)
- MarineTraffic RSS feeds for port congestion / anomalies
- (Stub) ACLED conflict data

Inserts into ClickHouse table: port_disruption_signals

Run daily via cron, GitHub Actions, or Render Cron Job.
"""

import datetime
import requests
import zipfile
import io
import logging
import pandas as pd
from clickhouse_driver import Client
from feedparser import parse as parse_rss
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

# ── Setup ──────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "disruption")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"

# ── ClickHouse client ──────────────────────────────────────────────────────────
def get_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        secure=CLICKHOUSE_SECURE,
        settings={'use_numpy': True}
    )

# ── Ensure table exists ────────────────────────────────────────────────────────
def create_table(client):
    client.execute("""
    CREATE TABLE IF NOT EXISTS port_disruption_signals (
        signal_id UInt64 DEFAULT generateUUIDv4(),
        source String,
        port_name String,
        country String,
        event_type String,
        description String,
        event_date DateTime,
        impact_score Float32,
        raw_data String,          -- JSON or full text for debugging
        ingested_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (ingested_at, source, port_name)
    """)
    logging.info("Table 'port_disruption_signals' ready.")

# ── GDELT daily fetch & parse ─────────────────────────────────────────────────
def fetch_gdelt_yesterday() -> List[Dict[str, Any]]:
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    url = f"http://data.gdeltproject.org/events/{yesterday}.export.CSV.zip"
    
    logging.info(f"Fetching GDELT file: {url}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, sep='\t', header=None, low_memory=False,
                                 usecols=[0, 1, 5, 26, 27, 30, 34, 57],  # GLOBALEVENTID, SQLDATE, Actor1CountryCode, EventRootCode, GoldsteinScale, Actor1Geo_Fullname, EventBaseCode, SOURCEURL
                                 names=["id", "sqldate", "country", "event_root", "goldstein", "location", "event_base", "source_url"])
                
                # Filter relevant events (protests, strikes, blockades near ports)
                df = df[df['event_root'].isin(['14', '18'])]  # 14=Protest, 18=Disruption
                
                records = df.to_dict(orient="records")
                logging.info(f"GDELT: {len(records)} relevant events loaded")
                return records
    except Exception as e:
        logging.error(f"GDELT fetch failed: {e}")
        return []

# ── MarineTraffic RSS parser ──────────────────────────────────────────────────
def parse_marinetraffic_rss() -> List[Dict[str, Any]]:
    # MarineTraffic RSS examples (replace with your subscribed feed URLs)
    # You need a free/paid MarineTraffic account to get personalized RSS for ports
    # Example public feeds (limited): https://www.marinetraffic.com/en/ais/home/rss
    feeds = [
        "https://www.marinetraffic.com/ais/index/rss?port=SHANGHAI",  # placeholder - get real from account
        "https://www.marinetraffic.com/ais/index/rss?port=LOSANGELES"
        # Add more ports you care about
    ]

    signals = []
    for url in feeds:
        try:
            feed = parse_rss(url)
            for entry in feed.entries[:20]:  # last 20 items
                title = entry.title.lower()
                summary = (entry.get("summary") or "").lower()
                
                # Simple keyword detection for congestion / disruption
                if any(k in title or k in summary for k in ["congestion", "delay", "waiting", "anchorage", "queue", "strike", "protest", "blockade"]):
                    port_name = "Unknown"  # parse from feed title or URL
                    if "shanghai" in url.lower():
                        port_name = "Shanghai"
                    elif "los angeles" in url.lower():
                        port_name = "Los Angeles"
                    # Add more port detection
                    
                    signals.append({
                        "source": "MarineTraffic",
                        "port_name": port_name,
                        "country": "Unknown",  # resolve via lookup table later
                        "event_type": "Congestion" if "congestion" in title else "Disruption",
                        "description": entry.title + " - " + (entry.get("summary") or ""),
                        "event_date": datetime.datetime.now().isoformat(),
                        "impact_score": 25.0 if "congestion" in title else 15.0,
                        "raw_data": entry.link
                    })
            logging.info(f"MarineTraffic ({url}): {len(signals)} signals extracted")
        except Exception as e:
            logging.error(f"MarineTraffic RSS failed ({url}): {e}")
    
    return signals

# ── Main refresh function ─────────────────────────────────────────────────────
def daily_refresh():
    client = get_client()
    create_table(client)
    
    # 1. GDELT
    gdelt_records = fetch_gdelt_yesterday()
    if gdelt_records:
        # Map to our schema
        mapped = []
        for r in gdelt_records:
            mapped.append({
                "source": "GDELT",
                "port_name": r.get("location", "Unknown"),
                "country": r.get("country", "Unknown"),
                "event_type": r.get("event_root", "Unknown"),
                "description": f"Event ID {r.get('id')}, Goldstein {r.get('goldstein')}",
                "event_date": str(r.get("sqldate", "")),
                "impact_score": abs(r.get("goldstein", 0)) * 5,  # scale -10 to +10 → 0-50
                "raw_data": json.dumps(r)
            })
        client.insert_dataframe("INSERT INTO port_disruption_signals VALUES", pd.DataFrame(mapped))
        logging.info(f"Inserted {len(mapped)} GDELT records")

    # 2. MarineTraffic RSS
    mt_signals = parse_marinetraffic_rss()
    if mt_signals:
        client.insert_dataframe("INSERT INTO port_disruption_signals VALUES", pd.DataFrame(mt_signals))
        logging.info(f"Inserted {len(mt_signals)} MarineTraffic signals")

    # 3. ACLED stub (extend with real API or CSV upload)
    logging.info("ACLED ingestion stub - implement with API key or manual CSV")

    logging.info("Daily refresh complete.")

if __name__ == "__main__":
    daily_refresh()
