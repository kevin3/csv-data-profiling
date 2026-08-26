#!/usr/bin/env python3
"""
AWS Data Profiling Pipeline — Python Core Engine
Version: 1.1
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
from pathlib import Path

import duckdb
import yaml


def setup_logging(config: dict) -> logging.Logger:
    log_dir = config.get("paths", {}).get("log_dir", "./logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "python_engine.log")

    logger = logging.getLogger("awdp_engine")
    logger.setLevel(getattr(logging, config.get("logging", {}).get("level", "INFO")))

    if not logger.handlers:
        fh = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=config.get("logging", {}).get("max_bytes", 10_485_760),
            backupCount=config.get("logging", {}).get("backup_count", 5),
        )
        ch = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def compute_sha256(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def profile_full(csv_path: str, output_dir: str, config: dict, logger: logging.Logger):
    logger.info(f"Running full scan on {csv_path}")
    conn = duckdb.connect(":memory:")
    threads = config.get("processing", {}).get("duckdb_threads", 4)
    conn.execute(f"SET threads={threads}")

    # Use rejects_table for malformed rows
    rejects_table = "csv_rejects"
    summarize_query = f"""
        SELECT * FROM read_csv_auto('{csv_path}',
            ignore_errors=true,
            rejects_table='{rejects_table}'
        )
    """

    # Run SUMMARIZE
    logger.info("Executing DuckDB SUMMARIZE...")
    result = conn.execute(f"SUMMARIZE {summarize_query}").fetchdf()

    # Save summary CSV
    summary_csv = os.path.join(output_dir, "summary.csv")
    result.to_csv(summary_csv, index=False)
    logger.info(f"Summary CSV saved: {summary_csv}")

    # Save summary JSON
    summary_json = os.path.join(output_dir, "summary.json")
    with open(summary_json, "w") as f:
        json.dump(result.to_dict(orient="records"), f, indent=2, default=str)
    logger.info(f"Summary JSON saved: {summary_json}")

    # Check for rejected rows
    try:
        rejects = conn.execute(f"SELECT * FROM {rejects_table}").fetchdf()
        if len(rejects) > 0:
            rejects_csv = os.path.join(output_dir, "rejected.csv")
            rejects.to_csv(rejects_csv, index=False)
            logger.warning(f"Found {len(rejects)} rejected rows. Saved to {rejects_csv}")
    except duckdb.CatalogException:
        logger.info("No rejected rows detected.")

    conn.close()


def profile_chunk(csv_path: str, output_dir: str, config: dict, logger: logging.Logger):
    """Profile a single chunk and save its JSON."""
    logger.info(f"Profiling chunk: {csv_path}")
    conn = duckdb.connect(":memory:")
    threads = config.get("processing", {}).get("duckdb_threads", 4)
    conn.execute(f"SET threads={threads}")

    chunk_name = Path(csv_path).stem
    chunk_output = os.path.join(output_dir, "chunks")
    os.makedirs(chunk_output, exist_ok=True)

    result = conn.execute(f"""
        SUMMARIZE SELECT * FROM read_csv_auto('{csv_path}', ignore_errors=true)
    """).fetchdf()

    chunk_json = os.path.join(chunk_output, f"{chunk_name}.json")
    with open(chunk_json, "w") as f:
        json.dump(result.to_dict(orient="records"), f, indent=2, default=str)

    logger.info(f"Chunk summary saved: {chunk_json}")
    conn.close()


def aggregate_chunks(output_dir: str, logger: logging.Logger):
    """Aggregate all chunk JSONs into a single summary."""
    chunk_dir = os.path.join(output_dir, "chunks")
    if not os.path.exists(chunk_dir):
        logger.error("No chunk directory found for aggregation.")
        return

    all_records = []
    for json_file in sorted(Path(chunk_dir).glob("*.json")):
        with open(json_file, "r") as f:
            records = json.load(f)
            all_records.extend(records)

    # Simple aggregation: group by column name, compute min/max/avg of stats
    from collections import defaultdict
    agg = defaultdict(lambda: {
        "count": 0,
        "min_vals": [],
        "max_vals": [],
        "avg_vals": [],
        "null_counts": [],
    })

    for rec in all_records:
        col = rec.get("column_name") or rec.get("column")
        if not col:
            continue
        agg[col]["count"] += 1
        agg[col]["min_vals"].append(rec.get("min"))
        agg[col]["max_vals"].append(rec.get("max"))
        agg[col]["null_counts"].append(rec.get("null_percentage", 0))

    # Build final aggregated records
    final_records = []
    for col, data in agg.items():
        final_records.append({
            "column": col,
            "chunks_processed": data["count"],
            "min_across_chunks": min(v for v in data["min_vals"] if v is not None) if any(v is not None for v in data["min_vals"]) else None,
            "max_across_chunks": max(v for v in data["max_vals"] if v is not None) if any(v is not None for v in data["max_vals"]) else None,
            "avg_null_percentage": sum(data["null_counts"]) / len(data["null_counts"]) if data["null_counts"] else 0,
        })

    # Save aggregated JSON
    agg_json = os.path.join(output_dir, "summary.json")
    with open(agg_json, "w") as f:
        json.dump(final_records, f, indent=2, default=str)

    # Save aggregated CSV
    import pandas as pd
    df = pd.DataFrame(final_records)
    agg_csv = os.path.join(output_dir, "summary.csv")
    df.to_csv(agg_csv, index=False)

    logger.info(f"Aggregated summary saved to {agg_json} and {agg_csv}")


def main():
    parser = argparse.ArgumentParser(description="AWDPP Python Core Engine")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--input", help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--mode", choices=["full", "chunk", "aggregate"], required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config)

    if args.mode == "full":
        if not args.input:
            logger.error("--input required for full mode")
            sys.exit(1)
        profile_full(args.input, args.output, config, logger)

    elif args.mode == "chunk":
        if not args.input:
            logger.error("--input required for chunk mode")
            sys.exit(1)
        profile_chunk(args.input, args.output, config, logger)

    elif args.mode == "aggregate":
        aggregate_chunks(args.output, logger)


if __name__ == "__main__":
    main()