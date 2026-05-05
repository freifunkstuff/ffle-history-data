#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path


DEFAULT_CSV = Path("firmware_dd.csv")
DEFAULT_ENDPOINT = "https://grafana.freifunk-dresden.de/api/datasources/proxy/uid/000000002/query"
DEFAULT_DATABASE = "freifunk"
DEFAULT_COMMUNITY = "Leipzig"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append new daily DD node counts from Grafana to firmware_dd.csv."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--community", default=DEFAULT_COMMUNITY)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def read_existing_rows(csv_path: Path) -> list[tuple[str, int]]:
    if not csv_path.exists():
        raise ValueError(f"CSV file not found: {csv_path}")

    rows: list[tuple[str, int]] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != ["time", "count"]:
            raise ValueError(f"Unexpected CSV header in {csv_path}: {header!r}")
        for raw_row in reader:
            if not raw_row:
                continue
            if len(raw_row) != 2:
                raise ValueError(f"Unexpected CSV row in {csv_path}: {raw_row!r}")
            timestamp, count = raw_row
            rows.append((timestamp, int(count)))

    if not rows:
        raise ValueError(f"CSV file has no data rows: {csv_path}")

    return rows


def build_query(since_timestamp: str, community: str) -> str:
    safe_community = community.replace("\\", "\\\\").replace("'", "\\'")
    return (
        'SELECT max("value") FROM "nodes_communities" '
        f"WHERE \"community\" = '{safe_community}' AND time >= '{since_timestamp}' "
        "GROUP BY time(1d)"
    )


def fetch_rows(endpoint: str, database: str, query: str, timeout: int) -> list[tuple[str, int]]:
    params = urllib.parse.urlencode({"db": database, "epoch": "s", "q": query})
    url = f"{endpoint}?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": "ffle-history-data/1.0"})

    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)

    return parse_influx_payload(payload)


def parse_influx_payload(payload: object) -> list[tuple[str, int]]:
    if not isinstance(payload, dict):
        raise ValueError("Grafana response is not a JSON object")

    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("Grafana response does not contain a results list")

    rows_by_timestamp: dict[str, int] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        series_list = result.get("series") or []
        if not isinstance(series_list, list):
            continue
        for series in series_list:
            if not isinstance(series, dict):
                continue
            values = series.get("values") or []
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, list) or len(item) < 2:
                    continue
                raw_timestamp, raw_count = item[0], item[1]
                if raw_count is None:
                    continue
                timestamp = epoch_seconds_to_iso(raw_timestamp)
                rows_by_timestamp[timestamp] = normalize_count(raw_count)

    return sorted(rows_by_timestamp.items(), key=lambda row: row[0])


def epoch_seconds_to_iso(raw_timestamp: object) -> str:
    if not isinstance(raw_timestamp, (int, float)):
        raise ValueError(f"Unexpected timestamp value: {raw_timestamp!r}")
    timestamp = dt.datetime.fromtimestamp(raw_timestamp, tz=dt.timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_count(raw_count: object) -> int:
    if isinstance(raw_count, bool) or not isinstance(raw_count, (int, float)):
        raise ValueError(f"Unexpected count value: {raw_count!r}")
    return int(raw_count)


def select_new_rows(
    existing_rows: list[tuple[str, int]], fetched_rows: list[tuple[str, int]]
) -> list[tuple[str, int]]:
    last_timestamp = existing_rows[-1][0]
    fetched_timestamps = {timestamp for timestamp, _ in fetched_rows}

    # Refuse to append if the datasource cannot reproduce the current tail row.
    if last_timestamp not in fetched_timestamps:
        raise ValueError(
            f"Datasource did not return overlap for last CSV timestamp {last_timestamp}"
        )

    return [row for row in fetched_rows if row[0] > last_timestamp]


def append_rows(csv_path: Path, rows: list[tuple[str, int]]) -> None:
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerows(rows)


def main() -> int:
    args = parse_args()

    try:
        existing_rows = read_existing_rows(args.csv)
        last_timestamp = existing_rows[-1][0]
        query = build_query(last_timestamp, args.community)
        fetched_rows = fetch_rows(args.endpoint, args.database, query, args.timeout)
        new_rows = select_new_rows(existing_rows, fetched_rows)
    except Exception as exc:
        print(f"Update failed: {exc}", file=sys.stderr)
        return 1

    if not new_rows:
        print(f"No new rows after {last_timestamp}.")
        return 0

    if args.dry_run:
        print(
            f"Would append {len(new_rows)} rows from {new_rows[0][0]} through {new_rows[-1][0]}."
        )
        return 0

    append_rows(args.csv, new_rows)
    print(f"Appended {len(new_rows)} rows through {new_rows[-1][0]}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())