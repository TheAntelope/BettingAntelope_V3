"""AWS Lambda entrypoint for scraping NFL depth charts and syncing them to Supabase."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List
from urllib import error, request

ESPN_TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
ESPN_DEPTH_CHART_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/depthchart"

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DEPTH_CHARTS_TABLE = os.environ.get("DEPTH_CHARTS_TABLE", "DepthCharts")
KEY_COLUMN = os.environ.get("DEPTH_CHART_KEY_COL", "key")
BATCH_SIZE = int(os.environ.get("DEPTH_CHART_BATCH_SIZE", "50"))

USER_AGENT = os.environ.get(
    "DEPTH_CHART_USER_AGENT",
    "Mozilla/5.0 (compatible; BettingAntelopeDepthChartBot/1.0; +https://example.com/bot)",
)


def _http_get_json(url: str) -> Dict:
    """Fetch JSON with a custom user-agent and retry handling."""
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(3):
        try:
            req = request.Request(url, headers=headers)
            with request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except error.HTTPError as exc:  # pragma: no cover - network errors handled at runtime
            if exc.code in {429, 500, 502, 503, 504} and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise
        except error.URLError:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(f"Failed to fetch {url}")


def _supabase_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def _http_upsert_rows(table: str, rows: Iterable[Dict]) -> None:
    """Send a batched upsert request to Supabase."""
    rows = list(rows)
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    payload = json.dumps(rows).encode("utf-8")
    req = request.Request(url, data=payload, headers=_supabase_headers(), method="POST")
    with request.urlopen(req, timeout=30) as resp:
        # Consume body so that errors raise.
        resp.read()


def _batched(iterable: Iterable[Dict], batch_size: int) -> Iterable[List[Dict]]:
    batch: List[Dict] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_depth_chart_rows() -> Iterable[Dict]:
    now_iso = datetime.now(timezone.utc).isoformat()
    teams_blob = _http_get_json(ESPN_TEAMS_URL)
    sports = teams_blob.get("sports", [])
    leagues = sports[0].get("leagues", []) if sports else []
    teams = []
    for league in leagues:
        for team_entry in league.get("teams", []):
            team = team_entry.get("team") or team_entry
            if team:
                teams.append(team)
    for team in teams:
        team_id = team.get("id")
        if not team_id:
            continue
        depth_blob = _http_get_json(ESPN_DEPTH_CHART_URL.format(team_id=team_id))
        depth_items = depth_blob.get("items", [])
        team_info = depth_blob.get("team", team)
        team_name = team_info.get("displayName") or team_info.get("name")
        last_updated = depth_blob.get("lastUpdated") or now_iso
        for item in depth_items:
            position = item.get("position", {})
            position_abbr = position.get("abbreviation") or position.get("name")
            position_name = position.get("name")
            unit_type = item.get("unit", {}).get("displayName")
            for athlete_entry in item.get("athletes", []):
                athlete = athlete_entry.get("athlete", {})
                athlete_id = athlete.get("id")
                if not athlete_id:
                    continue
                order = athlete_entry.get("order")
                depth = athlete_entry.get("depth", order)
                status = athlete_entry.get("status", {})
                status_desc = status.get("description") or status.get("type")
                is_injured = bool(athlete.get("injuries")) or athlete_entry.get("injured")
                row = {
                    KEY_COLUMN: f"{team_id}:{position_abbr}:{athlete_id}",
                    "team_id": team_id,
                    "team_name": team_name,
                    "position": position_abbr,
                    "position_name": position_name,
                    "unit": unit_type,
                    "athlete_id": athlete_id,
                    "athlete_name": athlete.get("displayName") or athlete.get("fullName"),
                    "athlete_number": athlete.get("jersey"),
                    "status": status_desc,
                    "depth_order": depth if depth is not None else order,
                    "is_injured": is_injured,
                    "last_updated": last_updated,
                    "synced_at": now_iso,
                }
                yield row


def lambda_handler(event, context):  # pragma: no cover - entry point for AWS Lambda
    rows = list(_iter_depth_chart_rows())
    for batch in _batched(rows, BATCH_SIZE):
        _http_upsert_rows(DEPTH_CHARTS_TABLE, batch)
    return {"rows_processed": len(rows), "table": DEPTH_CHARTS_TABLE}
