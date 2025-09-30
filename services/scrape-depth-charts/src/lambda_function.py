"""AWS Lambda entrypoint for scraping NFL depth charts and syncing them to Supabase."""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional

import requests

DEPTH_CHART_PAGE = "https://cdn.espn.com/core/nfl/team/depth/_/name/{team_code}?xhr=1"
NFL_TEAM_CODES = [
    "ari",
    "atl",
    "bal",
    "buf",
    "car",
    "chi",
    "cin",
    "cle",
    "dal",
    "den",
    "det",
    "gb",
    "hou",
    "ind",
    "jac",
    "kc",
    "lv",
    "lac",
    "lar",
    "mia",
    "min",
    "ne",
    "no",
    "nyg",
    "nyj",
    "phi",
    "pit",
    "sf",
    "sea",
    "tb",
    "ten",
    "wsh",
]

TEAM_NAME_FALLBACKS = {
    "ari": "Arizona Cardinals",
    "atl": "Atlanta Falcons",
    "bal": "Baltimore Ravens",
    "buf": "Buffalo Bills",
    "car": "Carolina Panthers",
    "chi": "Chicago Bears",
    "cin": "Cincinnati Bengals",
    "cle": "Cleveland Browns",
    "dal": "Dallas Cowboys",
    "den": "Denver Broncos",
    "det": "Detroit Lions",
    "gb": "Green Bay Packers",
    "hou": "Houston Texans",
    "ind": "Indianapolis Colts",
    "jac": "Jacksonville Jaguars",
    "kc": "Kansas City Chiefs",
    "lv": "Las Vegas Raiders",
    "lac": "Los Angeles Chargers",
    "lar": "Los Angeles Rams",
    "mia": "Miami Dolphins",
    "min": "Minnesota Vikings",
    "ne": "New England Patriots",
    "no": "New Orleans Saints",
    "nyg": "New York Giants",
    "nyj": "New York Jets",
    "phi": "Philadelphia Eagles",
    "pit": "Pittsburgh Steelers",
    "sf": "San Francisco 49ers",
    "sea": "Seattle Seahawks",
    "tb": "Tampa Bay Buccaneers",
    "ten": "Tennessee Titans",
    "wsh": "Washington Commanders",
}

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DEPTH_CHARTS_TABLE = os.environ.get("DEPTH_CHARTS_TABLE", "DepthCharts")
KEY_COLUMN = os.environ.get("DEPTH_CHART_KEY_COL", "key")
BATCH_SIZE = int(os.environ.get("DEPTH_CHART_BATCH_SIZE", "50"))

USER_AGENT = os.environ.get(
    "DEPTH_CHART_USER_AGENT",
    "Mozilla/5.0 (compatible; BettingAntelopeDepthChartBot/1.0; +https://example.com/bot)",
)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": USER_AGENT})


def _http_get_json(url: str) -> Dict[str, Any]:
    """Fetch JSON with a custom user-agent and retry handling."""

    for attempt in range(3):
        response = _SESSION.get(url, timeout=30)
        if response.status_code in RETRYABLE_STATUS_CODES and attempt < 2:
            time.sleep(2**attempt)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"Failed to fetch {url}")


def _supabase_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def _http_upsert_rows(table: str, rows: Iterable[Dict[str, Any]]) -> None:
    """Send a batched upsert request to Supabase."""

    rows = list(rows)
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = _SESSION.post(url, json=rows, headers=_supabase_headers(), timeout=30)
    response.raise_for_status()


def _batched(iterable: Iterable[Dict[str, Any]], batch_size: int) -> Iterator[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _walk_json_tree(root: Any) -> Iterator[Any]:
    """Depth-first traversal of a JSON-like structure."""

    stack = [root]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (dict, list)):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
        yield current
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


def _coalesce(*values: Optional[Any]) -> Optional[Any]:
    for value in values:
        if isinstance(value, str):
            value = value.strip()
        if value not in (None, "", []):
            return value
    return None


def _strip_uid(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.split(":")[-1]


def _safe_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_status_text(status: Any) -> Optional[str]:
    if not status:
        return None
    if isinstance(status, str):
        return status
    if isinstance(status, dict):
        for key in ("description", "detail", "text", "type", "displayName", "shortDisplayName"):
            value = status.get(key)
            if isinstance(value, dict):
                nested = _extract_status_text(value)
                if nested:
                    return nested
            elif value:
                return str(value)
        for value in status.values():
            nested = _extract_status_text(value)
            if nested:
                return nested
    if isinstance(status, list):
        for item in status:
            nested = _extract_status_text(item)
            if nested:
                return nested
    return None


def _find_team_info(team_code: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Locate the team metadata for the provided abbreviation."""

    target_abbr = team_code.upper()
    for node in _walk_json_tree(payload):
        if not isinstance(node, dict):
            continue
        abbr = str(node.get("abbreviation") or "").upper()
        if abbr != target_abbr:
            continue
        uid = node.get("uid") or ""
        if "team" not in uid and node.get("type") not in {"team", "CollegeTeam"} and "id" not in node:
            # Avoid matching other data (positions, statuses, etc.).
            continue
        if any(field in node for field in ("displayName", "name", "shortDisplayName")):
            return node
    return {"abbreviation": target_abbr}


def _find_first_timestamp(payload: Dict[str, Any]) -> Optional[str]:
    for node in _walk_json_tree(payload):
        if not isinstance(node, dict):
            continue
        for key in ("lastUpdated", "lastModified", "lastUpdateTime"):
            value = node.get(key)
            if value:
                return str(value)
    return None


def _iter_depth_nodes(payload: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    for node in _walk_json_tree(payload):
        if not isinstance(node, dict):
            continue
        athletes = node.get("athletes")
        position = node.get("position")
        if not isinstance(athletes, list) or not athletes:
            continue
        if not position and not node.get("unit") and not node.get("displayName"):
            continue
        if not any(isinstance(athlete, dict) and (athlete.get("athlete") or athlete.get("player")) for athlete in athletes):
            continue
        yield node


def _collect_team_rows(team_code: str, now_iso: str) -> List[Dict[str, Any]]:
    payload = _http_get_json(DEPTH_CHART_PAGE.format(team_code=team_code))
    team_info = _find_team_info(team_code, payload)
    team_id = str(
        _coalesce(
            team_info.get("id"),
            _strip_uid(team_info.get("uid")),
            team_info.get("slug"),
            team_info.get("abbreviation"),
            team_code.upper(),
        )
    )
    team_name = str(
        _coalesce(
            team_info.get("displayName"),
            team_info.get("name"),
            team_info.get("shortDisplayName"),
            TEAM_NAME_FALLBACKS.get(team_code),
            team_code.upper(),
        )
    )
    last_updated = _coalesce(payload.get("lastUpdated"), _find_first_timestamp(payload)) or now_iso

    rows: List[Dict[str, Any]] = []
    for node in _iter_depth_nodes(payload):
        position_info = node.get("position") or {}
        position_abbr = _coalesce(
            position_info.get("abbreviation"),
            position_info.get("displayName"),
            position_info.get("name"),
            node.get("displayName"),
        )
        position_name = _coalesce(
            position_info.get("name"),
            position_info.get("displayName"),
            node.get("displayName"),
        )
        unit_field = node.get("unit")
        unit_name: Optional[str]
        if isinstance(unit_field, dict):
            unit_name = _coalesce(
                unit_field.get("displayName"),
                unit_field.get("name"),
                unit_field.get("abbreviation"),
            )
        else:
            unit_name = unit_field if isinstance(unit_field, str) else None

        position_key = str(position_abbr or position_name or "UNKNOWN")

        for athlete_entry in node.get("athletes", []):
            if not isinstance(athlete_entry, dict):
                continue
            athlete = athlete_entry.get("athlete") or athlete_entry.get("player") or athlete_entry.get("member")
            if not isinstance(athlete, dict):
                continue
            athlete_id = _coalesce(
                athlete.get("id"),
                _strip_uid(athlete.get("uid")),
            )
            if not athlete_id:
                continue

            depth = _safe_int(athlete_entry.get("depth"))
            order = _safe_int(athlete_entry.get("order"))
            if depth is None and isinstance(athlete_entry.get("slot"), dict):
                depth = _safe_int(
                    _coalesce(
                        athlete_entry["slot"].get("position"),
                        athlete_entry["slot"].get("sequence"),
                    )
                )
            if order is None:
                order = depth

            status_text = _extract_status_text(athlete_entry.get("status") or athlete.get("status"))
            injuries = athlete.get("injuries")
            is_injured = bool(athlete_entry.get("injured"))
            if not is_injured and isinstance(injuries, list):
                is_injured = any(bool(injury) for injury in injuries)

            row = {
                KEY_COLUMN: f"{team_id}:{position_key}:{athlete_id}",
                "team_id": team_id,
                "team_name": team_name,
                "position": str(position_abbr or position_name or "UNKNOWN"),
                "position_name": str(position_name or position_abbr or "Unknown"),
                "unit": unit_name,
                "athlete_id": str(athlete_id),
                "athlete_name": _coalesce(
                    athlete.get("displayName"),
                    athlete.get("fullName"),
                    athlete.get("shortName"),
                    athlete.get("name"),
                ),
                "athlete_number": _coalesce(
                    athlete.get("jersey"),
                    athlete.get("uniformNumber"),
                    athlete_entry.get("jersey"),
                ),
                "status": status_text,
                "depth_order": depth if depth is not None else order,
                "is_injured": bool(is_injured),
                "last_updated": last_updated,
                "synced_at": now_iso,
            }
            rows.append(row)
    return rows


def _iter_depth_chart_rows(now_iso: str) -> Iterator[Dict[str, Any]]:
    failures: List[str] = []
    for team_code in NFL_TEAM_CODES:
        try:
            yield from _collect_team_rows(team_code, now_iso)
        except Exception as exc:  # pragma: no cover - network/transient errors logged
            failures.append(f"{team_code}:{exc}")
    if failures:
        print("Depth chart scrape completed with failures:", "; ".join(failures))


def lambda_handler(event, context):  # pragma: no cover - entry point for AWS Lambda
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = list(_iter_depth_chart_rows(now_iso))
    for batch in _batched(rows, BATCH_SIZE):
        _http_upsert_rows(DEPTH_CHARTS_TABLE, batch)
    return {"rows_processed": len(rows), "table": DEPTH_CHARTS_TABLE}
