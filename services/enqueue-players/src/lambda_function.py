import os
import json
import boto3
from datetime import datetime, timezone, timedelta
from datetime import datetime as dt
from itertools import islice
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
QUEUE_URL = os.environ["PLAYER_QUEUE_URL"]

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
sqs = boto3.client("sqs")

POSITION_COLS = [
    "qb1",
    "rb1", "rb2",
    "wr1", "wr2", "wr3", "wr4", "wr5", "wr6",
    "te1",
    "de1", "de2", "de3", "de4", "de5", "de6",
    "lb1", "lb2", "lb3", "lb4", "lb5", "lb6", "lb7", "lb8", "lb9", "lb10",
    "cb1", "cb2", "cb3", "cb4",
    "ss1", "ss2", "ss3",
]

TABLE = "DepthCharts"
PLAYER_METADATA_TABLE = "PlayerMetaData"
RECENT_UPDATE_WINDOW_DAYS = 3

# ---------- Helpers ----------

def _coerce_to_name_status(val):
    if val is None:
        return (None, None)
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            val = parsed
        except Exception:
            return (val.strip() or None, None)
    if isinstance(val, list):
        if not val:
            return (None, None)
        name = (val[0] or "").strip()
        status = (val[1] or "").strip() if len(val) > 1 else None
        return (name or None, status or None)
    if isinstance(val, dict):
        return (
            (val.get("name") or "").strip() or None,
            (val.get("status") or "").strip() or None,
        )
    return (str(val).strip() or None, None)

def _chunks(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch

def _distinct_teams():
    res = sb.table(TABLE).select("team").execute()
    return sorted({r["team"] for r in (res.data or []) if r.get("team")})



def _latest_row_for_team(team: str):
    q = (
        sb.table(TABLE)
        .select("*")
        .eq("team", team)
        .order("last_updated", desc=True)
        .limit(1)
    )
    res = q.execute()
    rows = res.data or []
    return rows[0] if rows else None

def _parse_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None

def _player_recently_updated(name, team, position, cutoff_ts, cache):
    key = (name, team, position)
    if key in cache:
        return cache[key]

    query = (
        sb.table(PLAYER_METADATA_TABLE)
        .select("*")
        .eq("Player Name", name)
        .order("updated_at", desc=True)
        .limit(5)
    )
    res = query.execute()
    rows = res.data or []

    normalized_team = (team or "").strip().lower()
    normalized_position = (position or "").strip().upper()

    target_row = None
    for row in rows:
        row_team = (row.get("Team") or "").strip().lower()
        row_position = (row.get("ESPN Roster Position") or "").strip().upper()
        if row_team == normalized_team and row_position == normalized_position:
            target_row = row
            break
    if not target_row and rows:
        target_row = rows[0]

    updated_dt = _parse_timestamp(target_row.get("updated_at")) if target_row else None
    is_recent = bool(updated_dt and updated_dt >= cutoff_ts)
    cache[key] = is_recent
    return is_recent

def get_nfl_season(today=None):
    """
    Returns the NFL season year as an integer for a given date.
    If no date is provided, uses today's date.
    NFL seasons typically start in September and end in February.
    """
    if today is None:
        today = dt.today()
    year = today.year
    # If before September, it's still the previous season
    if today.month < 9:
        return year - 1
    return year

# ---------- Lambda Entrypoint ----------

def lambda_handler(event, context):
    currentSeason = get_nfl_season()
    run_id = datetime.now(timezone.utc).isoformat()
    messages = []
    recent_cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_UPDATE_WINDOW_DAYS)
    recent_update_cache = {}

    teams = _distinct_teams()
    for team in teams:
        row = _latest_row_for_team(team)
        if not row:
            continue

        for pos in POSITION_COLS:
            if pos not in row:
                continue
            name, status = _coerce_to_name_status(row[pos])
            if not name:
                continue
            player_position = pos[:2].upper()

            if _player_recently_updated(name, team, player_position, recent_cutoff, recent_update_cache):
                continue

            # TODO: Need to modify the message to match what verify and extract expects

            messages.append({
                "TEAM_NAME": team,
                "PLAYER_POSITION": player_position,
                "PLAYER_NAME": name,
                "status": status,
                "source": "depthcharts",
                "depth_row_key": row.get("key"),
                "run_id": run_id,
                "currentSeason": currentSeason,
            })

    # Send to SQS in batches of 10
    counter = 0
    for batch in _chunks(messages, 10):
        print(batch)
        entries = []
        for msg in batch:
            counter += 1
            entries.append({
                "Id": f"{msg['TEAM_NAME']}-{msg['PLAYER_POSITION']}-{counter}",
                "MessageBody": json.dumps(msg),
            })
        sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)

    return {
        "teams_scanned": len(teams),
        "messages_enqueued": len(messages),
        "run_id": run_id,
    }

if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({}, None)
