import os
import json
import boto3
from datetime import datetime, timezone
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

            # TODO: Need to modify the message to match what verify and extract expects

            messages.append({
                "TEAM_NAME": team,
                "PLAYER_POSITION": pos[:2].upper(),
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