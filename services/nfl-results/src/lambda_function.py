import os
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
RESULTS_TABLE = os.environ.get("RESULTS_TABLE", "GameResults")  # set to your table name
ODDS_TABLE = os.environ.get("ODDS_TABLE", "Odds")
RESULTS_KEY_COL = os.environ.get("RESULTS_KEY_COL", "key")
# JSON: {"HomeScore":"homescore","AwayScore":"awayscore","HomeWinner":"home_winner","HomeCover":"home_cover","OverHit":"over_hit"}
RESULTS_COLMAP = json.loads(os.environ.get("RESULTS_COLMAP", "{}"))
DEBUG = os.environ.get("SUPABASE_DEBUG", "false").lower() == "true"

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"

def rename_team_str(team):
    """
    takes a string and renames to the NFL abbreviated name
    
    """
    teams_dict = {
        #AFC East
        'New England Patriots' : 'NE',
        'NE Patriots' : 'NE',
        'Miami Dolphins' : 'MIA',
        'MIA Dolphins' : 'MIA',
        'Buffalo Bills' : 'BUF',
        'BUF Bills': 'BUF',
        'New York Jets' : 'NYJ',
        'NY Jets' : 'NYJ',
        "NWE":"NE",
        
        #AFC North
        'Baltimore Ravens' : 'BAL',
        'Pittsburgh Steelers' : 'PIT',
        'Cleveland Browns' : 'CLE',
        'Cincinnati Bengals' : 'CIN',
        'BAL Ravens' : 'BAL',
        'PIT Steelers' : 'PIT',
        'CLE Browns' : 'CLE',
        'CIN Bengals' : 'CIN',
        
        #AFC South
        'Houston Texans' : 'HOU',
        'Jacksonville Jaguars' : 'JAX',
        'Tennessee Titans' : 'TEN',
        'Indianapolis Colts' : 'IND',
        'HOU Texans' : 'HOU',
        'JAX Jaguars' : 'JAX',
        'TEN Titans' : 'TEN',
        'IND Colts' : 'IND',
        
        #AFC West
        'OAK' : 'LV',
        'Oakland Raiders' : 'LV',
        'Las Vegas Raiders' : 'LV',
        'LV Raiders' : 'LV',
        'Kansas City Chiefs' : 'KC',
        'KC Chiefs' : 'KC',
        'Denver Broncos' : 'DEN',
        'Los Angeles Chargers' : 'LAC',
        'San Diego Chargers': 'LAC',
        'DEN Broncos' : 'DEN',
        'LA Chargers' : 'LAC',
        'LAC Chargers' : 'LAC',
        "KAN":"KC",
        "LVR":"LV",
        
        #NFC East
        'New York Giants' : 'NYG',
        'Philadelphia Eagles' : 'PHI',
        'Dallas Cowboys' : 'DAL',
        'NY Giants' : 'NYG',
        'PHI Eagles' : 'PHI',
        'DAL Cowboys' : 'DAL',
        'Washington Redskins': 'WAS',
        'Washington Commanders': 'WAS',
        'Washington' : 'WAS',
        'WSH' : 'WAS',
        'Washington Football Team' : 'WAS',
        'WAS Commanders' : 'WAS',
        'WAS Football Team' : 'WAS',
        
        #NFC North
        'Green Bay Packers' : 'GB',
        'Minnesota Vikings' : 'MIN',
        'Detroit Lions' : 'DET',
        'Chicago Bears' : 'CHI',
        'GB Packers' : 'GB',
        'MIN Vikings' : 'MIN',
        'DET Lions' : 'DET',
        'CHI Bears' : 'CHI',
        'GNB' : 'GB',
        
        #NFC South
        'New Orleans Saints' : 'NO',
        'Atlanta Falcons' : 'ATL',
        'Carolina Panthers' : 'CAR',
        'Tampa Bay Buccaneers' : 'TB',
        'NO Saints' : 'NO',
        'ATL Falcons' : 'ATL',
        'CAR Panthers' : 'CAR',
        'TB Buccaneers' : 'TB',
        "NOR":"NO",
        "TAM":"TB",
        
        #NFC West
        'San Francisco 49ers' : 'SF',
        'Seattle Seahawks' : 'SEA',
        'Los Angeles Rams' : 'LAR',
        'St. Louis Rams': 'LAR',
        'Arizona Cardinals' : 'ARI',
        'SF 49ers' : 'SF',
        'SEA Seahawks' : 'SEA',
        'LA Rams' : 'LAR',
        'ARI Cardinals' : 'ARI',
        "SFO":"SF",
    }

    new_team = team
    
    #iterate though the teams to reanme the string
    for value in teams_dict:
        new_team = new_team.replace(value, teams_dict[value])
    
    return new_team

def _http_get_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _http_post_json(url, payload, headers=None, method="POST"):
    data = json.dumps(payload).encode("utf-8")
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            try:
                return json.loads(body or b"[]"), resp.status
            except Exception:
                return (body.decode() if body else ""), resp.status
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode()
        except Exception:
            err_body = "<no body>"
        # If in debug, bubble the error body back in the Lambda result
        if DEBUG:
            return {"supabase_error": err_body, "url": url, "method": method}, e.code
        # Otherwise just log and re-raise
        print(f"[Supabase] HTTP {e.code} on {method} {url} :: {err_body}")
        raise

def _patch_by_key(table, key_col, row_dict):
    qp = urllib.parse.urlencode({key_col: f"eq.{row_dict[key_col]}"}, safe=".,=")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{qp}"
    headers = _supabase_headers(); headers["Prefer"] = "return=representation"
    return _http_post_json(url, row_dict, headers=headers, method="PATCH")

def _insert_row(table, row_dict):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = _supabase_headers(); headers["Prefer"] = "return=representation"
    return _http_post_json(url, [row_dict], headers=headers, method="POST")


def _supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Prefer": "resolution=merge-duplicates"  # enable UPSERT
    }

def _fetch_latest_odds(key):
    # GET /rest/v1/Odds?key=eq.<key>&select=PointSpread,OverUnder,UpdatedAt&order=UpdatedAt.desc.nullslast&limit=1
    qp = urllib.parse.urlencode({
        "key": f"eq.{key}",
        "select": "PointSpread,OverUnder,created_at",
        "order": "created_at.desc.nullslast",
        "limit": 1
    }, safe=".,=")
    url = f"{SUPABASE_URL}/rest/v1/{ODDS_TABLE}?{qp}"
    req = urllib.request.Request(url, headers=_supabase_headers())
    with urllib.request.urlopen(req, timeout=30) as resp:
        rows = json.loads(resp.read())
    return rows[0] if rows else None

def _upsert_results(rows):
    """
    Idempotent upsert without requiring UNIQUE/PK on `key`:
      1) PATCH where key matches; if 0 rows changed, 2) POST insert.
    """
    upserted = 0
    for r in rows:
        if RESULTS_KEY_COL not in r:
            raise RuntimeError(f"Row missing key column '{RESULTS_KEY_COL}': {r}")
        updated, _ = _patch_by_key(RESULTS_TABLE, RESULTS_KEY_COL, r)
        if isinstance(updated, list) and updated:
            upserted += 1
            continue
        inserted, _ = _insert_row(RESULTS_TABLE, r)
        if isinstance(inserted, list) and inserted:
            upserted += 1
        else:
            print(f"[Supabase] Insert returned empty for key={r.get(RESULTS_KEY_COL)}")
    return {"status": 200, "upserted": upserted}

def _prev_week_date_range(now_utc):
    """Return (start_date, end_date_inclusive) as US/Eastern Thu..Tue for the NFL week that just ended Monday."""
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    days_since_monday = (now_et.weekday() - 0) % 7  # Monday=0
    last_monday = (now_et - timedelta(days=days_since_monday)).date()
    start_date = last_monday - timedelta(days=4)  # Thursday
    end_date = last_monday + timedelta(days=1)    # Tuesday
    return start_date, end_date

def _fetch_completed_events(dates_range=None):
    if dates_range:
        url = f"{ESPN_SCOREBOARD}?dates={dates_range}&limit=500"
    else:
        s, e = _prev_week_date_range(datetime.now(tz=ZoneInfo('UTC')))
        dates_param = f"{s.strftime('%Y%m%d')}-{e.strftime('%Y%m%d')}"
        url = f"{ESPN_SCOREBOARD}?dates={dates_param}&limit=500"
    data = _http_get_json(url)
    events = data.get("events", []) or []
    completed = []
    for ev in events:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        st = (comp.get("status") or {}).get("type") or {}
        if not st.get("completed", False):
            continue
        completed.append((ev, comp))
    return completed

def _parse_game(ev, comp):
    home = next(c for c in comp["competitors"] if c.get("homeAway") == "home")
    away = next(c for c in comp["competitors"] if c.get("homeAway") == "away")
    home_team = home["team"]["abbreviation"]
    away_team = away["team"]["abbreviation"]
    home_team = rename_team_str(home_team)
    away_team = rename_team_str(away_team)
    home_score = int(home.get("score", 0))
    away_score = int(away.get("score", 0))
    season = (ev.get("season") or {}).get("year")
    week = (ev.get("week") or {}).get("number")
    key = f"{season}_{week}_{away_team}_{home_team}"
    return key, home_team, away_team, home_score, away_score

def _compute_flags(home_score, away_score, point_spread, over_under):
    # Winner (ties -> False)
    home_winner = home_score > away_score

    # Cover (home perspective)
    home_cover = None
    if point_spread is not None:
        margin = home_score - away_score
        adj = margin + float(point_spread)  # home spread (negative if favored)
        home_cover = True if adj > 0 else (False if adj < 0 else None)

    # Total
    over_hit = None
    if over_under is not None:
        total = home_score + away_score
        diff = total - float(over_under)
        over_hit = True if diff > 0 else (False if diff < 0 else None)

    return home_winner, home_cover, over_hit

def lambda_handler(event, context):
    # Optional manual test: pass {"dates":"YYYYMMDD-YYYYMMDD"} in event
    dates_override = None
    if isinstance(event, dict):
        dates_override = event.get("dates")

    completed = _fetch_completed_events(dates_override)
    rows = []

    for ev, comp in completed:
        
        key, home_team, away_team, home_score, away_score = _parse_game(ev, comp)
        odds = _fetch_latest_odds(key)
        ps = odds.get("PointSpread") if odds else None
        ou = odds.get("OverUnder") if odds else None

        home_winner, home_cover, over_hit = _compute_flags(home_score, away_score, ps, ou)

        print("appending", key, home_score, away_score, home_winner, home_cover, over_hit)

        

        # Only write the fields you said your table has (+ key)
        rows.append({
            "key": key,
            "HomeScore": home_score,
            "AwayScore": away_score,
            "HomeWinner": home_winner,
            "HomeCover": home_cover,
            "OverHit": over_hit
        })

    if not rows:
        return {"status": "ok", "message": "No completed games found (offseason/bye week window)."}

    body, status = _upsert_results(rows)
    print(f"[Supabase] Upserted {len(rows)} rows with status {status}: {body}")
    return {"status": status, "upserted": len(rows)}

