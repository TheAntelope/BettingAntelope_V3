"""AWS Lambda entrypoint for scraping NFL depth charts and syncing them to Supabase."""
from __future__ import annotations

import os
import time
from datetime import datetime as dt
from datetime import datetime
from datetime import date
from collections.abc import Mapping, Sequence
from typing import Any, Dict, Iterable, Iterator, List, Optional

import supabase
import pandas as pd
# TODO: Need to basically just pull in the eniter extract roster utils
# then I need to add some code to upsert to supabase

DEPTH_CHART_PAGE = "https://cdn.espn.com/core/nfl/team/depth/_/name/{team_code}?xhr=1"
NFL_TEAM_CODES = [
    "WAS",
    "ARI",
    "ATL",
    "BAL",
    "BUF",
    "CAR",
    "CHI",
    "CIN",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GB",
    "HOU",
    "IND",
    "JAX",
    "KC",
    "LV",
    "LAC",
    "LAR",
    "MIA",
    "MIN",
    "NE",
    "NO",
    "NYG",
    "NYJ",
    "PHI",
    "PIT",
    "SF",
    "SEA",
    "TB",
    "TEN",
    
]

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DEPTH_CHARTS_TABLE = os.environ.get("DEPTH_CHARTS_TABLE", "DepthCharts")
KEY_COLUMN = os.environ.get("DEPTH_CHART_KEY_COL", "key")
BATCH_SIZE = int(os.environ.get("DEPTH_CHART_BATCH_SIZE", "50"))

USER_AGENT = os.environ.get(
    "DEPTH_CHART_USER_AGENT",
    "Mozilla/5.0 (compatible; BettingAntelopeDepthChartBot/1.0; +https://example.com/bot)",
)

def read_roster(PATH, TEAM):
    df = pd.read_html(PATH,header=0)

    # get the list of positions on the depth chart from espn
    positions_list = [i[0] for i in df[0].values]
    # need to insert QB at the beginning because of a blank header
    positions_list.insert(0,"QB")

    roster_df = pd.DataFrame()
    roster_df['Position'] = positions_list

    # join columsn
    roster_df = roster_df.join(df[1])
    
    # add team to dataframe
    roster_df['Team'] = TEAM
    
    return roster_df
    
def read_def_roster(PATH, TEAM):
    df = pd.read_html(PATH,header=0)
    # get the list of positions on the depth chart from espn
    positions_list = [i[0] for i in df[2].values]
    # need to insert QB at the beginning because of a blank header
    positions_list.insert(0,"LDE")

    roster_df = pd.DataFrame()
    roster_df['Position'] = positions_list

    # join columsn
    roster_df = roster_df.join(df[3])
    
    # add team to dataframe
    roster_df['Team'] = TEAM
    
    return roster_df

def insert_position(df, position, name, status, team):
    new_row = pd.DataFrame([{
        'Position': position,
        'Name': name,
        'Status': status,
        'Team': team
    }])
    df = pd.concat([df, new_row], ignore_index=True)
    return df

def check_status(name):
    # check for status
    # possible Statusses:
        # P : Probable
        # Q : Questionable
        # O : Out
        # PUP : Physically unable to perform
        # SUS : Suspended by NFL or current team
        # IR : Injured Reserve

    # if there is no player, no need for status    
    if name == '-':
        return None

    if name[-2:]== ' P':
        status = "Probable"
    elif name[-2:] == ' Q':
        status = "Questionable"
    elif name[-2:] == ' D':
        status = "Doubtful"
    elif name[-2:] == ' O':
        status = "Out"
    elif name[-4:] == ' PUP':
        status = 'Physically unable to perform'
    elif name[-4:] == ' SUS':
        status = 'Suspended by NFL or current team'
    elif name[-5:] == ' SUSP':
        status = 'Suspended by NFL or current team'
    elif name[-3:] == ' IR':
        status = "Injured Reserve"
    else:
        status = "Healthy"
    return status

def create_roster_df(TEAM):
    """
    reads, groups and returns a roster_df for a given team
    
    Params:
    -------
    - TEAM: str
    
    Returns:
    -------
    - roster_df: df
        data frame has 4 columns for a given team
        - Position
        - Name
        - Status
        - Team
    
    """
    # variables
    healthy_statusses = ['Healthy', 'Questionable']
    if TEAM == 'WAS':
        PATH = 'https://www.espn.com/nfl/team/depth/_/name/wsh/washington-commanders'
    else:
        PATH = 'https://www.espn.com/nfl/team/depth/_/name/' + TEAM
    
    # read_roster
    df = read_roster(PATH, TEAM)

    # read def roster
    def_df = read_def_roster(PATH, TEAM)

    # append o_df and d_df
    df = pd.concat([df, def_df], ignore_index=True)
    
    grouped_df = df.groupby(['Position', 'Starter', '2nd', '3rd', '4th' , 'Team']).count()
    
    roster_df = pd.DataFrame(columns = ['Position', 'Name', 'Status'])
    for row, col in grouped_df.iterrows():

        name = row[1]
        position = row[0]
        status = check_status(name)
        team = row[5]

        # get the starting QB
        if position == "QB":
            names = list(row[1:5])
            healthy_names = [check_status(i) in healthy_statusses for i in names]
            healthy_idx = healthy_names.index(True)
            # Append the first healthy QB
            roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="QB", status=check_status(names[healthy_idx]),team=team)

        # get RB1 & RB2
        if position == 'RB':
            names = list(row[1:5])
            # while RBs < 2:
            RBs = []
            while len(RBs) < 2:
                print(len(RBs))
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                print("Healthy RBs:", names, healthy_names)
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy RB
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="RB", status=check_status(names[healthy_idx]),team=team)
                    RBs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break

        # get TE1 & TE2
        if position == 'TE':
            names = list(row[1:5])
            # while TEs < 2:
            TEs = []
            while len(TEs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                healthy_idx = healthy_names.index(True)
                # Append the first healthy TE
                roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="TE", status=check_status(names[healthy_idx]),team=team)
                TEs.append(names[healthy_idx])
                names.remove(names[healthy_idx])
                if names.count(True) == 0:
                    break

        # Get WR
        if position == "WR":
            names = list(row[1:5])
            # while WRs < 5:
            WRs = []
            while len(WRs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy WR
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="WR", status=check_status(names[healthy_idx]),team=team)
                    WRs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break
                
        # Get CB1 & CB2
        if position in ["LCB","RCB"]:
            names = list(row[1:5])
            CBs = []
            while len(CBs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy CB
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="CB", status=check_status(names[healthy_idx]),team=team)
                    CBs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break

        # Get SS
        if position in ["SS"]:
            names = list(row[1:5])
            
            SSs = []
            while len(SSs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy SSs
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="SS", status=check_status(names[healthy_idx]),team=team)
                    SSs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break
                
        # Get LB
        if position in ["WLB", "LILB", "RILB", "SLB"]:
            names = list(row[1:5])
            
            LBs = []
            while len(LBs) < 4:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy Bss
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="LB", status=check_status(names[healthy_idx]),team=team)
                    LBs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break

        # Get DE
        if position in ["LDE", "RDE"]:
            names = list(row[1:5])
            
            DEs = []
            while len(DEs) < 4:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy Bss
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="DE", status=check_status(names[healthy_idx]),team=team)
                    DEs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break
                
    return roster_df


def build_depth_chart(df):
    # Ensure consistent casing and strip whitespace
    df['Position'] = df['Position'].str.upper().str.strip()

    # Sort by Position (alphabetical) and optionally within each position
    df_sorted = df.sort_values(by=['Position', 'Name']).reset_index(drop=True)

    # Count how many times each position has occurred so far
    position_counts = {}

    result = {}

    for _, row in df_sorted.iterrows():
        pos = row['Position'].lower()
        name = row['Name'].strip()
        status = row['Status'].strip()

        # Strip qualifiers like " Q" from names, if desired
        if name.endswith(" Q"):
            name = name[:-2].strip()

        # Increment count per position
        count = position_counts.get(pos, 0) + 1
        position_counts[pos] = count

        # Compose label like "rb1", "cb2", etc.
        key = f"{pos}{count}"
        result[key] = (name, status)

    # Add last_updated timestamp
    result["last_updated"] = dt.now()
    result["synced_at"] = dt.now()

    return result

def serialize_datetimes(obj):
    if isinstance(obj, dict):
        return {k: serialize_datetimes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetimes(v) for v in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj



def to_jsonable(obj):
    if obj is None or isinstance(obj, (int, float, str, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, set) or isinstance(obj, tuple) or (isinstance(obj, list)):
        return [to_jsonable(x) for x in obj]
    if isinstance(obj, Mapping):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    # fallback
    return str(obj)


def lambda_handler(event, context):  # pragma: no cover - entry point for AWS Lambda
    print("Lambda handler started")

    # Initialize the Supabase client
    client = supabase.Client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    for team in NFL_TEAM_CODES:
        print(f"Processing team: {team}")
        roster_df = create_roster_df(team)
        print(roster_df.sort_values(by=['Position', 'Name']))

        upload_dict = build_depth_chart(roster_df)
        upload_dict = serialize_datetimes(upload_dict)
        upload_dict["team"] = team
        upload_dict = to_jsonable(upload_dict)
        print(upload_dict)

        # upload to supabase
        response_write = client.from_(DEPTH_CHARTS_TABLE).insert([upload_dict]).execute()
        print(f"Upsert response for team {team}: {response_write}")

    return {"DEPTH CHARTS PROCESSED"}

if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({}, None)