"""Lambda entrypoint for the weekly update-team-stats job.

This file is intentionally lightweight so new logic can be dropped in quickly.
Place any shared modules inside the sibling `antelope_utils` package.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict
import supabase
import pandas as pd
from datetime import datetime as dt
from antelope_utils.supabase_utils import read_schedule_from_supabase
from antelope_utils.data_prep_utils import convert_date_to_nfl_week, get_nfl_season
from antelope_utils.extract_team_stats_utils import collect_season_team_stats, load_stat_from_OU_table

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    invocation_time = datetime.now(timezone.utc).isoformat()
    logger.info(
        "update-team-stats invoked",
        extra={
            "invocation_time": invocation_time,
            "request_id": getattr(context, "aws_request_id", "unknown"),
        },
    )

    supabase_url = 'https://ombuhcmutttxxjsyjerf.supabase.co'
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tYnVoY211dHR0eHhqc3lqZXJmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU5NzUxNTUsImV4cCI6MjA3MTU1MTE1NX0._SsqZCRWM-e77gqxsrZSuw1OPvIIpYxFoNFsPkBx_Gc"   
    # Initialize the Supabase client
    client = supabase.Client(supabase_url, supabase_key)

    # load the schedule
    schedule = read_schedule_from_supabase(season=currentSeason, week=currentWeek, client=client)

    # load current week and season
    currentWeek = convert_date_to_nfl_week(dt.now())
    currentSeason = get_nfl_season()


    print("trying to collect Season OUs",str(currentSeason),str(currentWeek))

    """if currentWeek == 'WildCard':
        currentWeek = 19
    elif currentWeek == 'Division':
        currentWeek = 20
    elif currentWeek == "ConfChamp":
        currentWeek = 21
    elif currentWeek == "SuperBowl":
        currentWeek = 23"""

    season_ou_df = schedule

    # load the stats
    

    print("trying to collect Season OUs",season,week)

    # reading csv
    path = 'futures_table_2024.csv'
    OUHist = pd.read_csv(path)
        
    # aggregate SeasonOUs to schedule
    schedule['HomeTeamSeasonOU'] = season_ou_df.apply(
        lambda row: load_stat_from_OU_table('Win Total', row['HomeTeam'], row['Season'],OUHist),
        axis = 1)

    schedule['AwayTeamSeasonOU'] = season_ou_df.apply(
        lambda row: load_stat_from_OU_table('Win Total', row['AwayTeam'], row['Season'],OUHist),
        axis = 1)
    # last year's Team Win Toal
    schedule['AwayTeamSeasonOU_lastYear'] = season_ou_df.apply(
        lambda row: load_stat_from_OU_table('Win Total', row['AwayTeam'], row['Season']-1,OUHist),axis=1
    )

    schedule['HomeTeamSeasonOU_lastYear'] = season_ou_df.apply(
        lambda row: load_stat_from_OU_table('Win Total', row['HomeTeam'], row['Season']-1,OUHist),axis=1
    )


    season_ou_df['Season'] = currentSeason
    season_ou_df['Week'] = currentWeek
    season_ou_df['key'] = season_ou_df['Season'].astype(str) + '_' + season_ou_df['Week'].astype(str) + '_' + season_ou_df['AwayTeam'].astype(str) + '_' +  season_ou_df['HomeTeam'].astype(str)
    season_ou_df = season_ou_df.drop(['Season', 'Week', 'HomeTeam', 'AwayTeam'], axis=1)

    print(season_ou_df)

    # write or update the week schedule
    table_name = "SeasonOU_matchup"

    # table columns: key, AWAY, HOME, date, Season, Week 

    for index, row in season_ou_df.iterrows():
        
        # check if record is already in table
        response_check = client.from_(table_name).select("*").eq('key', row['key']).execute()
        
        if response_check.data == []:
            
            # Data to insert (as a dictionary)
            data_to_insert = {
                "key": row['key'],
                'AwayTeamSeasonOU': row['AwayTeamSeasonOU'],
                'HomeTeamSeasonOU': row['HomeTeamSeasonOU'],
                'AwayTeamSeasonOU_lastYear': row['AwayTeamSeasonOU_lastYear'],
                'HomeTeamSeasonOU_lastYear': row['HomeTeamSeasonOU_lastYear'],
            }

            # Perform the insert operation
            response_write = client.from_(table_name).insert([data_to_insert]).execute()
        
        # else update the record with latest
        else:
            pass
            # TODO: // need to set up an update function

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "update-team-stats placeholder executed",
                "timestamp": invocation_time,
            }
        ),
    }
