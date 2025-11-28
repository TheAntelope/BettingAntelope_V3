"""Entry point for the get-schedules Lambda function."""
import json
import logging
from datetime import datetime, timezone
from antelope_utils.data_prep_utils import process_dates_from_football_reference
from antelope_utils.data_prep_utils import convert_date_to_nfl_week
import supabase
import pandas as pd
from datetime import datetime as dt
import requests


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    supabase_url = 'https://ombuhcmutttxxjsyjerf.supabase.co'
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tYnVoY211dHR0eHhqc3lqZXJmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU5NzUxNTUsImV4cCI6MjA3MTU1MTE1NX0._SsqZCRWM-e77gqxsrZSuw1OPvIIpYxFoNFsPkBx_Gc"

    # Initialize the Supabase client
    client = supabase.Client(supabase_url, supabase_key)


    week = convert_date_to_nfl_week(dt.now())
    print('Week Number:',week)

    season = 2025
    currentWeek = week

    if week == 19:
        currentWeek = 'WildCard'
    elif week == 20:
        currentWeek = 'Division'
    elif week == 21:
        currentWeek = "ConfChamp"
    elif week == 23:
        currentWeek = "SuperBowl"

    currentSeason = season
    
    schedule = pd.DataFrame()

    start = dt.now()
    api_str = "https://u2jagxeesd.execute-api.us-east-2.amazonaws.com/development/get_season_schedule37?season="+str(currentSeason)+"&week="+str(currentWeek)
    response = requests.get(api_str) 
    print(response.json())
    homeTeams = response.json()['HomeTeams'].replace('[','').replace(']','').replace('"','').replace(' ','').split(",")
    awayTeams = response.json()['AwayTeams'].replace('[','').replace(']','').replace('"','').replace(' ','').split(",")
    days = response.json()['Days'].replace('[','').replace(']','').replace('"','').replace(' ','').split(",")
    dates = response.json()['Dates'].replace('[','').replace(']','').replace('"','').replace(' ','').split(",")

    schedule['Day'] = days
    schedule['Date'] = dates
    schedule['HomeTeam'] = homeTeams
    schedule['HomeTeam'] = homeTeams
    schedule['AwayTeam'] = awayTeams
    schedule['Season'] = currentSeason
    schedule['Week'] = currentWeek
    schedule['key'] = schedule['Season'].astype(str) + '_' + schedule['Week'].astype(str) + '_' + schedule['AwayTeam'].astype(str) + '_' +  schedule['HomeTeam'].astype(str)


    # write or update the week schedule
    table_name = "Schedule"

    # table columns: key, AWAY, HOME, date, Season, Week 

    for index, row in schedule.iterrows():
        
        # check if record is already in table
        response_check = client.from_(table_name).select("*").eq('key', row['key']).execute()
        
        if currentWeek == 'WildCard':
            week_to_insert = 19
        elif currentWeek == 'Division':
            week_to_insert = 20
        elif currentWeek == "ConfChamp":
            week_to_insert = 21
        elif currentWeek == "SuperBowl":
            week_to_insert = 23
        else:
            week_to_insert = row['Week']
        
        # if record is not in table, write it to table
        if response_check.data == []:
            # Data to insert (as a dictionary)
            data_to_insert = {
                "key": row['key'],
                'Day': row['Day'],
                'Date': process_dates_from_football_reference(row['Date']),
                'HomeTeam': row['HomeTeam'],
                'AwayTeam': row['AwayTeam'],
                'Season': row['Season'],
                'Week': week_to_insert
            }
            
            # Perform the insert operation
            response_write = client.from_(table_name).insert([data_to_insert]).execute()
                
        # else, pass
        else: pass
    msg = 'loaded schedule in:' + str(dt.now() - start)

    request_id = getattr(context, 'aws_request_id', 'unknown')
    timestamp = datetime.now(timezone.utc).isoformat()
    LOGGER.info("get-schedules invoked", extra={"request_id": request_id, "timestamp": timestamp})
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": msg,
            "request_id": request_id,
            "timestamp": timestamp,
        }),
    }

if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({}, None)