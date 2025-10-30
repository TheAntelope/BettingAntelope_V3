# imports
import yaml
from yaml.loader import SafeLoader
from datetime import datetime as dt
from datetime import datetime
from datetime import date
from collections.abc import Mapping
import supabase
import os
from antelope_utils.extract_player_stats import refresh_player_info

# lambda_function to verify and extract player data
def lambda_handler(event, context):

    # Initialize the Supabase client
    SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
    SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
    client = supabase.Client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # read metadata dict
    with open('meta_data.yaml') as f:
        meta_data_dict = yaml.load(f, Loader=SafeLoader)

    PLAYER_NAME = event['PLAYER_NAME']
    TEAM_NAME = event['TEAM_NAME']
    PLAYER_POSITION = event['PLAYER_POSITION']
    currentSeason = event['currentSeason']
    status = event['status']

    print(
        "Verifying:", PLAYER_NAME, 
        'Team', TEAM_NAME, 
        'Position', PLAYER_POSITION, 
        'Season', currentSeason, 
        'Status', status
        )
    
    # refresh player info
    response = refresh_player_info(
            name=PLAYER_NAME,
            team=TEAM_NAME,
            position=PLAYER_POSITION,
            currentSeason=currentSeason,
            client=client,
            meta_data_dict=meta_data_dict,
            status=status
        )
    print(response)

    return "player stats loaded successfully"

# run when testing locally
if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({
        "PLAYER_NAME": "Josh Allen",
        "TEAM_NAME": "BUF",
        "PLAYER_POSITION": "QB",
        "currentSeason": 2025,
        "status": "Healthy"
    }, None)