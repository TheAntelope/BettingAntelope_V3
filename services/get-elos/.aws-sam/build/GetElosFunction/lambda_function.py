from datetime import datetime
import logging
from typing import Any, Dict
import pandas as pd
from antelope_utils.data_prep_utils import rename_teams, rename_team_str
import requests
import supabase

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

def unpack_game_id_nfelo(game_id: str):
    """
    Unpack a string like '2008_19_PHI_NYG' into Season, Week, AwayTeam, HomeTeam.

    Parameters:
        game_id (str): The game identifier in format 'Season_Week_Away_Home'.

    Returns:
        tuple: (Season, Week, AwayTeam, HomeTeam)
    """
    season, week, away, home = game_id.split("_")
    return int(season), int(week), away, home

def fetch_csv(url: str) -> pd.DataFrame:
    """
    Fetches a CSV from a URL and returns a pandas DataFrame.
    Raises an exception on failure.
    """
    resp = requests.get(url)
    resp.raise_for_status()  # Raises an HTTPError on bad request
    from io import StringIO
    return pd.read_csv(StringIO(resp.text))

def get_most_recent_elo(df: pd.DataFrame, team: str, stat: str) -> float:
    """
    Get the most recent Elo rating for a given team.
    Chooses the max Season, then within that Season the max Week.

    Parameters:
        df (pd.DataFrame): DataFrame with ['Season', 'Week', 'Team', 'Elo'].
        team (str): Team code (e.g. 'PHI').
        stat (str): eg: elo, 

    Returns:
        float: The most recent Elo rating.
    """
    # Filter for the team
    mask = (df["AwayTeam"] == team) | (df["HomeTeam"] == team)
    team_df = df[mask].copy()

    if team_df.empty:
        raise ValueError(f"No records found for team {team}")

    # Find the latest season
    max_season = team_df["Season"].max()

    # Within that season, find the latest week
    latest_week = team_df.loc[team_df["Season"] == max_season, "Week"].max()

    # Get the Elo value at that season & week
    latest_row = team_df[(team_df["Season"] == max_season) & 
                         (team_df["Week"] == latest_week)]

    if latest_row["HomeTeam"].iloc[0] == team:
        stat_name = "Home" + stat
        return latest_row[stat_name].iloc[0]
    elif latest_row["AwayTeam"].iloc[0] == team:
        stat_name = "Away" + stat
        return latest_row[stat_name].iloc[0]
    else:
        return "No stat found"




def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle the scheduled invocation."""
    LOGGER.info("get-elos Lambda invoked with event: %s", event)

    raw_url = (
        "https://raw.githubusercontent.com/"
        "greerreNFL/nfelo/main/output_data/nfelo_games.csv"
    )

    supabase_url = 'https://ombuhcmutttxxjsyjerf.supabase.co'
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tYnVoY211dHR0eHhqc3lqZXJmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU5NzUxNTUsImV4cCI6MjA3MTU1MTE1NX0._SsqZCRWM-e77gqxsrZSuw1OPvIIpYxFoNFsPkBx_Gc"

    # Initialize the Supabase client
    client = supabase.Client(supabase_url, supabase_key)

    elo_df = fetch_csv(raw_url)
    print("CSV loaded successfully!")
    elo_df['Season'] = elo_df.apply(lambda row: unpack_game_id_nfelo(
        row['game_id'])[0], axis = 1)

    elo_df = elo_df[elo_df['Season'] ==2025]  # Display first few rows; remove if not needed

    # unpack Team Names
    elo_df['AwayTeam'] = elo_df.apply(lambda row: unpack_game_id_nfelo(
        row['game_id'])[2], axis = 1)

    elo_df['HomeTeam'] = elo_df.apply(lambda row: unpack_game_id_nfelo(
        row['game_id'])[3], axis = 1)

    # rename Team names eg OAK --> LV
    elo_df['AwayTeam'] = elo_df.apply(lambda row: rename_team_str(row['AwayTeam']), axis = 1)
    elo_df['HomeTeam'] = elo_df.apply(lambda row: rename_team_str(row['HomeTeam']), axis = 1)

    elo_df['Week'] = elo_df.apply(lambda row: unpack_game_id_nfelo(
    row['game_id'])[1], axis = 1)

    elo_df['Homeelo'] = elo_df['starting_nfelo_home']
    elo_df['Awayelo'] = elo_df['starting_nfelo_away']

    elo_df['Homeelo_prob'] = elo_df['nfelo_home_probability_open']
    elo_df['Awayelo_prob'] = 1 - elo_df['nfelo_home_probability_open']

    elo_df['Homeqb_value'] = elo_df['home_538_qb_adj']
    elo_df['Awayqb_value'] = elo_df['away_538_qb_adj']

    elo_df['elo_diff'] = elo_df['Homeelo'] - elo_df['Awayelo']
    elo_df['elo_prob_diff'] = elo_df['Homeelo_prob'] - elo_df['Awayelo_prob']
    elo_df['total_elo'] = elo_df['Homeelo'] + elo_df['Awayelo']

    elo_df['key'] = elo_df['Season'].astype(str) + '_' + elo_df['Week'].astype(str) + '_' + elo_df['AwayTeam'].astype(str) + '_' +  elo_df['HomeTeam'].astype(str)


    # I need to make sure I am uploading the latest known elo data to supabase
# I need to also make sure that I am upoading with Antelope Key instead of NFLelo key
    table_name = "Elo"
    for index, row in elo_df.iterrows():
    
    # check if record is already in table
        response_check = client.from_(table_name).select("*").eq('key', row['key']).execute()
        
        if response_check.data == []:
            
            # Data to insert (as a dictionary)
            data_to_insert = {
                "key": row['key'],
                'Homeelo': row['Homeelo'],
                'Awayelo': row['Awayelo'],
                'Homeqb_value': row['Homeqb_value'],
                'Awayqb_value': row['Awayqb_value'],
                "Homeelo_prob":row['Homeelo_prob'],
                'Awayelo_prob':row['Awayelo_prob'],
                'elo_diff':row['elo_diff'],
                'elo_prob_diff':row['elo_prob_diff'],
                'total_elo':row['total_elo'],
            }

            # Perform the insert operation
            response_write = client.from_(table_name).insert([data_to_insert]).execute()
        
        # else update the record with latest
        else:
            pass
            # TODO: // need to set up an update function

    return {
        "message": "get-elos placeholder response",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({}, None)