"""Placeholder AWS Lambda for create-team-efficiencues service."""
from __future__ import annotations

import os
import supabase
from datetime import datetime as dt
import importlib
from typing import Any, Dict
import requests


from antelope_utils.extract_player_stats import verify_and_extract_player_data_v2
from antelope_utils.extract_player_stats import clean_player_name
from antelope_utils.data_prep_utils import undict, rename_positions
from antelope_utils.extract_roster_utils import create_roster_df, check_status
from antelope_utils.extract_player_stats import verify_and_extract_player_data_v2
from antelope_utils.feature_engineering_utils import calc_team_effieciencies
from antelope_utils.data_prep_utils import get_stadium
from antelope_utils.s3_utils import fetch_pkl_from_s3, read_df_from_s3

from antelope_utils.data_prep_utils import undict, process_team_stats_from_dict, convert_date_to_nfl_week
from antelope_utils.supabase_utils import read_schedule_from_supabase

def get_wind_speed_v2(team: str, api_key: str) -> float:
    """
    Fetch the wind speed (m/s) for a given US city using OpenWeatherMap API.
    
    Args:
        city (str): City name (e.g., "Chicago")
        api_key (str): Your OpenWeatherMap API key

    Returns:
        float: Wind speed in meters/second
    """
    print(team)
    stadium = get_stadium(team)
    city = stadium['city']
    
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": f"{city},US",   # search within US
        "appid": api_key,
        "units": "metric"    # or "imperial" if you want mph
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    if response.status_code != 200:
        raise Exception(f"Error fetching data: {data.get('message', 'Unknown error')}")

    wind_speed = data["wind"]["speed"]
    return wind_speed


def collect_and_calc_team_efficiencies_v2(TEAM_NAME, client, meta_data_dict):
    
    """
    new version of the collect and calc_team_efficiencies function in feature_engineering_utils
    """
    table_name = 'PlayerMetaData'
    
    # collect roster for a team
    roster_df = create_roster_df(TEAM_NAME)
    
    roster_df['Name'] = roster_df.apply(lambda row: clean_player_name(row['Name']), axis=1)
    """# for each player on the team, collect their stats dictionary
    for index, row in roster_df.iterrows():
        table_name = 'PlayerMetaData'
        name = row['Name']
        team = TEAM_NAME
        position = row['Position']"""
        
        # get player_id
    roster_df['player_id'] = roster_df.apply(lambda row: get_player_id(name=row['Name'], team=row['Team'], position=row['Position'], client=client, table_name=table_name),axis=1)
    
    #filter out players withough player_ids
    roster_df = roster_df[roster_df['player_id'] != "Not Found" ]
    roster_df = roster_df.fillna(0)
    roster_df = roster_df[roster_df['player_id'] != 0 ]
    
    # collect stats_dict for player
    roster_df['Stats_dict'] = roster_df.apply(lambda row: collect_player_stats_dict_from_supabase(player_id=row['player_id'], client=client, table_name=table_name), axis=1)
    print('roster df before removing records without player stats dictionaries')
    print(roster_df)
    
    # remove records without player stats dictionaries
    idx_to_remove = []
    for index, row in roster_df.iterrows():
        print(row['Stats_dict'])
        if row['Stats_dict'] == None:
            idx_to_remove.append(index)
        else:
            pass
    
    roster_df = roster_df[~(roster_df.index.isin(idx_to_remove))]
    
    #print(roster_df)
    # unpack the stats dictionary to make the dataframe look like the roster_stats_df I am using now
    # get player stats from cfg
    PLAYER_STATS = [stat for stat in meta_data_dict if meta_data_dict[stat]['stat_level'] == 'player']

    # unpack the stats from the roster_df
    for stat in PLAYER_STATS:
        roster_df[stat] = roster_df.apply(lambda row: undict(row['Stats_dict'][0])[stat],axis=1)
        
    # rename positions
    roster_df = rename_positions(roster_df)
    #print(roster_df)
    
    # then calculate team efficiencies 
    team_eff_stats_dict = calc_team_effieciencies(meta_data_dict,roster_df)
    
    # return the team_eff_stats_dict
    return team_eff_stats_dict

def get_player_id(name, team, position, client, table_name):
    # now we check if the player has match verification

    response = client.table(table_name).select('*').eq('Player Name', name).execute()
    
    # collect correct player id
    player_id = 0
    # the player name is unique this should pass easily
    # sorting this out with the case Josh Allen
    print(name, team, position)
    try:
        if (response.data[0]['Player Name'] == name) and (response.data[0]['Team'] == team) and (response.data[0]['ESPN Roster Position'] == position):
            player_id = response.data[0]['id']
            return int(player_id)
        elif (response.data[1]['Player Name'] == name) and (response.data[1]['Team'] == team) and (response.data[1]['ESPN Roster Position'] == position):
            player_id = response.data[1]['id']
            return int(player_id)
        else:
            return "Not Found"    
    except:
        return "Not Found" 
        
def collect_player_stats_dict_from_supabase(player_id, client, table_name):
    try:
        response = client.table(table_name).select('*').eq('id', int(player_id)).execute()
        return response.data[0]['stats_dict']
    except:
        return "Not found"
    
# rename columns
columns_to_rename = {
    'HOME_CB/SS_tackle_eff':'HOME_CB_SS_tackle_eff',
    'HOME_CB/SS_QB_pressure_eff':'HOME_CB_SS_QB_pressure_eff',
    'HOME_CB/SS_Turnover_eff':'HOME_CB_SS_Turnover_eff',
    'HOME_CB/SS_PassesDefended_eff':'HOME_CB_SS_PassesDefended_eff',
    'AWAY_CB/SS_tackle_eff':'AWAY_CB_SS_tackle_eff', 
    'AWAY_CB/SS_QB_pressure_eff':'AWAY_CB_SS_QB_pressure_eff',
    'AWAY_CB/SS_Turnover_eff':'AWAY_CB_SS_Turnover_eff',
    'AWAY_CB/SS_PassesDefended_eff':'AWAY_CB_SS_PassesDefended_eff',
    'HOME_DE/LB_tackle_eff':'HOME_DE_LB_tackle_eff',
    'HOME_DE/LB_QB_pressure_eff':'HOME_DE_LB_QB_pressure_eff',
    'HOME_DE/LB_Turnover_eff':'HOME_DE_LB_Turnover_eff',
    'HOME_DE/LB_PassesDefended_eff':'HOME_DE_LB_PassesDefended_eff', 
    'AWAY_DE/LB_tackle_eff':'AWAY_DE_LB_tackle_eff',
    'AWAY_DE/LB_QB_pressure_eff':'AWAY_DE_LB_QB_pressure_eff',
    'AWAY_DE/LB_Turnover_eff':'AWAY_DE_LB_Turnover_eff',
    'AWAY_DE/LB_PassesDefended_eff':'AWAY_DE_LB_PassesDefended_eff'    
}

stat_list = [
    'key',
    'HOME_QB_eff', 'AWAY_QB_eff',
    'HOME_WR_eff', 'AWAY_WR_eff',
    'HOME_RB_eff', 'AWAY_RB_eff',
    'HOME_TE_eff', 'AWAY_TE_eff',
    'AwayTotal_eff', 'HomeTotal_eff',
    'delta_eff', 'eff_total',
    'delta_QB_eff', 'total_QB_eff', 'delta_RB_eff', 'delta_TE_eff', 'delta_WR_eff',
    'HOME_CB_SS_tackle_eff', 'AWAY_CB_SS_tackle_eff',
    'HOME_CB_SS_QB_pressure_eff','AWAY_CB_SS_QB_pressure_eff',
    'HOME_CB_SS_Turnover_eff', 'AWAY_CB_SS_Turnover_eff',
    'HOME_CB_SS_PassesDefended_eff', 'AWAY_CB_SS_PassesDefended_eff',
    'HOME_DE_LB_tackle_eff', 'AWAY_DE_LB_tackle_eff',
    'HOME_DE_LB_QB_pressure_eff', 'AWAY_DE_LB_QB_pressure_eff',
    'HOME_DE_LB_Turnover_eff', 'AWAY_DE_LB_Turnover_eff',
    'HOME_DE_LB_PassesDefended_eff', 'AWAY_DE_LB_PassesDefended_eff'
    ]


def sum_up_efficiency_for_defensive_stats(efficiency_dict, stat):
    """
    accepts an efficiency dictionary in the following form and returns a sum of the 3 components. 
    
    {
      'PassingYardsEfficiency': nan,
      'RushingYardsEfficiency': nan,
      'ReceivingYardsEfficiency': nan,
      'SoloTacklesEfficiency': 0.06457739791073125,
      'AssistedTacklesEfficiency': 0.027856916745805635,
      'PassesDefendedEfficiency': 0.008547008547008548,
      'SacksEfficiency': 0.0,
      'QBHitsEfficiency': 0.0015827793605571383,
      'InterceptionsEfficiency': 0.001774622892635315,
      'ForcedFumblesEfficiency': 0.0006331117442228553
    }
    """
    if stat == 'tackle':
        sum_of_efficiencies = efficiency_dict['AssistedTacklesEfficiency'] + efficiency_dict['SoloTacklesEfficiency']
    elif stat == 'QB_pressure':
        sum_of_efficiencies = efficiency_dict['SacksEfficiency'] + efficiency_dict['QBHitsEfficiency']
    elif stat == 'Turnover':
        sum_of_efficiencies = efficiency_dict['ForcedFumblesEfficiency'] + efficiency_dict['InterceptionsEfficiency'] + efficiency_dict['PassesDefendedEfficiency']
    elif stat == 'PassesDefended':
        sum_of_efficiencies = efficiency_dict['PassesDefendedEfficiency']
    
    return sum_of_efficiencies

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

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    # Initialize the Supabase client
    client = supabase.Client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    currentSeason = get_nfl_season()
    currentWeek = convert_date_to_nfl_week(dt.now())

    # get the schedule
    # get the schedule
    schedule = read_schedule_from_supabase(currentSeason, currentWeek, client)

    # get efficiency stats
    start = dt.now()
    schedule['HomeTeamEfficiencies'] = schedule.apply(lambda row: collect_and_calc_team_efficiencies_v2(row['HomeTeam'], client, meta_data_dict), axis=1)
    schedule['AwayTeamEfficiencies'] = schedule.apply(lambda row: collect_and_calc_team_efficiencies_v2(row['AwayTeam'], client, meta_data_dict), axis=1)
    print('collected team efficiencies in', dt.now() - start)

    import antelope_utils.feature_engineering_utils
    importlib.reload(antelope_utils.feature_engineering_utils)
    from antelope_utils.feature_engineering_utils import sum_up_efficiency_stats

    for position in ['QB', 'WR', 'RB', 'TE']:
        for home_or_away in ['HOME','AWAY']:
            
            col_name = home_or_away + '_' + position + '_eff'
            
            if home_or_away == 'HOME':
                dict_col_name = 'HomeTeamEfficiencies'
            else:
                dict_col_name = 'AwayTeamEfficiencies'
            
            schedule[col_name] = schedule.apply(lambda row: sum_up_efficiency_stats(row[dict_col_name][position]), axis = 1)

    schedule['AwayTotal_eff'] = schedule['AWAY_QB_eff']+ schedule['AWAY_RB_eff']+ schedule['AWAY_TE_eff']+ schedule['AWAY_WR_eff']
    schedule['HomeTotal_eff'] = schedule['HOME_QB_eff']+ schedule['HOME_RB_eff']+ schedule['HOME_TE_eff']+ schedule['HOME_WR_eff']

    schedule['delta_eff'] = schedule['HomeTotal_eff'] - schedule['AwayTotal_eff']
    schedule['eff_total'] = schedule['HomeTotal_eff'] + schedule['AwayTotal_eff']

    schedule['delta_QB_eff'] = schedule['HOME_QB_eff'] - schedule['AWAY_QB_eff']
    schedule['total_QB_eff'] = schedule['HOME_QB_eff'] + schedule['AWAY_QB_eff']
    schedule['delta_RB_eff'] = schedule['HOME_RB_eff'] - schedule['AWAY_RB_eff']
    schedule['delta_TE_eff'] = schedule['HOME_TE_eff'] - schedule['AWAY_TE_eff']
    schedule['delta_WR_eff'] = schedule['HOME_WR_eff'] - schedule['AWAY_WR_eff']
    
    for position in ['CB/SS', 'DE/LB']:
        for home_or_away in ['HOME','AWAY']:
            for stat in ['tackle','QB_pressure', 'Turnover', 'PassesDefended']:
            
                col_name = home_or_away + '_' + position + '_'+ stat + '_eff'

                if home_or_away == 'HOME':
                    dict_col_name = 'HomeTeamEfficiencies'
                else:
                    dict_col_name = 'AwayTeamEfficiencies'

                schedule[col_name] = schedule.apply(lambda row: sum_up_efficiency_for_defensive_stats(row[dict_col_name][position], stat), axis = 1)
        
    schedule['Wind'] = schedule.apply(lambda row: get_wind_speed_v2(row['HomeTeam'],'f8804c3a6dd41e1040548277851eca43'), axis=1)
    
    # Rename the 'OldName' column to 'NewName'
    schedule = schedule.rename(columns=columns_to_rename)

    for index, row in schedule.fillna(0).iterrows():
        table_name = 'PlayerEfficiencies_matchup'
        
        data_to_insert = {}
        #data_to_insert['HomeTeamEfficiencies'] = row['HomeTeamEfficiencies']
        #data_to_insert['AwayTeamEfficiencies'] = row['AwayTeamEfficiencies']
        
        for stat in stat_list:
            data_to_insert[stat] = row[stat]
            
        # check if record is already in table
        response_check = client.from_(table_name).select("*").eq('key', row['key']).execute()
        #print(row['key'], "Home QB Eff:", response_check.data[0]['HOME_QB_eff'])

        # if there is no record, write the record
        if response_check.data == []:
            response_write = client.from_(table_name).insert([data_to_insert]).execute()
        else:
            # else update the record
            response_update = client.table(table_name).update([data_to_insert]).eq("key", row['key']).execute()
            
        # update wind
        response = client.from_('Schedule').update({'Wind':row['Wind']}).eq("key", row['key']).execute()
           
    
    """Minimal stub that confirms the Lambda executed."""
    return {
        "statusCode": 200,
        "body": "create-team-efficiencues completed successfully.",
    }

if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({}, None)