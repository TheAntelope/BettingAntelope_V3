import http.client, urllib.request, urllib.parse, urllib.error, base64
import sys
sys.path.append("/antelope_utils")
import json
from datetime import datetime as dt
import pandas as pd
from antelope_utils.player_stats import calc_team
from antelope_utils.s3_utils import fetch_pkl_from_s3
import boto3
from antelope_utils.data_prep_utils import undict
from antelope_utils.data_prep_utils import rename_team_str
import time
import math

def get_roster_stats_for_team(team, season, week, meta_data_dict):
    """
    for a given team, gets the latest verified roster and collects team stats
    NOTE: Need to use pandas==1.3.5
    
    Params:
    -------
        team: str
        season: int or str
        week: into or str
        meta_data_dict: dict
        
    Returns:
    --------
        roster_stats_df: df
        
    """
    # get latest roster path from s3
    key = get_latest_verified_roster(team, 2022, 1)

    # fetch roster and return a df
    roster_df = fetch_pkl_from_s3('antelope', key)

    # calc team
    roster_stats_df = calc_team(team=team, currentSeason=season, roster_df=roster_df, meta_data_dict=meta_data_dict)

    # get player stats from cfg
    PLAYER_STATS = [stat for stat in meta_data_dict if meta_data_dict[stat]['stat_level'] == 'player']

    # unpack the stats from the roster_df
    for stat in PLAYER_STATS:
        roster_stats_df[stat] = roster_stats_df.apply(lambda row: undict(row['Stats_dict'])[stat],axis=1)

    return roster_stats_df

def get_latest_verified_roster(team, season, week):
    """
    Goes to s3 and collects the latest roster and returns the path to the latest depth chart for a team
    
    Params:
    -------
        team: str
        season: int or str
        week: inot or str
        
    Returns:
    --------
        last_added_roster:str
    
    """

    # get latest roster_df for a team
    s3 = boto3.client('s3')
    key = 'dev/ExtractedStats/DepthCharts/Verified/' + str(season) + '/' + str(week) + '/' + team + '/'
    # get list of objects in the antelope bucket
    objs = s3.list_objects_v2(Bucket='antelope', Prefix=key)['Contents']
    # get list of objects in the depth charts directory
    relevent_objs = [obj for obj in objs if key in obj['Key'] ]
    # define a function to get the last modition
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    # get the last added file
    last_added_roster = [obj['Key'] for obj in sorted(relevent_objs, key=get_last_modified)][-1]

    return last_added_roster

def rename_team_for_football_reference(team):
    
    if team == 'San Diego Chargers':
        team = 'LAC'
    
    # make lowercase
    team = team.lower()
    
    if team == 'kc':
        return "kan"
    elif team == 'lar':
        return 'ram'
    elif team == 'hou':
        return 'htx'
    elif team == 'ari':
        return 'crd'
    elif team == 'ten':
        return 'oti'
    elif team == 'lac':
        return 'sdg'
    elif team == 'no':
        return 'nor'
    elif team == 'sf':
        return 'sfo'
    elif team == 'lv':
        return 'rai'
    elif team == 'ind':
        return 'clt'
    elif team == 'ne':
        return 'nwe'
    elif team == 'bal':
        return 'rav'
    elif team == 'gb':
        return 'gnb'
    elif team == 'tb':
        return 'tam'
    else:
        return team

def collect_season_team_stats(team, season, week):
    print('Collecting Season team stats:')
    print("Sleeping for 7s")
    time.sleep(7)
    print('Team', team, 'Season', season, 'Week', week)
    team_renamed = rename_team_for_football_reference(team)
    
    url = 'https://www.pro-football-reference.com/teams/'+team_renamed+'/'+str(season)+'/gamelog/'
    print(url)

    # read team stats table from football reference
    df = pd.read_html(url)
    time.sleep(5)

    # the 2nd in the list is the schedule and game results
    season_df = df[0] #0 if collecting regular season games, 1 if collecting playoffs games
    season_df = season_df.fillna(0)
    print(list(season_df))
    print(season_df)
    season_df = season_df[season_df['Unnamed: 2_level_0', 'Week'] <= week]
    schedule_dict = {}
    season_df = season_df.fillna(0)
    if week == 1:
        stats_dict = {
            'PointScored' : 0,
            'PointAllowed' : 0,
            'PassingTDs' : 0,
            'RushingTDs' : 0,
            'TotalTDs' : 0,
            'YardsFor': 0,
            'YardsAgainst': 0,
            'schedule': schedule_dict
        }
    else:
        # opponent df
        if len(df) == 2:
            # handles he case where a team did not make the playoffs
            opponent_df = df[1]
            opponent_df[opponent_df['Unnamed: 2_level_0', 'Week'] < week]
        else: 
            opponent_df = df[2]
            opponent_df[opponent_df['Unnamed: 2_level_0', 'Week'] < week]
            
        passing_TDs = season_df['Passing','TD'].sum()
        rushing_TDs = season_df['Rushing','TD'].sum()
        points_scored = season_df['Score','Pts'].sum()
        points_scored_list = [i for i in season_df['Score','Pts']]
        points_allowed = season_df['Score','PtsO'].sum()
        points_allowed_list = [i for i in season_df['Score','PtsO']]
        opponents = season_df['Unnamed: 6_level_0', 'Opp']
        weeks = season_df['Unnamed: 2_level_0', 'Week']
        passing_yards_for_list = [i for i in season_df['Passing','Yds'] if not math.isnan(i)]
        passing_yards_against_list = [i for i in opponent_df['Passing','Yds'] if not math.isnan(i)]
        rushing_yards_for_list = [i for i in season_df['Rushing','Yds'] if not math.isnan(i)]
        rushing_yards_against_list = [i for i in opponent_df['Rushing','Yds'] if not math.isnan(i)]
        dates = [i for i in season_df['Unnamed: 3_level_0', 'Date']]
    
        # create schedule dictionary
        print("extract_team_stats_utils: creating schedule dict for weeks", weeks)
        for i in range(0, len(weeks)-1):
            print("extract_team_stats_utils:", i)
            schedule_dict[weeks[i]] = {}
            schedule_dict[weeks[i]]['Opponent'] = rename_team_str(opponents[i])
            schedule_dict[weeks[i]]['Score'] = points_scored_list[i]
            schedule_dict[weeks[i]]['OpponentScore'] = points_allowed_list[i]
            schedule_dict[weeks[i]]['PassingYardsFor'] = passing_yards_for_list[i] 
            schedule_dict[weeks[i]]['PassingYardsAgainst'] = passing_yards_against_list[i] 
            schedule_dict[weeks[i]]['RushingYardsFor'] = rushing_yards_for_list[i] 
            schedule_dict[weeks[i]]['RushingYardsAgainst'] = rushing_yards_against_list[i]
            schedule_dict[weeks[i]]['YardsFor'] = passing_yards_for_list[i] + rushing_yards_for_list[i]
            schedule_dict[weeks[i]]['YardsAgainst'] = passing_yards_against_list[i] + rushing_yards_against_list[i]
            schedule_dict[weeks[i]]['Date'] = dates[i]        
            
        total_TDs = passing_TDs + rushing_TDs
        
        stats_dict = {
            'PointScored' : points_scored,
            'PointAllowed' : points_allowed,
            'PassingTDs' : passing_TDs,
            'RushingTDs' : rushing_TDs,
            'TotalTDs' : total_TDs,
            'YardsFor': sum(passing_yards_for_list) + sum(rushing_yards_for_list),
            'YardsAgainst': sum(passing_yards_against_list) + sum(rushing_yards_against_list),
            'schedule': schedule_dict
        }
    print('Done collecting Team Stats.')
    return stats_dict

def collect_season_team_stats_for_one_week(team, season, week):
    print('Collecting game team stats:')
    print('Team', team, 'Season', season, 'Week', week)
    team_renamed = rename_team_for_football_reference(team)
    
    url = 'https://www.pro-football-reference.com/teams/'+team_renamed+'/'+str(season)+'/gamelog/'
    print(url)

    # read team stats table from football reference
    df = pd.read_html(url)
    print("printing df...")
    print(list(df[0]))
    print(df[0])
    time.sleep(7)

    # the 2nd in the list is the schedule and game results
    season_df = df[0]
    season_df = season_df[season_df[('Unnamed: 2_level_0', 'Week')] == week]
    
    # opponent df
    if len(df) == 2:
        # handles he case where a team did not make the playoffs
        opponent_df = df[1]
        opponent_df[opponent_df[('Unnamed: 2_level_0', 'Week')] == week]
    else: 
        opponent_df = df[2]
        opponent_df[opponent_df[('Unnamed: 2_level_0', 'Week')] == week]
        
    passing_TDs = season_df['Passing','TD'].sum()
    rushing_TDs = season_df['Rushing','TD'].sum()
    points_scored = season_df['Score','Pts'].sum()
    points_scored_list = [i for i in season_df['Score','Pts']]
    points_allowed = season_df['Score','PtsO'].sum()
    points_allowed_list = [i for i in season_df['Score','PtsO']]
    opponents = season_df['Unnamed: 6_level_0', 'Opp']
    weeks = season_df['Unnamed: 2_level_0', 'Week']
    passing_yards_for_list = [i for i in season_df['Passing','Yds']]
    passing_yards_against_list = [i for i in opponent_df['Passing','Yds']]
    rushing_yards_for_list = [i for i in season_df[('Rushing','Yds')]]
    rushing_yards_against_list = [i for i in opponent_df['Rushing','Yds']]
    sacksAllowed_list = [i for i in season_df['Passing','Sk']]
    
    total_TDs = passing_TDs + rushing_TDs
        
    stats_dict = {
        'PointScored' : points_scored,
        'PointAllowed' : points_allowed,
        'PassingTDs' : passing_TDs,
        'RushingTDs' : rushing_TDs,
        'TotalTDs' : total_TDs,
        'RushingYardsFor': sum(rushing_yards_for_list),
        'PassingYardsFor': sum(passing_yards_for_list),
        'SacksAllowed': sum(sacksAllowed_list)
    }
    
    return stats_dict

def load_stat_from_OU_table(
    stat,
    team,
    season,
    OUHist):

    try:
        df = OUHist[
                (OUHist['Team'] == team)
                & (OUHist['Season'] == season)
        ]
        
        val = df[stat].values[0]

        return val
    except:
        print('Broken Futures fetch. returning 8.5 for last year. If its the start of the season, you may need to go collect the latest season and append it to the futures table.')
        return 8.5