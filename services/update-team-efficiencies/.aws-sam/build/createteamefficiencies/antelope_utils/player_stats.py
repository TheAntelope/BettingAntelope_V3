from datetime import datetime as dt
import pandas as pd
# append utils to sys path
import sys
sys.path.append("/antelope_utils")
from antelope_utils.extract_player_stats import clean_player_log
#from antelope_utils.feature_engineering_utils import create_efficiency_stats
from antelope_utils.player_scraping_utils import get_player_metadata, verify_player_data_source
import time
from random import randrange

### DECOMMISIONING ###
### MOVED ALL FUNCTIONS TO EXTRACT_PLATYER_STATS ###

def nfl_season_from_date(date_str: str) -> int:
    """
    Convert a date string (YYYY-MM-DD) to the NFL season year.
    NFL season is defined by the year in which the regular season began (September).
    """
    date = dt.strptime(date_str, "%Y-%m-%d")
    
    # If month is January–July, season belongs to previous year
    if date.month < 8:
        return date.year - 1
    else:
        return date.year

def verify_player(name, position, team):
    "takes player, team and position details and verifies them against the details on football reference"
    
    match_verification = False
    increment = 0
    
    # if the match cannot be verified
    while match_verification == False:

        # look up player in table. if the player exists, use his football reference URL. 
        sleep_time = randrange(8, 14)
        print("Sleep Time:", sleep_time)
        time.sleep(sleep_time)
        number_str = '0' + str(increment)
        increment += 1
        # get player meta data
        player_dict = get_player_metadata(name, number_str)

        # verify the scrape
        match_verification = verify_player_data_source(
            roster_name=name, roster_position=position, roster_team=team, 
            scraped_name=player_dict['Name'], scraped_position=player_dict['Position'], scraped_team=player_dict['Team'])[0]
        
        player_dict['match_verification'] = match_verification

        # break the while loop just in case
        if increment > 12:
            player_dict['Name'] = None
            player_dict['Position'] = None
            player_dict['Team'] = None
            print('Player Not Verified:', team, name, position)
            break
            
    return player_dict

### Calc team
def calc_team(team, currentSeason, roster_df, meta_data_dict):
    
    """
    returns a calculated dictionary with the following calculations
    - QB_ff
    - RB_eff
    - WR_eff
    - TE_eff
    
    TODO: Add the efficiency calculation
    
    Params:
    -------
    team: str
    currentSeason: int
    roster_df: dataframe
        dataframe with a team roster
    meta_data_dict: dict
        dictionary with all stats and their metadata
    
    Returns:
    --------
    calc_dict: dict
        dictionary with a values listed above for a given team
    """
    
    # TODO: Why am I providing the team and roster_df?
    
    calc_team_start = dt.now()
    calc_dict = {}
    
    PLAYER_STATS = [stat for stat in meta_data_dict if meta_data_dict[stat]['stat_level'] == 'player']
        
    # add stats to roster
    start = dt.now()
    
    roster_df['Stats_dict'] = roster_df.apply(lambda row: add_stats_for_player(url=row['url'],
                                    currentSeason = currentSeason,
                                    position=row['Position'],
                                    meta_data_dict=meta_data_dict),
                                     axis=1)
        
    print('added stats in:',dt.now() - calc_team_start)
                
    return roster_df

# moved to extract_player_stats
def add_stats_for_player(url, currentSeason, position, meta_data_dict):
    """
    - fetches player log from football reference 
    - collects stats of interest
    - returns stats dictionary for undicting later
     
    Params:
    -------
    url: str
        url to player log
    stats_list: list
    currentSeason: int
    position: str
    
    Returns:
    --------
    stats_dict: dict
        stats dictionary for undicting later
    
    """
    print(url)
    stats_dict = {}
    PLAYER_STATS = [stat for stat in meta_data_dict if meta_data_dict[stat]['stat_level'] == 'player']
    PLAYER_LOG_EMPTY = False
    
    # get player log
    player_log = get_player_log(url, position, meta_data_dict)
    full_player_log = player_log
    
    # the player log isnt empyt then we will filter. IF not, no need
    if player_log.empty == False:
        print('player_stats: player log not empty!')
        # only include this year and last year in the player log
        player_log[('level_0','Year')] = player_log.apply(lambda row: nfl_season_from_date(date_str=row[('level_0','Date')]),axis=1)
        player_log = player_log[player_log[('level_0','Year')] >= (currentSeason - 1)]
        print(player_log)
    else:
        # set player log empty to true
        PLAYER_LOG_EMPTY = True
        # esle we'll just put this season in there :) 
        player_log[('level_0','Year')] = currentSeason
        
    # collect total stats for players from this year and last year
    for stat in PLAYER_STATS:
        # only player stats are required
        if meta_data_dict[stat]['stat_level'] == 'player':
        
            # get level from meta data dict for stat collection
            l0 = meta_data_dict[stat]['player_log_level_0']
            l1 = meta_data_dict[stat]['player_log_level_1']

            # if there is no player log, we'll put 0s in for the stat
            if PLAYER_LOG_EMPTY:
                stats_dict[stat] = 0
            else:
                stats_dict[stat] = player_log[(l0,l1)].sum()
                
    # convert all values to python float because supabase doesn't ike numpy float
    for key in stats_dict.keys():
        stats_dict[key] = float(stats_dict[key])
        
    return stats_dict, full_player_log

def get_player_log(url, position, meta_data_dict):
    """
    fetches player log from football reference and returns a cleaned dataframe
    """
    
    # collect player log 
    start = dt.now()
    try:
        df = pd.read_html(url)[0]
        end = dt.now()
        print('player_stats: Player log loaded:', end - start)
        # clean player log
        print('player_stats: cleaning player log...')
        df = clean_player_log(
            df=df,
            meta_data_dict=meta_data_dict,
            position = position)
        print('player_stats: player log cleaned')

        # Calculate efficiency stats
        print('player_stats: creating efficienciy df')
        df = create_efficiency_stats(df)
        print('player_stats: efficienciy df created')

        return df
    
    except:
        print('player_stats: unable to collect player log for', url, 'is this his first game?')
        df = pd.DataFrame()
        return df

def create_efficiency_stats(df):
    df[('efficiency','pass_yards_efficiency')] = df[('Passing','Yds')] / df[('Snap Counts', 'OffSnp')]
    df[('efficiency','rush_yards_efficiency')] = df[('Rushing','Yds')] / df[('Snap Counts', 'OffSnp')]
    df[('efficiency','rec_yards_efficiency')] = df[('Receiving','Yds')] / df[('Snap Counts', 'OffSnp')]
    df[('efficiency','total_yds_eff')] = df[('efficiency','pass_yards_efficiency')] + df[('efficiency','rush_yards_efficiency')] + df[('efficiency','rec_yards_efficiency')]

    df[('efficiency','OffensiveTDs')] = df[('Rushing','TD')] + df[('Receiving','TD')]
    df[('efficiency','OffensiveTouchdownEfficiency')] = df[('efficiency','OffensiveTDs')] / df[('Snap Counts', 'OffSnp')]
    df[('efficiency','OffensiveYards')] = df[('Rushing','Yds')] + df[('Receiving','Yds')]
    df[('efficiency','O_td_eff')] = df[('efficiency','OffensiveTDs')] / df[('Snap Counts', 'OffSnp')]

    df[('efficiency','tackle_efficiency')] =(df[('Tackles','Solo')] + df['Tackles','Ast']) / df[('Snap Counts', 'DefSnp')]
    df[('efficiency','sack_efficiency')] = df[('Tackles','Sk')] / df[('Snap Counts', 'DefSnp')]
    df[('efficiency','QB_pressure_eff')] = (df[('Tackles','QBHits')] + df[('Tackles','Sk')]) / df[('Snap Counts', 'DefSnp')]
    df[('efficiency','Turnover_eff')] = (df[('Def Interceptions','Int')] + df[('Fumbles','FF')] + df[('Def Interceptions','PD')]) / df[('Snap Counts', 'DefSnp')]
    df[('efficiency','PassesDefended_eff')] = df[('Def Interceptions','PD')] / df[('Snap Counts', 'DefSnp')]

    
    return df