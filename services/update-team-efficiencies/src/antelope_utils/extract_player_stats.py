import sys
sys.path.append("/antelope_utils")
from datetime import datetime as dt
from datetime import datetime
from datetime import date
from collections.abc import Mapping, Sequence
import pandas as pd
import http.client, urllib.request, urllib.parse, urllib.error, base64
import json
from antelope_utils.extract_roster_utils import check_status
import time
from random import randrange
from antelope_utils.player_scraping_utils import get_player_metadata, verify_player_data_source

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

def load_player_stats(currentSeason, verbose, API_KEY):
    """
    Loads player stats from last two years for each player
    """
    start = dt.now()
    LASTYEARPLAYERSTATS = get_player_season_stats(currentSeason - 1, API_KEY)
    THISYEARPLAYERSTATS = get_player_season_stats(currentSeason, API_KEY)
    
    lastSeasonStats = tabulate_player_stats(LASTYEARPLAYERSTATS)
    thisSeasonStats = tabulate_player_stats(THISYEARPLAYERSTATS)
    
    try: totalStats = lastSeasonStats.append(thisSeasonStats)
    except: totalStats = lastSeasonStats
        
    # print the time it takes if verbose 
    if verbose:
        print('Loaded Player Stats in:',dt.now() - start)   
        
    return totalStats

def tabulate_player_stats(LASTYEARPLAYERSTATS):
    df = pd.DataFrame()
    for I in range(0,len(LASTYEARPLAYERSTATS)):
        df = df.append({

            'PlayerID':LASTYEARPLAYERSTATS[I]['PlayerID'],
            'Name':LASTYEARPLAYERSTATS[I]['Name'],
            'Team': LASTYEARPLAYERSTATS[I]['Team'],
            #'Opponent': LASTYEARPLAYERSTATS[I]['Opponent'],
            #'HomeOrAway':LASTYEARPLAYERSTATS[I]['HomeOrAway'],
            'Season':LASTYEARPLAYERSTATS[I]['Season'],
            'SeasonType':LASTYEARPLAYERSTATS[I]['SeasonType'],
            #'Week':LASTYEARPLAYERSTATS[I]['Week'],
            'PassingYards':LASTYEARPLAYERSTATS[I]['PassingYards'],
            'RushingYards':LASTYEARPLAYERSTATS[I]['RushingYards'],
            'RushingAttempts':LASTYEARPLAYERSTATS[I]['RushingAttempts'],
            'ReceivingYards':LASTYEARPLAYERSTATS[I]['ReceivingYards'],
            'Position':LASTYEARPLAYERSTATS[I]['Position'],
            'Activated':LASTYEARPLAYERSTATS[I]['Activated'],
            'Played':LASTYEARPLAYERSTATS[I]['Played'],
            'Started':LASTYEARPLAYERSTATS[I]['Started'],

            'PassingTouchdowns':LASTYEARPLAYERSTATS[I]['PassingTouchdowns'],
            'PassingRating':LASTYEARPLAYERSTATS[I]['PassingRating'],
            'PassingSacks':LASTYEARPLAYERSTATS[I]['PassingSacks'],
            'PassingSackYards':LASTYEARPLAYERSTATS[I]['PassingSackYards'],
            'PassingInterceptions':LASTYEARPLAYERSTATS[I]['PassingInterceptions'],

            #'FantasyDraftSalary': LASTYEARPLAYERSTATS[I]['FantasyDraftSalary'],
            'ReceivingYardsPerReception': LASTYEARPLAYERSTATS[I]['ReceivingYardsPerReception'],
            'ReceivingTouchdowns': LASTYEARPLAYERSTATS[I]['ReceivingTouchdowns'],

            'RushingYardsPerAttempt': LASTYEARPLAYERSTATS[I]['RushingYardsPerAttempt'],
            'RushingTouchdowns':LASTYEARPLAYERSTATS[I]['RushingTouchdowns'],
            'RushingLong': LASTYEARPLAYERSTATS[I]['RushingLong'],
            'ReceivingTargets': LASTYEARPLAYERSTATS[I]['ReceivingTargets'],
            'Receptions': LASTYEARPLAYERSTATS[I]['Receptions'],
            'OffensiveSnapsPlayed':LASTYEARPLAYERSTATS[I]['OffensiveSnapsPlayed'],

            ### Defense
            'DefensiveSnapsPlayed' : LASTYEARPLAYERSTATS[I]['DefensiveSnapsPlayed'],
            'SoloTackles' : LASTYEARPLAYERSTATS[I]['SoloTackles'],
            'AssistedTackles' : LASTYEARPLAYERSTATS[I]['AssistedTackles'],
            'Sacks' : LASTYEARPLAYERSTATS[I]['Sacks'],
            'SackYards' : LASTYEARPLAYERSTATS[I]['SackYards'],
            'QuarterbackHits' : LASTYEARPLAYERSTATS[I]['QuarterbackHits'],
            'MiscSoloTackles' : LASTYEARPLAYERSTATS[I]['MiscSoloTackles'],
            'SpecialTeamsSoloTackles' : LASTYEARPLAYERSTATS[I]['SpecialTeamsSoloTackles'],
            'Tackles' : LASTYEARPLAYERSTATS[I]['Tackles'],
            'FumblesForced' : LASTYEARPLAYERSTATS[I]['FumblesForced'],
            'DefensiveTeamSnaps' : LASTYEARPLAYERSTATS[I]['DefensiveTeamSnaps'],
            'PassesDefended' : LASTYEARPLAYERSTATS[I]['PassesDefended'],
            'Interceptions' : LASTYEARPLAYERSTATS[I]['Interceptions'],

            ### Kickers
            'PuntYards' : LASTYEARPLAYERSTATS[I]['PuntYards'],
            'PuntNetYards' : LASTYEARPLAYERSTATS[I]['PuntNetYards'],
            'FieldGoalsAttempted' : LASTYEARPLAYERSTATS[I]['FieldGoalsAttempted'],
            'FieldGoalsMade' : LASTYEARPLAYERSTATS[I]['FieldGoalsMade'],
            'FieldGoalsLongestMade' : LASTYEARPLAYERSTATS[I]['FieldGoalsLongestMade'],
            'FieldGoalsMade0to19' : LASTYEARPLAYERSTATS[I]['FieldGoalsMade0to19'],
            'FieldGoalsMade20to29' : LASTYEARPLAYERSTATS[I]['FieldGoalsMade20to29'],
            'FieldGoalsMade30to39' : LASTYEARPLAYERSTATS[I]['FieldGoalsMade30to39'],
            'FieldGoalsMade40to49' : LASTYEARPLAYERSTATS[I]['FieldGoalsMade40to49'],
            'FieldGoalsMade50Plus' : LASTYEARPLAYERSTATS[I]['FieldGoalsMade50Plus'],

            },ignore_index=True)

    return df

def clean_level_0(df):

    new_level_0_list = []
    new_level_1_list = []

    for col_name in list(df):   
        # rename unnamed level to level 0
        if 'Unnamed:' in col_name[0]:
            df = df.rename(
                level = 0,
                columns = {
                    col_name[0]:'level_0'})

        # rename unnamed level to level 0
        if 'Unnamed:' in col_name[1]:
            df = df.rename(
                level = 1,
                columns = {
                    col_name[1]:'level_1'})

    for col_name in list(df):
        # check for sacks and reassign them to times sacked
        if (col_name[0] == 'Passing') and (col_name[1] == 'Sk'):
            print('renaming Sk to times sacked', col_name)
            df = df.rename(
                level = 1,
                columns = {
                    col_name[1]:'Sacks'})

    for col_name in list(df):   
        # check for sacks and reassign them to tackles
        if (col_name[0] == 'level_0') and (col_name[1] == 'Sacks'):
            print('reassigning sacks to tackles', col_name)
            df[('Tackles','Sacks')] = df[('level_0','Sacks')]
            df = df.drop(('level_0','Sacks'),axis=1)
            
    
    return df

def remove_player_went_from(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Removes rows where the given column starts with 'Player went from '.

    Args:
        df (pd.DataFrame): Input DataFrame
        column (str): Column name to check for the pattern

    Returns:
        pd.DataFrame: Cleaned DataFrame with rows removed
    """
    print("extract_player_stats: removing player went from")
    mask = ~df[column].str.startswith("Player went from ", na=False)
    return df[mask].reset_index(drop=True)

def clean_player_log(df, meta_data_dict, position):
    """
    Takes a player log from football reference anc returns a cleaned data frame
    
    Params:
    -------
    df: datframe
        scraped dataframe from football reference
    meta_data_dict: dict
        meta data dictionary loaded from config file
    position: str
        
    Returns:
    --------
    df: dataframe
        cleaned dataframe
    """
    print("extract_player_stats:" )
    STATS_LIST = list(meta_data_dict.keys())

    # clean level 0
    print('extract_player_stats: cleaning level 0 from player stats df')
    df = clean_level_0(df)
    print('extract_player_stats: cleaned level 0 from player stats df')
    
    # drop unnamed col
    if 'Unnamed: 0' in list(df):
        df = df.drop('Unnamed: 0', axis=1)

    # fill null values
    df = df.fillna(0)
    
    # remove headers mid-table
    df = df[df[('level_0','Date')] != 'Date']

    print(list(df))

    # rename columns
    print('extract_player_stats: renaming columns')
    #df = rename_player_log_columns(df)

    # filter out games where player is inactive
    print('extract_player_stats: removing inactive games')
    df = df[df[('Snap Counts', 'OffSnp')] != "Inactive"]

    # filter out games where player didnot play
    print('extract_player_stats: removing Did not play')
    df = df[df[('Snap Counts', 'OffSnp')] != 'Did Not Play']

    # filter out games where player on injured reserve
    df = df[df[('Snap Counts', 'OffSnp')] != 'Injured Reserve']
    
    # filter out games where the player was suspended
    df = df[df[('Snap Counts', 'OffSnp')] != 'Suspended']

    # filter out games where the player was on COVID-19 List
    df = df[df[('Snap Counts', 'OffSnp')] != 'COVID-19 List']

    # filter out games where the player was on 'Exempt List' List
    df = df[df[('Snap Counts', 'OffSnp')] != 'Exempt List']
    print('extract_player_stats: removed all inactive game types')


    # filter out the totals at bottom okeysf the table
    df = df[df[('level_0','Team')] != 0]
    print('extract_player_stats: removed all inactive game types')


    # now we can fill na with 0s
    df = df.fillna(0)

    # where needed, convert columns to floats
    print('extract_player_stats: where needed, convert columns to floats')
    print(STATS_LIST)
    for stat in STATS_LIST: 
        print(stat)
        # check if this data type should be a float
        if meta_data_dict[stat]['type'] == 'float':
            level_0 = meta_data_dict[stat]['player_log_level_0']
            level_1 = meta_data_dict[stat]['player_log_level_1']
            #print(df[(level_0,level_1)])
            # Remove player transfer
            
            #print(df[(level_0,level_1)].unique())
            
            # check if data is in dataframe
            if (level_0, level_1) in list(df):
                print("extract_player_stats: remove player transfer")
                df = remove_player_went_from(df, (level_0,level_1))
                print(df)
                try:
                    df[(level_0,level_1)] = df[(level_0,level_1)].astype(float)
                    print("extract_player_stats:",  level_0, level_1, "are in df and converted to float")
                except:
                    print('extract_player_stats: issue converting', level_0, level_1, 'to float')
                    #df[(level_0,level_1)] = 0
            # else we'll just add 0s
            else:
                df[(level_0, level_1)] = 0
    print('extract_player_stats: returning cleaned player log')
    return df

def refresh_player_info(name, team, position, currentSeason, client, meta_data_dict, status):
    table_name = 'PlayerMetaData'
    
    # check if player exists in the player table
    response = client.table(table_name).select('*').eq('Player Name', name).execute()
    
    # if there is no player data we verify the player and then write to the data base
    if response.data == []:
        print('player name not in PlayerMetaData table')
        write_dict = {
            'Player Name' : name,
            'Team' : team,
            'ESPN Roster Position': position,
            'Status': status,
            'updated_at': dt.now(),
        }
        response = client.from_(table_name).insert(to_jsonable(write_dict)).execute()

    # other wise we pass    
    else:
        print('Name found in PlayerMetaData table')
        pass

    # now we check if the player has match verification
    response = client.table(table_name).select('*').eq('Player Name', name).execute()
    # collect correct player id
    player_id = 0
    player_confirmed = False
    for record in response.data:
        # the player name is unique this should pass easily
        # sorting this out with the case Josh Allen
        if (record['Player Name'] == name) and (record['Team'] == team) and (record['ESPN Roster Position'] == position):
            player_id = record['id']
            print("Player Identified! player_id:", player_id)
            break
        else:
            pass 
    
    # if no id was found
    if player_id == 0:
        write_dict = {
                'Player Name' : name,
                'Team' : team,
                'ESPN Roster Position': position,
                'Status': status,
                'updated_at': dt.now(),
                }
        response = client.from_(table_name).insert(to_jsonable(write_dict)).execute() 
        print(response)
        player_id = response.data[0]['id']

    print("using player_id:", player_id)
    # use player id to check specific record
    response = client.table(table_name).select('*').eq('id', player_id).execute()

    if response.data[0]['match_verification'] == True:
        print("player already verified")
        pass
    else:
        print('need to verify player')
        player_dict = verify_player(name=name, position=position, team=team)

        update_dict = {
            "Football Reference URL" : player_dict['url'],
            "match_verification" : player_dict['match_verification'],
            'NFL Reference Position': player_dict['Position'],
            'updated_at': dt.now(),

        }
        response_update = client.table(table_name).update(to_jsonable(update_dict)).eq("id", player_id).execute()

    # if player is verifud
    # use player id to check specific record
    response = client.table(table_name).select('*').eq('id', player_id).execute()
    player_url = response.data[0]["Football Reference URL"]
    player_position = position

    if response.data[0]['match_verification'] == True:
        print("loading player log for", name)
        player_stats_dict, full_player_log = add_stats_for_player(url=player_url, currentSeason=currentSeason, position=player_position, meta_data_dict=meta_data_dict)
        # updating player record with player stats for calculation and their complete player log
        update_dict = {
            'stats_dict' : [player_stats_dict],
            'full_player_log': [full_player_log.to_json(orient='records')],
            'updated_at': dt.now(),
        }
        response_update = client.table(table_name).update(to_jsonable(update_dict)).eq("id", player_id).execute()

    return "player stats loaded successfully"

def verify_and_extract_player_data_v2(roster_df, currentSeason, currentWeek, meta_data_dict, client):
    """
    ***Complex***
    The function works in a number of stages iterationg through a roster_df generated by
    create_roster function in extract_roster_utils
        1. checks player status (eg: healthy, suspended, etc.) and cleans the player names
        2. checks if the player name is in the table
        3. if there is no player data in the table, write the basic data --> else moves to #4
            - player name, team, position from espn, status
        4. Check if the player has been verified yet, it not verify the player. 
            - this is a time consuming process of identifying the correct url to scrape the
            game logs from off of football reference
        5. update the player record with the player stats dictionary needed for calculation team efficiencies
        as well as the full_player_log which can be converted back to a dataframe (just use pd.read_json() )
    
    """
    table_name = 'PlayerMetaData'

    # iterate through the roster and check if the player is in the table and verified
    # if the player is in the table and verified, we can collect the player stats
    # other wise we need to verify the player and update the table
    for index, row in roster_df.iterrows():
        
        time.sleep(3)

        #player name
        name = row['Name']
        # remove player status from name
        status = check_status(name)
        print("Verifying:", name, 'Team', row['Team'], 'Position', row['Position'])
        name = clean_player_name(name)
        

        # check if player exists in the player table
        response = refresh_player_info(
            name=name,
            team=row['Team'],
            position=row['Position'],
            currentSeason=currentSeason,
            client=client,
            meta_data_dict=meta_data_dict,
            status=status
        )
        print(response)

    return "player stats loaded successfully"

def clean_player_name(name):

    # remove injury status from name
    status = check_status(name)

    if status == "Healthy":
        name = name
    else:
        # remove injury status

        # Split the sentence into words
        words = name.split()

        # Remove the last word by slicing the list of words
        words_without_last = words[:-1]

        # Join the remaining words back into a sentence
        new_sentence = ' '.join(words_without_last)

        name = new_sentence

    return name



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
