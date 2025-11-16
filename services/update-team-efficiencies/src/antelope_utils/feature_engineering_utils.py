import sys
sys.path.append("/antelope_utils")
import pandas as pd
import numpy as np
from datetime import datetime as dt
import time
import requests
from antelope_utils.data_prep_utils import rename_positions
from antelope_utils.data_prep_utils import add_stat
from antelope_utils.data_prep_utils import undict, rename_teams
from antelope_utils.s3_utils import fetch_pkl_from_s3
from antelope_utils.extract_team_stats_utils import collect_season_team_stats
import supabase


def create_efficiency_stats(df):
    df[('efficiency','pass_yards_efficiency')] = df[('Passing','Yds')] / df[('Off. Snaps', 'Num')]
    df[('efficiency','rush_yards_efficiency')] = df[('Rushing','Yds')] / df[('Off. Snaps', 'Num')]
    df[('efficiency','rec_yards_efficiency')] = df[('Receiving','Yds')] / df[('Off. Snaps', 'Num')]
    df[('efficiency','total_yds_eff')] = df[('efficiency','pass_yards_efficiency')] + df[('efficiency','rush_yards_efficiency')] + df[('efficiency','rec_yards_efficiency')]

    df[('efficiency','OffensiveTDs')] = df[('Rushing','TD')] + df[('Receiving','TD')]
    df[('efficiency','OffensiveTouchdownEfficiency')] = df[('efficiency','OffensiveTDs')] / df[('Off. Snaps', 'Num')]
    df[('efficiency','OffensiveYards')] = df[('Rushing','Yds')] + df[('Receiving','Yds')]
    df[('efficiency','O_td_eff')] = df[('efficiency','OffensiveTDs')] / df[('Off. Snaps', 'Num')]

    df[('efficiency','tackle_efficiency')] =(df[('Tackles','Solo')] + df['Tackles','Ast']) / df[('Def. Snaps', 'Num')]
    df[('efficiency','sack_efficiency')] = df[('Tackles','Sk')] / df[('Def. Snaps', 'Num')]
    df[('efficiency','QB_pressure_eff')] = (df[('Tackles','QBHits')] + df[('Tackles','Sk')]) / df[('Def. Snaps', 'Num')]
    df[('efficiency','Turnover_eff')] = (df[('Def Interceptions','Int')] + df[('Fumbles','FF')] + df[('Def Interceptions','PD')]) / df[('Def. Snaps', 'Num')]
    df[('efficiency','PassesDefended_eff')] = df[('Def Interceptions','PD')] / df[('Def. Snaps', 'Num')]

    
    return df

def get_OL_stats(season, PlayerID, OL_hist_dict, API_KEY,boxScores_dict):
    start = dt.now()
    lastYearLog = get_player_log_by_season(season - 1, PlayerID, API_KEY)
    thisYearLog = get_player_log_by_season(season, PlayerID, API_KEY) 

    for game in lastYearLog:
        OL_hist_dict['Started'].append(game['Started'])

        if game['Started'] == 1: 
            OL_hist_dict['ScoreID'].append(game['ScoreID'])
            OL_hist_dict['Team'].append(game['Team'])
            OL_hist_dict['OffensiveSnapsPlayed'].append(game['OffensiveSnapsPlayed'])
    
    for game in thisYearLog:
        OL_hist_dict['Started'].append(game['Started'])

        if (game['Started'] == 1): 
            OL_hist_dict['ScoreID'].append(game['ScoreID'])
            OL_hist_dict['Team'].append(game['Team'])
            OL_hist_dict['OffensiveSnapsPlayed'].append(game['OffensiveSnapsPlayed'])            

    for ScoreID, team in zip(OL_hist_dict['ScoreID'], OL_hist_dict['Team']):
        
        
        OL_hist_dict['PassingYards'].append(boxScores_dict[ScoreID][team]['PassingYards'])
        OL_hist_dict['RushingYards'].append(boxScores_dict[ScoreID][team]['RushingYards'])
        OL_hist_dict['QuarterbackHits'].append(boxScores_dict[ScoreID][team]['OpponentQuarterbackHits'])
        OL_hist_dict['Sacks'].append(boxScores_dict[ScoreID][team]['OpponentSacks'])

    #print('Got OL stats in:',dt.now() - start)
    return OL_hist_dict

def calc_OL_stats(PlayerID, position, season, API_KEY,boxScores_dict):
    OL_hist_dict = {}
    OL_hist_dict['Started'] = []
    OL_hist_dict['ScoreID'] = []
    OL_hist_dict['OffensiveSnapsPlayed'] = []
    OL_hist_dict['Team'] = []
    OL_hist_dict['PassingYards'] = []
    OL_hist_dict['RushingYards'] = []
    OL_hist_dict['QuarterbackHits'] = []
    OL_hist_dict['Sacks'] = []
    #if OL then continue
    if position != 'OL':
        #return empty dictionary
        return OL_hist_dict
    else:
        #Get ScoreIDs and team played for the OL started in (need to research best way to collect)
        return get_OL_stats(season, PlayerID, OL_hist_dict, API_KEY,boxScores_dict)
    
def create_OL_stats(df, currentSeason, API_KEY, boxScores_dict):
    df['OL_dict'] =  df.apply(lambda row:calc_OL_stats(row['PlayerID'],row['DepthPosition'],currentSeason, API_KEY,boxScores_dict), axis = 1)
    df['OL_OffensiveSnapsPlayed'] = df.apply(lambda row: np.mean(undict(row['OL_dict'])['OffensiveSnapsPlayed']),axis = 1)
    df['OL_PassingYards'] = df.apply(lambda row: np.mean(undict(row['OL_dict'])['PassingYards']),axis = 1)
    df['OL_QuarterbackHits'] =df.apply(lambda row: np.mean(undict(row['OL_dict'])['QuarterbackHits']),axis = 1)
    df['OL_RushingYards'] = df.apply(lambda row: np.mean(undict(row['OL_dict'])['RushingYards']),axis = 1)
    df['OL_Sacks'] = df.apply(lambda row: np.mean(undict(row['OL_dict'])['Sacks']),axis = 1)
    df['OL_Started'] = df.apply(lambda row: np.mean(undict(row['OL_dict'])['Started']),axis = 1)
    return df

def calc_roster(team, stats_list, totalStats, currentSeason, API_KEY, boxScores_dict):
    print('Getting Roster Details...')
    #get roster
    start = dt.now()
    roster = get_depth_chart(team, API_KEY)
    print('Got player details in:',dt.now() - start)
    
    #Rename positions
    start = dt.now()
    roster = rename_positions(roster)
    print('renamed positions in:',dt.now() - start)
    
    #Add stats
    start = dt.now()
    for stat in stats_list:
        roster[stat] = roster.apply(lambda row: add_stat(row['PlayerID'], stat, totalStats),axis = 1)
    print('added stats in:',dt.now() - start)
    
    #calc efficiencies
    start = dt.now()
    roster = create_efficiency_stats(roster)
    
    #Add OL Stats
    start = dt.now()
    roster = create_OL_stats(roster, currentSeason, API_KEY, boxScores_dict)
    print('created OL stats in:',dt.now() - start)
    
    return roster.fillna(0)

def wavg(group, avg_name, weight_name):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()

def calc_team(team, stats_list, totalStats, currentSeason, API_KEY, boxScores_dict):
    from datetime import datetime as dt
    start = dt.now()
    print(team,start)
    calc_dict = {}
    calc_dict = calc_dict.copy()
        
    rosterStart = dt.now()
    print()
    roster = calc_roster(team, stats_list, totalStats, currentSeason, API_KEY, boxScores_dict)
    print('Calculated roster in:', dt.now() - rosterStart)
    
    #calc wavgs
    QBStart = dt.now()
    calc_dict['QB_eff'] = wavg(roster[(roster['DepthPosition'] == 'QB')], "total_yds_eff", "OffensiveSnapsPlayed")
    print('Calculated QB efficiency in:', dt.now() - QBStart)
    
    calc_dict['RB_eff'] = wavg(roster[(roster['DepthPosition'] == 'RB')], "total_yds_eff", "OffensiveSnapsPlayed")
    calc_dict['WR_eff'] = wavg(roster[(roster['DepthPosition'] == 'WR')], "total_yds_eff", "OffensiveSnapsPlayed")
    calc_dict['TE_eff'] = wavg(roster[(roster['DepthPosition'] == 'TE')], "total_yds_eff", "OffensiveSnapsPlayed")
    
    calc_dict['CB/SS_tackle_eff'] = wavg(roster[(roster['DepthPosition'] == 'CB/SS')], 'tackle_efficiency', 'DefensiveSnapsPlayed')
    calc_dict['DE/LB_tackle_eff'] = wavg(roster[(roster['DepthPosition'] == 'DE/LB')], 'tackle_efficiency', 'DefensiveSnapsPlayed')
    
    calc_dict['CB/SS_QB_pressure_eff'] = wavg(roster[(roster['DepthPosition'] == 'CB/SS')], 'QB_pressure_eff', 'DefensiveSnapsPlayed')
    calc_dict['DE/LB_QB_pressure_eff'] = wavg(roster[(roster['DepthPosition'] == 'DE/LB')], 'QB_pressure_eff', 'DefensiveSnapsPlayed')
    
    calc_dict['CB/SS_Turnover_eff'] = wavg(roster[(roster['DepthPosition'] == 'CB/SS')], 'Turnover_eff', 'DefensiveSnapsPlayed')
    calc_dict['DE/LB_Turnover_eff'] = wavg(roster[(roster['DepthPosition'] == 'DE/LB')], 'Turnover_eff', 'DefensiveSnapsPlayed')
    
    calc_dict['CB/SS_PassesDefended_eff'] = wavg(roster[(roster['DepthPosition'] == 'CB/SS')], 'PassesDefended_eff', 'DefensiveSnapsPlayed')
    calc_dict['DE/LB_PassesDefended_eff'] = wavg(roster[(roster['DepthPosition'] == 'DE/LB')], 'PassesDefended_eff', 'DefensiveSnapsPlayed')
    
    calc_dict['OL_PassingYards_eff'] = wavg(roster[(roster['DepthPosition'] == 'OL')], 'OL_PassingYards', 'OL_OffensiveSnapsPlayed')
    calc_dict['OL_QuarterbackHits_eff'] = wavg(roster[(roster['DepthPosition'] == 'OL')], 'OL_QuarterbackHits', 'OL_OffensiveSnapsPlayed')
    calc_dict['OL_RushingYards_eff'] = wavg(roster[(roster['DepthPosition'] == 'OL')], 'OL_RushingYards', 'OL_OffensiveSnapsPlayed')
    calc_dict['OL_Sacks_eff'] = wavg(roster[(roster['DepthPosition'] == 'OL')], 'OL_Sacks', 'OL_OffensiveSnapsPlayed')    
    
    print('Calculated team in:',dt.now() - start)
    return calc_dict  

def aggregate_player_eff(schedule, stats_list, totalStats, currentSeason, API_KEY, boxScores_dict):
    
    schedule['HOME_eff_dict'] = schedule.apply(lambda row: calc_team(row['HomeTeam'], stats_list, totalStats, currentSeason, API_KEY, boxScores_dict), axis=1)
    schedule['AWAY_eff_dict'] = schedule.apply(lambda row: calc_team(row['AwayTeam'], stats_list, totalStats, currentSeason, API_KEY, boxScores_dict), axis=1)
    
    schedule['HOME_QB_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['QB_eff'], axis=1)
    schedule['HOME_QB_eff'] = schedule['HOME_QB_eff'].fillna(0)
    schedule['AWAY_QB_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['QB_eff'], axis=1)
    schedule['AWAY_QB_eff'] = schedule['AWAY_QB_eff'].fillna(0)
    
    schedule['HOME_RB_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['RB_eff'], axis=1)
    schedule['AWAY_RB_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['RB_eff'], axis=1)

    schedule['HOME_WR_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['WR_eff'], axis=1)
    schedule['AWAY_WR_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['WR_eff'], axis=1)

    schedule['HOME_TE_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['TE_eff'], axis=1)
    schedule['AWAY_TE_eff']= schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['TE_eff'], axis=1)

    schedule['AWAY_CB/SS_tackle_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['CB/SS_tackle_eff'], axis=1)
    schedule['HOME_CB/SS_tackle_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['CB/SS_tackle_eff'], axis=1)

    schedule['AWAY_DE/LB_tackle_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['DE/LB_tackle_eff'] , axis=1)
    schedule['HOME_DE/LB_tackle_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['DE/LB_tackle_eff'] , axis=1)

    schedule['AWAY_CB/SS_QB_pressure_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['CB/SS_QB_pressure_eff'] , axis=1)
    schedule['HOME_CB/SS_QB_pressure_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['CB/SS_QB_pressure_eff'] , axis=1)

    schedule['AWAY_DE/LB_QB_pressure_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['DE/LB_QB_pressure_eff'] , axis=1)
    schedule['HOME_DE/LB_QB_pressure_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['DE/LB_QB_pressure_eff'] , axis=1)

    schedule['AWAY_CB/SS_Turnover_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['CB/SS_Turnover_eff'] , axis=1)
    schedule['HOME_CB/SS_Turnover_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['CB/SS_Turnover_eff'] , axis=1)

    schedule['AWAY_DE/LB_Turnover_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['DE/LB_Turnover_eff'] , axis=1)
    schedule['HOME_DE/LB_Turnover_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['DE/LB_Turnover_eff'] , axis=1)

    schedule['AWAY_CB/SS_PassesDefended_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['CB/SS_PassesDefended_eff'] , axis=1)
    schedule['HOME_CB/SS_PassesDefended_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['CB/SS_PassesDefended_eff'] , axis=1)

    schedule['AWAY_DE/LB_PassesDefended_eff'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['DE/LB_PassesDefended_eff'] , axis=1)
    schedule['HOME_DE/LB_PassesDefended_eff'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['DE/LB_PassesDefended_eff'] , axis=1)

    schedule['AWAY_OL_PASS'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['OL_PassingYards_eff'] , axis=1)
    schedule['HOME_OL_PASS'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['OL_PassingYards_eff'] , axis=1)

    schedule['AWAY_OL_QBHit'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['OL_QuarterbackHits_eff'] , axis=1)
    schedule['HOME_OL_QBHit'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['OL_QuarterbackHits_eff'] , axis=1)

    schedule['AWAY_OL_RUSH'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['OL_RushingYards_eff'] , axis=1)
    schedule['HOME_OL_RUSH'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['OL_RushingYards_eff'] , axis=1)

    schedule['AWAY_OL_SACK'] = schedule.apply(lambda row: undict(row['HOME_eff_dict'])['OL_Sacks_eff'] , axis=1)
    schedule['HOME_OL_SACK'] = schedule.apply(lambda row: undict(row['AWAY_eff_dict'])['OL_Sacks_eff'] , axis=1)
    
    schedule['HomeTotal_eff'] =  schedule['HOME_QB_eff']+ schedule['HOME_RB_eff']+ schedule['HOME_TE_eff']+ schedule['HOME_WR_eff']
    schedule['AwayTotal_eff'] =  schedule['AWAY_QB_eff']+ schedule['AWAY_RB_eff']+ schedule['AWAY_TE_eff']+ schedule['AWAY_WR_eff']
    
    schedule['delta_eff'] = schedule['HomeTotal_eff'] - schedule['AwayTotal_eff']
    
    schedule['delta_QB_eff'] = schedule['HOME_QB_eff'] - schedule['AWAY_QB_eff']
    schedule['total_QB_eff'] = schedule['HOME_QB_eff'] + schedule['AWAY_QB_eff']
    schedule['delta_RB_eff'] = schedule['HOME_RB_eff'] - schedule['AWAY_RB_eff']
    schedule['delta_TE_eff'] = schedule['HOME_TE_eff'] - schedule['AWAY_TE_eff']
    schedule['delta_WR_eff'] = schedule['HOME_WR_eff'] - schedule['AWAY_WR_eff']
    
    schedule['effScore_HOME'] = (schedule['delta_QB_eff'] > 0).astype(int) + (schedule['delta_RB_eff'] > 0).astype(int) + (schedule['delta_TE_eff'] > 0).astype(int) + (schedule['delta_WR_eff'] > 0).astype(int)
    schedule['effScore_AWAY'] = (schedule['delta_QB_eff'] < 0).astype(int) + (schedule['delta_RB_eff'] < 0).astype(int) + (schedule['delta_TE_eff'] < 0).astype(int) + (schedule['delta_WR_eff'] < 0).astype(int)
    schedule['delta_effScore'] = schedule['effScore_HOME'] - schedule['effScore_AWAY']
    schedule['eff_total'] = schedule['HomeTotal_eff'] + schedule['AwayTotal_eff']
    
    schedule['tackle_delta'] = schedule['HOME_CB/SS_tackle_eff'] + schedule['HOME_DE/LB_tackle_eff'] - schedule['AWAY_CB/SS_tackle_eff'] - schedule['AWAY_DE/LB_tackle_eff']
    schedule['QB_pressure_delta'] = schedule['HOME_CB/SS_QB_pressure_eff'] + schedule['HOME_DE/LB_QB_pressure_eff'] - schedule['AWAY_CB/SS_QB_pressure_eff'] - schedule['AWAY_DE/LB_QB_pressure_eff']
    schedule['Turnover_delta'] = schedule['HOME_CB/SS_Turnover_eff'] + schedule['HOME_DE/LB_Turnover_eff'] - schedule['AWAY_CB/SS_Turnover_eff'] - schedule['AWAY_DE/LB_Turnover_eff']
    schedule['PassesDefended_delta'] = schedule['HOME_CB/SS_PassesDefended_eff'] + schedule['HOME_DE/LB_PassesDefended_eff'] - schedule['AWAY_CB/SS_PassesDefended_eff'] - schedule['AWAY_DE/LB_PassesDefended_eff']
    
    #drop eff dict
    schedule = schedule.drop(['HOME_eff_dict','AWAY_eff_dict'],axis=1)
    
    return schedule

def team_table_ML_result(score, opponent_score):
    """
    Returns the ML result of a game for the team table
    """
    
    if score > opponent_score:
        result = 'Win'
    elif score < opponent_score:
        result = 'Loss'
    elif score == opponent_score:
        result = 'Tie'
        
    return result    



def calc_wins(season, week, team, season_df):
    """
    for a given season, calculate a team total wins to date this season
    """
    # if week 1, 0 wins. everytime
    if week == 1:
        wins = 0
        return wins
    # else we use this years numbers
    else:
        season_df = create_team_game_table(season_df, team, week)
        season_df = season_df[season_df['Season'] == season]
        df = season_df[season_df['Week'] < week]
        wins = df[df['ML_Result'] == 'Win'].shape[0]
        return wins
    
def calc_wins_from_last_year(season, team, season_df, week):
    """
    for a given season, calculate a team total wins from last season
    """
    season_df = create_team_game_table(season_df, team, week)
    season_df = season_df[season_df['Season'] == (season-1)]
    df = season_df.drop_duplicates()
    wins = df[df['ML_Result'] == 'Win'].shape[0]
    return wins

def calc_total_stat_from_last_year(season, team, season_df, stat):
    """
    for a given season, calculate a team total points from last season
    """
    season_df = create_team_game_table(season_df, team)
    season_df = season_df[season_df['Season'] == (season-1)]
    df = season_df.drop_duplicates()
    pts = df[stat].sum()
    return pts

def calc_mov(team, season,week):
    print('Calculating MoV:')
    print('Team', team, 'Season', season, 'Week', week)
    """
    for a given season, calculate a teams MoV (avg doint differential)
    """
    # if week 1, use last years numbers
    # its week 1, who the fuck knows
    if  (team=='TB') and (season==2017) and (week==2): return 0
    elif  (team=='MIA') and (season==2017) and (week==2): return 0
    elif week == 1: 
        season_df = create_team_game_table(team, season-1, 18)
        season_df = season_df[season_df['Season'] == (season-1)]
        df = season_df.drop_duplicates()
        mov = (df['Score'].sum() - df['OpponentScore'].sum()) / df.shape[0]
        return mov
    # else we use this years numbers
    else:
        season_df = create_team_game_table(team=team, season=season,week=week)
        season_df = season_df[season_df['Season'] == season]
        df = season_df[season_df['Week'] < week].drop_duplicates()
        mov = (df['Score'].sum() - df['OpponentScore'].sum()) / df.shape[0]
        return mov
    
def calc_moy(season,week, team):
    """
    for a given season, calculate a teams MoY (avg yard differential)
    """
    # if week 1, use last years numbers
    # its week 1, who the fuck knows
    if week == 1: 
        season_df = create_team_game_table(team, season-1, 18)
        season_df = season_df[season_df['Season'] == (season-1)]
        df = season_df.drop_duplicates()
        moy = (df['OffensiveYards'].sum() - df['OpponentOffensiveYards'].sum()) / df.shape[0]
        return moy
    # else we use this years numbers
    else:
        season_df = create_team_game_table(team, season, week)
        season_df = season_df[season_df['Season'] == season]
        df = season_df[season_df['Week'] < week].drop_duplicates()
        moy = (df['OffensiveYards'].sum() - df['OpponentOffensiveYards'].sum()) / df.shape[0]
        return moy
    
def calc_mot(season,week, team, thisYearTeamStats, lastYearTeamStats):
    """
    for a given season, calculate a teams MoT (avg TO differential)
    """
    # if week 1, use last years numbers
    # its week 1, who the fuck knows
    if week == 1: 
        season_df = create_team_game_table(team, season, 20)
        season_df = season_df[season_df['Season'] == (season-1)]
        df = season_df.drop_duplicates()
        mot = (df['TurnoverDifferential'].sum()) / df.shape[0]
        return mot
    # else we use this years numbers
    else:
        season_df = create_team_game_table(team, season, week)
        season_df = season_df[season_df['Season'] == season]
        df = season_df[season_df['Week'] < week].drop_duplicates()
        mot = (df['TurnoverDifferential'].sum()) / df.shape[0]
        return mot

def create_team_game_table(team, season, week):
    print('Creating Game Table:')
    print('Team', team, 'Season', season, 'Week', week)
    
    team_games = collect_season_team_stats(team=team, season=season, week=week)
    

    df_out = pd.DataFrame()
    df_out['Team'] = team
    df_out['Week'] = [i for i in team_games['schedule'].keys()]
    df_out['Season'] = season
    df_out['Opponent'] = [team_games['schedule'][i]['Opponent'] for i in team_games['schedule'].keys()]
    df_out['Score'] = [team_games['schedule'][i]['Score'] for i in team_games['schedule'].keys()]
    df_out['OpponentScore'] = [team_games['schedule'][i]['OpponentScore'] for i in team_games['schedule'].keys()]
    df_out['OffensiveYards'] = [team_games['schedule'][i]['YardsFor'] for i in team_games['schedule'].keys()]
    df_out['OpponentOffensiveYards'] = [team_games['schedule'][i]['YardsAgainst'] for i in team_games['schedule'].keys()]
    
    print(df_out)

    df_out['MoV'] = df_out['Score'] - df_out['OpponentScore']
    df_out['MoY'] = df_out['OffensiveYards'] - df_out['OpponentOffensiveYards']
    #df_out['MoT'] = df_out['TurnoverDifferential']
    
    df_out['ML_Result'] = df_out.apply(lambda row: team_table_ML_result(row['Score'], row['OpponentScore']), axis = 1)
    
    return df_out
    
def calc_SRS(season, week, team, team_games, client):
    """
    
    Params:
    -------
        season: int or float
        week: int or float
        team: str
        team_games: dict
            - the out put of the collect_season_team_stats function
        client: supabase client
        
    Returns:
        SRS: float
            - simple rating system
    
    """
    #print('Calculating SRS:')
    #print('Team', team, 'Season', season, 'Week', week) 
    'https://www.sports-reference.com/blog/2015/03/srs-calculation-details/'
    if (week == 1): return 0
    elif  (team=='TB') and (season==2017) and (week==2): return 0
    elif  (team=='MIA') and (season==2017) and (week==2): return 0
    else:
       
        # get list of opponents
        opponents = []
        for i in team_games['schedule'].keys():
            if i < week:
                opponents.append(team_games['schedule'][i]['Opponent'])
        
        # get schdule from s3
        schedule = fetch_pkl_from_s3('antelope', 'dev/ExtractedStats/Schedules/'+str(season)+'_Schedule_Latest.pkl')
        print("feature engineering utils:", schedule)
        # filter out post season
        schedule = schedule[schedule['Week'] != 'WildCard']
        schedule = schedule[schedule['Week'] != 'Division']
        schedule = schedule[schedule['Week'] != 'ConfChamp']
        schedule = schedule[schedule['Week'] != 'SuperBowl']
        schedule['Week'] = schedule['Week'].astype(int)
        schedule = schedule[schedule['Week'] < week]

        #Home field advantage
        HFA = (schedule.HomeScore.sum() - schedule.AwayScore.sum()) / schedule.shape[0]
        
        
        # league MoV
        schedule['adjusted_MoV'] = schedule['HomeScore'] - schedule['AwayScore'] - HFA
        schedule = rename_teams(schedule, "HomeTeam")
        schedule = rename_teams(schedule, "AwayTeam")

        leagueMoV = schedule['adjusted_MoV'].mean()
        

        # calculate opponents MoV
        opponent_MoV_list = []
        for opponent in opponents:
            print(", collecting MOV for",opponent,"from",opponents, "in", season, week)
            #opponent_MoV_list.append(calc_mov(team=opponent, season=season, week=week))
            opponent_MoV_list.append(collect_mov_from_supabase(team=opponent, season=season, week=week, client=client))
            
        
        print(opponents)
        print(opponent_MoV_list)
        print('opponent mov', opponent_MoV_list)
        sos =  np.mean(opponent_MoV_list) - leagueMoV
        print('Strength of Schedule:', sos)

        #ownMoV = calc_mov(team=team, season=season, week=week)
        ownMoV = collect_mov_from_supabase(team=team, season=season, week=week, client=client)
        
        srs = ownMoV + sos
        
        print('Homefield Advantage:' , HFA)
        print('opponents:',opponents)
        print('Strength of Schedule:', sos)
        print('leagueMoV:' , leagueMoV)
        print('ownMov', ownMoV)
        print('SRS', srs)

        return srs
    
def calc_margins_and_SRS(schedule ,thisYearTeamStats, lastYearTeamStats):
    """
    Aggregates the following calculations to the schedule:
    Calculations:
        - Games won this year and last year
        - MoV
        - MoT
        - MoT
        - SRS
    """
    #add games won this year
    schedule['HomeGamesWonThisYear'] = schedule.apply(lambda row: calc_wins(row['Season'],row['Week'], row['HomeTeam'], thisYearTeamStats), axis = 1)
    schedule['AwayGamesWonThisYear'] = schedule.apply(lambda row: calc_wins(row['Season'],row['Week'], row['AwayTeam'], thisYearTeamStats), axis = 1)

    # add games won last year
    schedule['HomeGamesWonLastYear'] = schedule.apply(lambda row: calc_wins_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats), axis = 1)
    schedule['AwayGamesWonLastYear'] = schedule.apply(lambda row: calc_wins_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats), axis = 1)

    #add points for last year
    schedule['HomePtsForLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'Score'), axis = 1)
    schedule['AwayPtsForLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'Score'), axis = 1)

    #add points for last year
    schedule['HomePtsAgainstLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'OpponentScore'), axis = 1)
    schedule['AwayPtsAgainstLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'OpponentScore'), axis = 1)

    #add points for last year
    schedule['HomePtsForLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'Score'), axis = 1)
    schedule['AwayPtsForLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'Score'), axis = 1)

    #add points for last year
    schedule['HomePtsAgainstLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'OpponentScore'), axis = 1)
    schedule['AwayPtsAgainstLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'OpponentScore'), axis = 1)

    #add points for last year
    schedule['HomeYdsForLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'OffensiveYards'), axis = 1)
    schedule['AwayYdsForLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'OffensiveYards'), axis = 1)

    #add points for last yearOpponentOffensiveYards
    schedule['HomeYdsAgainstLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'OpponentOffensiveYards'), axis = 1)
    schedule['AwayYdsAgainstLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'OpponentOffensiveYards'), axis = 1)

    #add points for last year
    schedule['HomeTurnoversWonLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'Takeaways'), axis = 1)
    schedule['AwayTurnoversWonLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'Takeaways'), axis = 1)

    #add points for last year
    schedule['HomeTurnoversLostLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'OpponentTakeaways'), axis = 1)
    schedule['AwayTurnoversLostLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'OpponentTakeaways'), axis = 1)

    # add margin of victory
    schedule['HomeMoV'] = schedule.apply(lambda row: calc_mov(row['Season'],row['Week'], row['HomeTeam'], thisYearTeamStats, lastYearTeamStats), axis = 1)
    schedule['AwayMoV'] = schedule.apply(lambda row: calc_mov(row['Season'],row['Week'], row['AwayTeam'], thisYearTeamStats, lastYearTeamStats), axis = 1)

    # need to fillna with 0 for 2017 TB/MIA hurricane game
    schedule['HomeMoV'] = schedule['HomeMoV'].fillna(0)
    schedule['AwayMoV'] = schedule['AwayMoV'].fillna(0)

    # add margin of yards
    schedule['HomeMoY'] = schedule.apply(lambda row: calc_moy(row['Season'],row['Week'], row['HomeTeam'], thisYearTeamStats, lastYearTeamStats), axis = 1)
    schedule['AwayMoY'] = schedule.apply(lambda row: calc_moy(row['Season'],row['Week'], row['AwayTeam'], thisYearTeamStats, lastYearTeamStats), axis = 1)

    # need to fillna with 0 for 2017 TB/MIA hurricane game
    schedule['HomeMoY'] = schedule['HomeMoY'].fillna(0)
    schedule['AwayMoY'] = schedule['AwayMoY'].fillna(0)

    # add margin of Turnovers
    schedule['HomeMoT'] = schedule.apply(lambda row: calc_mot(row['Season'],row['Week'], row['HomeTeam'], thisYearTeamStats, lastYearTeamStats), axis = 1)
    schedule['AwayMoT'] = schedule.apply(lambda row: calc_mot(row['Season'],row['Week'], row['AwayTeam'], thisYearTeamStats, lastYearTeamStats), axis = 1)

    # need to fillna with 0 for 2017 TB/MIA hurricane game
    schedule['HomeMoT'] = schedule['HomeMoT'].fillna(0)
    schedule['AwayMoT'] = schedule['AwayMoT'].fillna(0)

    #calc SRS
    schedule['HomeSRS'] = schedule.apply(lambda row: calc_SRS(row['Season'],row['Week'], row['HomeTeam'], thisYearTeamStats, thisYearTeamStats, lastYearTeamStats), axis = 1)
    schedule['AwaySRS'] = schedule.apply(lambda row: calc_SRS(row['Season'],row['Week'], row['AwayTeam'], thisYearTeamStats, thisYearTeamStats, lastYearTeamStats), axis = 1)
    
    #add TDs for last year
    schedule['HomeTouchdownsLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['HomeTeam'], lastYearTeamStats, 'Touchdowns'), axis = 1)
    schedule['AwayTouchdownsLastYear'] = schedule.apply(lambda row: calc_total_stat_from_last_year(row['Season'], row['AwayTeam'], lastYearTeamStats, 'Touchdowns'), axis = 1)
    
    return schedule

def add_team_avg(team, stat, thisYearTeamStats, lastYearTeamStats, week):
    # use last years avg if week 1
    if week == 1:
        stats_df = lastYearTeamStats[(lastYearTeamStats['Team'] == team)]
        
    #else use this years stats
    else:
        stats_df = thisYearTeamStats[
            (thisYearTeamStats['Team'] == team)
            & (thisYearTeamStats['Week'] < week)
        ] 
    
    try:
        df = stats_df
        #print('Total:', df[stat].sum())
        return df[stat].sum() / df.shape[0]
    except: return 0
    
def calc_home_away_totals(OverUnder,PointSpread):
    
    away = (OverUnder + PointSpread) / 2
    home = OverUnder - away
    
    return away, home

def calc_team_effieciencies(meta_data_dict, roster_stats_df):
    """
    Calculates team efficiencies for all positions
    
    Inputs, 
    ------
    meta_data_dict: dict
    roster_stats_df: df
    
    Outputs
    -------
    team_eff_stats_dict: dict
    
    """
    # get player stats
    PLAYER_STATS = [stat for stat in meta_data_dict if meta_data_dict[stat]['stat_level'] == 'player']
    
    # get positions
    POSITIONS = meta_data_dict['Positions']['values']
    
    # dictionary to add stats to
    team_eff_stats_dict = {} 
    
    for position in POSITIONS:
        team_eff_stats_dict[position] = {}
        for stat in PLAYER_STATS:
            if meta_data_dict[stat]['efficiency'] == 'on':

                # calculate stats efficiency across sample
                eff_stat_name = stat + "Efficiency"
                numerator_stat = stat
                denominator_stat = meta_data_dict[stat]['efficiencyDenominator']

                roster_stats_df[eff_stat_name] = roster_stats_df[numerator_stat] / roster_stats_df[denominator_stat] 

                # for a given offensive stat, calculate that teams stat efficiency
                eff = wavg(roster_stats_df[(roster_stats_df['Position'] == position)], eff_stat_name , denominator_stat)
                
                # insert into dictionary
                team_eff_stats_dict[position][eff_stat_name] = eff
                
    return team_eff_stats_dict

def collect_and_calc_team_efficiencies(team, season, week, meta_data_dict):
    """
    1. collects team efficiencies from s3
    2. calculates team efficiencieseturns:
    3. returns team stat dictionary
    
    Params:
    -------
        team: str
        season: int or str
        week: int or str
        meta_data_dict: dict
    
    Returns:
    --------
        team_eff_stats_dict: dict
    
    """
    print('collecting roster stats for:', team)
    print('sleeping 60s')
    time.sleep(60)
    start = dt.now()
    api_str = "https://ggavfgzh45.execute-api.us-east-2.amazonaws.com/default/GetRosterStats?season="+str(season)+"&week="+str(week)+"&team="+team
    response = requests.get(api_str)
    print(response.json())
    s3_bucket = response.json()['s3_bucket'].replace('"','')
    s3_key = response.json()['s3_key'].replace('"','')
    roster_stats_df = fetch_pkl_from_s3(s3_bucket, s3_key)
    #print(roster_stats_df)

    roster_stats_df = rename_positions(roster_stats_df)
    # print('\tCollected roster_stats_df in:', dt.now() - start)

    start = dt.now()
    team_eff_stats_dict = calc_team_effieciencies(meta_data_dict,roster_stats_df)
    # print('\tCalculated team_eff_stats_dict in:', dt.now() - start)
    print('Passing Yards Efficiency:', team_eff_stats_dict['QB']['PassingYardsEfficiency'])
    print('Rushing Yards Efficiency:', team_eff_stats_dict['RB']['RushingYardsEfficiency'])
    return team_eff_stats_dict

def calc_home_away_totals(OverUnder,PointSpread):
    
    away = (OverUnder + PointSpread) / 2
    home = OverUnder - away
    
    return away, home

def sum_up_efficiency_stats(efficiency_dict):
    """
    accepts an efficiency dictionary in the following form and returns a sum of the 3 components. 
    {
     'PassingYardsEfficiency': 3.9973045822102424,
     'RushingYardsEfficiency': 0.13117699910152741,
     'ReceivingYardsEfficiency': 0.0
    }
    """
    sum_of_efficiencies = efficiency_dict['PassingYardsEfficiency'] + efficiency_dict['RushingYardsEfficiency'] + efficiency_dict['ReceivingYardsEfficiency']
    
    return sum_of_efficiencies

def sum_up_efficiency_for_defensive_stats(efficiency_dict, stat):
    """
    accepts an efficiency dictionary in the following form and returns a sum of the 3 components. 
    {
     'PassingYardsEfficiency': 3.9973045822102424,
     'RushingYardsEfficiency': 0.13117699910152741,
     'ReceivingYardsEfficiency': 0.0
    }
    """
    sum_of_efficiencies = efficiency_dict['PassingYardsEfficiency'] + efficiency_dict['RushingYardsEfficiency'] + efficiency_dict['ReceivingYardsEfficiency']
    
    return sum_of_efficiencies

def collect_mov_from_supabase(team, season, week, client):
    """Collects home and away MoV from supabase
    
    Params:
    -------
        team: str
        season: int
        week: int
        client: supabase client
    
    Returns:
        MoVs: float
    
    """
    print('collecting mov:', team, season, week)
    
     # check if record is already in table
    response = client.from_('MoV_table').select("*").eq('Team',team).execute()
    
    data = response.data
    print(data)
    
    for record in data:
        print(record['Season'], record['Week'])
        if record['Season'] == season and record['Week'] == week:
            MoV = record['margin_of_victory']
        else:
            MoV = 'Unable to return margin of victory'
    
    return MoV