

# extract stat from bookmakers dictionary
### Call get_odds api
API_KEY = '707089c36385cf209b8473df00b7770f'
REGIONS = 'us'
MARKETS = 'h2h,spreads,totals'
ODDS_FORMAT = 'decimal'
DATE_FORMAT = 'iso'
SPORT = 'americanfootball_nfl'

def lambda_handler(event, context):
    print("Lambda handler started")
    # connect to database
    print("importing supabase")
    import supabase
    print("importing requests")
    import requests
    print("importing pandas and datetime")
    import pandas as pd
    from datetime import datetime as dt
    print("importing antelope_utils")
    from antelope_utils.OddsScrapers import calc_home_away_totals, extract_stat_from_bookmaker_dict
    from antelope_utils.data_prep_utils import rename_team_str, convert_date_to_nfl_week, get_nfl_season
    week = convert_date_to_nfl_week(dt.now())
    print('Week Number:',week)

    currentSeason = get_nfl_season()
    currentWeek = week

    print("calling odds API...")
    odds_response = requests.get(
        f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds',
        params={
            'api_key': API_KEY,
            'regions': REGIONS,
            'markets': MARKETS,
            'oddsFormat': ODDS_FORMAT,
            'dateFormat': DATE_FORMAT,
        }
    )
        
    odds_df = pd.DataFrame(odds_response.json())
    odds_df[['commence_time','home_team','away_team','bookmakers']]

    odds_df['Week'] = odds_df.apply(lambda row: convert_date_to_nfl_week(dt.strptime(row['commence_time'], "%Y-%m-%dT%H:%M:%SZ")), axis=1)
    odds_df = odds_df[odds_df['Week'] == int(currentWeek)]

    # process for playoffs
    def process_playoff_week(odds_df):
        week_list =[]
        
        # if week = 19 --> WildCard
        for index, row in odds_df.iterrows():
            if row['Week'] == 19:
                week_list.append("WildCard")
            elif row['Week'] == 20:
                week_list.append("Division")
            elif row['Week'] == 21:
                week_list.append("ConfChamp")
            else:
                week_list.append(row['Week'])
        odds_df['Week'] = week_list
        return odds_df

    print("Processing playoff week if applicable...")
    odds_df = process_playoff_week(odds_df)
    print("Bookmakers sample:", odds_df['bookmakers'].head())

    odds_df['PointSpread'] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'spreads', row['home_team'])[1], axis = 1)
    odds_df['OverUnder'] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'totals', row['home_team'])[1], axis = 1)
    odds_df['AwayTeamMoneyLine'] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'h2h', row['away_team'])[0], axis = 1)
    odds_df['HomeTeamMoneyLine'] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'h2h', row['home_team'])[0], axis = 1)

    # rename teams for appending to schedule
    odds_df['HomeTeam'] = odds_df.apply(lambda row: rename_team_str(row['home_team']), axis = 1)
    odds_df['AwayTeam'] = odds_df.apply(lambda row: rename_team_str(row['away_team']), axis = 1)

    odds_df = odds_df.drop(['home_team', 'away_team', 'id', 'sport_key', 'sport_title'], axis = 1)
    odds_df['Season'] = currentSeason
    odds_df['key'] = odds_df['Season'].astype(str) + '_' + odds_df['Week'].astype(str) + '_' + odds_df['AwayTeam'].astype(str) + '_' +  odds_df['HomeTeam'].astype(str)

    odds_df['AWAY_OU'] = odds_df.apply(lambda row: calc_home_away_totals(row['OverUnder'],row['PointSpread'])[0],axis=1)
    odds_df['HOME_OU'] = odds_df.apply(lambda row: calc_home_away_totals(row['OverUnder'],row['PointSpread'])[1],axis=1)

    # Replace with your Supabase project URL and API key
    supabase_url = 'https://ombuhcmutttxxjsyjerf.supabase.co'
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tYnVoY211dHR0eHhqc3lqZXJmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU5NzUxNTUsImV4cCI6MjA3MTU1MTE1NX0._SsqZCRWM-e77gqxsrZSuw1OPvIIpYxFoNFsPkBx_Gc"

    # Initialize the Supabase client
    client = supabase.Client(supabase_url, supabase_key)

    # write or update the week schedule
    table_name = "Odds"

    # table columns: key, AWAY, HOME, date, Season, Week 

    for index, row in odds_df.iterrows():

        # Data to insert (as a dictionary)
        data_to_insert = {
            "key": row['key'],
            'PointSpread': row['PointSpread'],
            'OverUnder': row['OverUnder'],
            'AwayTeamMoneyLine': row['AwayTeamMoneyLine'],
            'HomeTeamMoneyLine': row['HomeTeamMoneyLine'],
            "BookMakers":row['bookmakers'],
            'AWAY_OU':row['AWAY_OU'],
            'HOME_OU':row['HOME_OU']
        }

        # Perform the insert operation
        response_write = client.from_(table_name).insert([data_to_insert]).execute()