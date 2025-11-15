

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

    books = ["draftkings", "fanduel", "mybookieag", "betonlineag", "lowvig", "betrivers", "betus", "bovada", "betmgm"]

    for book in books:
        PS_str = "PointSpread_" + book
        PS_HT_price = "PointSpreadHTPrice_" + book
        PS_AT_price = "PointSpreadATPrice_" + book
        OU_str = "OverUnder_" + book
        OU_Over_price = "OverPrice_" + book
        OU_Under_price = "UnderPrice_" + book
        ATML_str = "AwayTeamMoneyLine_" + book
        HTML_str = "HomeTeamMoneyLine_" + book
        odds_df[PS_str] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'spreads', row['home_team'])[1], axis = 1)
        odds_df[PS_HT_price] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'spreads', row['home_team'])[0], axis = 1)
        odds_df[PS_AT_price] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'spreads', row['away_team'])[0], axis = 1)
        odds_df[OU_str] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'totals', row['home_team'])[1], axis = 1)
        odds_df[OU_Over_price] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'totals', row['home_team'])[0], axis = 1)
        odds_df[OU_Under_price] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'totals', row['home_team'])[2], axis = 1)
        odds_df[ATML_str] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'h2h', row['away_team'])[0], axis = 1)
        odds_df[HTML_str] = odds_df.apply(lambda row: extract_stat_from_bookmaker_dict(row['bookmakers'], 'draftkings', 'h2h', row['home_team'])[0], axis = 1)


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
        'BookMakers': row["bookmakers"],
        'PointSpread': row["PointSpread"],
        'OverUnder': row["OverUnder"],
        'AwayTeamMoneyLine': row["AwayTeamMoneyLine"],
        'HomeTeamMoneyLine': row["HomeTeamMoneyLine"],
        'PointSpread_draftkings': row["PointSpread_draftkings"],
        'PointSpreadHTPrice_draftkings': row["PointSpreadHTPrice_draftkings"],
        'PointSpreadATPrice_draftkings': row["PointSpreadATPrice_draftkings"],
        'OverUnder_draftkings': row["OverUnder_draftkings"],
        'OverPrice_draftkings': row["OverPrice_draftkings"],
        'UnderPrice_draftkings': row["UnderPrice_draftkings"],
        'AwayTeamMoneyLine_draftkings': row["AwayTeamMoneyLine_draftkings"],
        'HomeTeamMoneyLine_draftkings': row["HomeTeamMoneyLine_draftkings"],
        'PointSpread_fanduel': row["PointSpread_fanduel"],
        'PointSpreadHTPrice_fanduel': row["PointSpreadHTPrice_fanduel"],
        'PointSpreadATPrice_fanduel': row["PointSpreadATPrice_fanduel"],
        'OverUnder_fanduel': row["OverUnder_fanduel"],
        'OverPrice_fanduel': row["OverPrice_fanduel"],
        'UnderPrice_fanduel': row["UnderPrice_fanduel"],
        'AwayTeamMoneyLine_fanduel': row["AwayTeamMoneyLine_fanduel"],
        'HomeTeamMoneyLine_fanduel': row["HomeTeamMoneyLine_fanduel"],
        'PointSpread_mybookieag': row["PointSpread_mybookieag"],
        'PointSpreadHTPrice_mybookieag': row["PointSpreadHTPrice_mybookieag"],
        'PointSpreadATPrice_mybookieag': row["PointSpreadATPrice_mybookieag"],
        'OverUnder_mybookieag': row["OverUnder_mybookieag"],
        'OverPrice_mybookieag': row["OverPrice_mybookieag"],
        'UnderPrice_mybookieag': row["UnderPrice_mybookieag"],
        'AwayTeamMoneyLine_mybookieag': row["AwayTeamMoneyLine_mybookieag"],
        'HomeTeamMoneyLine_mybookieag': row["HomeTeamMoneyLine_mybookieag"],
        'PointSpread_betonlineag': row["PointSpread_betonlineag"],
        'PointSpreadHTPrice_betonlineag': row["PointSpreadHTPrice_betonlineag"],
        'PointSpreadATPrice_betonlineag': row["PointSpreadATPrice_betonlineag"],
        'OverUnder_betonlineag': row["OverUnder_betonlineag"],
        'OverPrice_betonlineag': row["OverPrice_betonlineag"],
        'UnderPrice_betonlineag': row["UnderPrice_betonlineag"],
        'AwayTeamMoneyLine_betonlineag': row["AwayTeamMoneyLine_betonlineag"],
        'HomeTeamMoneyLine_betonlineag': row["HomeTeamMoneyLine_betonlineag"],
        'PointSpread_lowvig': row["PointSpread_lowvig"],
        'PointSpreadHTPrice_lowvig': row["PointSpreadHTPrice_lowvig"],
        'PointSpreadATPrice_lowvig': row["PointSpreadATPrice_lowvig"],
        'OverUnder_lowvig': row["OverUnder_lowvig"],
        'OverPrice_lowvig': row["OverPrice_lowvig"],
        'UnderPrice_lowvig': row["UnderPrice_lowvig"],
        'AwayTeamMoneyLine_lowvig': row["AwayTeamMoneyLine_lowvig"],
        'HomeTeamMoneyLine_lowvig': row["HomeTeamMoneyLine_lowvig"],
        'PointSpread_betrivers': row["PointSpread_betrivers"],
        'PointSpreadHTPrice_betrivers': row["PointSpreadHTPrice_betrivers"],
        'PointSpreadATPrice_betrivers': row["PointSpreadATPrice_betrivers"],
        'OverUnder_betrivers': row["OverUnder_betrivers"],
        'OverPrice_betrivers': row["OverPrice_betrivers"],
        'UnderPrice_betrivers': row["UnderPrice_betrivers"],
        'AwayTeamMoneyLine_betrivers': row["AwayTeamMoneyLine_betrivers"],
        'HomeTeamMoneyLine_betrivers': row["HomeTeamMoneyLine_betrivers"],
        'PointSpread_betus': row["PointSpread_betus"],
        'PointSpreadHTPrice_betus': row["PointSpreadHTPrice_betus"],
        'PointSpreadATPrice_betus': row["PointSpreadATPrice_betus"],
        'OverUnder_betus': row["OverUnder_betus"],
        'OverPrice_betus': row["OverPrice_betus"],
        'UnderPrice_betus': row["UnderPrice_betus"],
        'AwayTeamMoneyLine_betus': row["AwayTeamMoneyLine_betus"],
        'HomeTeamMoneyLine_betus': row["HomeTeamMoneyLine_betus"],
        'PointSpread_bovada': row["PointSpread_bovada"],
        'PointSpreadHTPrice_bovada': row["PointSpreadHTPrice_bovada"],
        'PointSpreadATPrice_bovada': row["PointSpreadATPrice_bovada"],
        'OverUnder_bovada': row["OverUnder_bovada"],
        'OverPrice_bovada': row["OverPrice_bovada"],
        'UnderPrice_bovada': row["UnderPrice_bovada"],
        'AwayTeamMoneyLine_bovada': row["AwayTeamMoneyLine_bovada"],
        'HomeTeamMoneyLine_bovada': row["HomeTeamMoneyLine_bovada"],
        'PointSpread_betmgm': row["PointSpread_betmgm"],
        'PointSpreadHTPrice_betmgm': row["PointSpreadHTPrice_betmgm"],
        'PointSpreadATPrice_betmgm': row["PointSpreadATPrice_betmgm"],
        'OverUnder_betmgm': row["OverUnder_betmgm"],
        'OverPrice_betmgm': row["OverPrice_betmgm"],
        'UnderPrice_betmgm': row["UnderPrice_betmgm"],
        'AwayTeamMoneyLine_betmgm': row["AwayTeamMoneyLine_betmgm"],
        'HomeTeamMoneyLine_betmgm': row["HomeTeamMoneyLine_betmgm"],
        'key': row["key"],
        'AWAY_OU': row["AWAY_OU"],
        'HOME_OU': row["HOME_OU"]
        }

        # Perform the insert operation
        response_write = client.from_(table_name).insert([data_to_insert]).execute()

if __name__ == "__main__":
    # You can pass a test event and context here
    lambda_handler({}, None)