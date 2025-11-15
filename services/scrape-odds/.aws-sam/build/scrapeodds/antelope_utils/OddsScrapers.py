import requests
import json
import pandas as pd

def extract_stat_from_bookmaker_dict(bookmakers, book, stat, team):
    """
    Params:
    -------
        bookmakers: dict
        book: stat
        stat: str (h2h, speads, totals)
        team: str
    
    Returns:
    --------
        price: float
        point (Only for spreads, totals)
        
        
    Sample Debugging Code:
    ----------------------
    # can be brought into another cell for debugging
    
    API_KEY = '707089c36385cf209b8473df00b7770f'
    REGIONS = 'us'
    MARKETS = 'h2h,spreads,totals'
    ODDS_FORMAT = 'decimal'
    DATE_FORMAT = 'iso'
    SPORT = 'americanfootball_nfl'

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

    bookmakers = pd.DataFrame(odds_response.json())['bookmakers'][0]
    df = pd.DataFrame(bookmakers)

    book_df = df[df['key'] == 'draftkings'].reset_index()
    book_df['markets'][0]

    game_df = pd.DataFrame(book_df['markets'][0])

    bet_df = game_df[game_df['key']=='h2h'].reset_index()

    outcomes_df = pd.DataFrame(bet_df['outcomes'][0])
    outcomes_df
    
    
    """
    #print(bookmakers[0]['markets'])
    
    # dataframe made from bookmakers dictionary
    df = pd.DataFrame(bookmakers)

    # book df for just the book we are interested in
    book_df = df[df['key'] == book].reset_index()

    # game df with the odds displayed from a given sports book
    game_df = pd.DataFrame(book_df['markets'][0])
    # print(game_df)

    # bet df the given bet type
    bet_df = game_df[game_df['key']==stat].reset_index()
    #print(bet_df)

    # outcomes df - this is actually the odds
    outcomes_df = pd.DataFrame(bet_df['outcomes'][0])
    #print(outcomes_df)

    if stat == 'h2h':
        point = 'No Point Provided in ML scores'
        price = outcomes_df[outcomes_df['name'] == team]['price'].values[0]
        return price, point

    elif stat == 'totals':
        over_price = outcomes_df[outcomes_df['name'] == 'Over']['price'].values[0]
        over_point = outcomes_df[outcomes_df['name'] == 'Over']['point'].values[0]
        under_price = outcomes_df[outcomes_df['name'] == 'Under']['price'].values[0]
        under_point = outcomes_df[outcomes_df['name'] == 'Under']['point'].values[0]
        return over_price, over_point, under_price, under_point

    elif stat == 'spreads':
        point = outcomes_df[outcomes_df['name'] == team]['point'].values[0]
        price = outcomes_df[outcomes_df['name'] == team]['price'].values[0]
        return price, point

def calc_home_away_totals(OverUnder,PointSpread):
    
    away = (OverUnder + PointSpread) / 2
    home = OverUnder - away
    
    return away, home