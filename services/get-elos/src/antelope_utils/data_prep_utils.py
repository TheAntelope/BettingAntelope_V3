import pandas as pd
from datetime import date
from datetime import timedelta
from calendar import WEDNESDAY
from calendar import THURSDAY
from calendar import SATURDAY
from calendar import SUNDAY
from calendar import MONDAY
from datetime import datetime as dt
from datetime import timedelta
import math
import sys
sys.path.append("/antelope_utils")
from antelope_utils.s3_utils import list_all_objects_version

def get_stadium(team):
    """for a given team returns the stadium city name and/or coordinates"""
    
    stadiums_dict = {
        'CAR': {
            'city':'Charlotte, USA',    
            'type':'Outdoor'
            },
        'HOU': {
            'city':'Houston, USA',   
            'type':'Outdoor'
            },
        'WAS': {
            'city':'Washington,DC , USA',    
            'type':'Outdoor'
            },
        'ATL': {
            'city':'Charlotte, USA',
            'type':'Dome'
            },
        'MIA': {
            'city':'Atlanta, USA', 
            'type':'Outdoor'
            },
        'DET': {
            'city':'Charlotte, USA',
            'type':'Dome'
            },
        'CIN': {
            'city':'Cincinnati, USA',  
            'type':'Outdoor'
            },
        'NYJ': {
            'city':'East Rutherford, USA', 
            'type':'Outdoor'
            },
        'NYG': {
            'city':'East Rutherford, USA', 
            'type':'Outdoor'
            },
        'CHI': {
            'city':'Chicago, USA',  
            'type':'Outdoor'
            },
        'MIN': {
            'city':'Minneapolis, USA',      
            'type':'Dome'
            },
        'ARI': {
            'city':'Glendale, USA',  
            'type':'Dome'
            },
        'TEN': {
            'city':'Nashville, USA',  
            'type':'Outdoor'
            },
        'LAC': {
            'city':'Inglewood, USA',  
            'type':'Outdoor'
            },
        'LAR': {
            'city':'Inglewood, USA',  
            'type':'Outdoor'
            },
        'DAL': {
            'city':'Arlington, USA',  
            'type':'Dome'
            },
        'SEA': {
            'city':'Seattle, USA',  
            'type':'Outdoor'
            },
        'KC': {
            'city':'Kansas City, USA',  
            'type':'Outdoor'
            },
        'JAX': {
            'city':'Jacksonville, USA',  
            'type':'Outdoor'
            },
        'BAL': {
            'city':'Baltimore, USA',  
            'type':'Outdoor'
            },
        'PIT': {
            'city':'Pittsburgh, USA',  
            'type':'Outdoor'
            },
        'CLE': {
            'city':'Cleveland, USA',  
            'type':'Outdoor'
            },
        'NO': {
            'city':'New Orleans, USA',  
            'type':'Dome'
            },
        'SF': {
            'city':'Santa Clara, USA',  
            'type':'Outdoor'
            },
        'LV': {
            'city':'Las Vegas, USA',  
            'type':'Dome'
            },
        'DEN': {
            'city':'Denver, USA',  
            'type':'Outdoor'
            },
        'GB': {
            'city':'Green Bay, USA',  
            'type':'Outdoor'
            },
        'BUF': {
            'city':'Buffalo, USA',  
            'type':'Outdoor'
            },
        'PHI': {
            'city':'Philadelphia, USA',  
            'type':'Outdoor'
            },
        'IND': {
            'city':'Indianapolis, USA',  
            'type':'Dome'
            },
        'NE': {
            'city':'Foxborough, USA',  
            'type':'Outdoor'
            },
        'TB': {
            'city':'Tampa, USA',  
            'type':'Outdoor'
            },
    }
    
    stadium = stadiums_dict[team]
    
    return stadium

def convert_dome_wind(team, wind):
    """
    Converts wind to 0 in games played in a dome
    """
    if team in ['DAL', 'DET', 'NO', 'ATL', 'IND', 'ARI', 'LAC', 'LAR']:
        return 0
    else:
        return wind
    
def schedule_prep(schedule, currentWeek):
    schedule = schedule[schedule['Week'] == currentWeek]
    # rename windspeed to wind and drop the windspeed column
    schedule['Wind'] = schedule['WindSpeed']
    schedule = schedule.drop('WindSpeed', axis = 1)
    # remove wind in dome games
    schedule['Wind'] = schedule.apply(lambda row: convert_dome_wind(row['HomeTeam'], row['Wind']), axis = 1)
    
    return schedule

def rename_teams(df, teamCol):
    
    #AFC East
    df[teamCol].replace('New England Patriots','NE',inplace=True)
    df[teamCol].replace('Miami Dolphins','MIA',inplace=True)
    df[teamCol].replace('Buffalo Bills','BUF',inplace=True)
    df[teamCol].replace('New York Jets','NYJ',inplace=True)
    
    #AFC North
    df[teamCol].replace('Baltimore Ravens','BAL',inplace=True)
    df[teamCol].replace('Pittsburgh Steelers','PIT',inplace=True)
    df[teamCol].replace('Cleveland Browns','CLE',inplace=True)
    df[teamCol].replace('Cincinnati Bengals','CIN',inplace=True)
    
    #AFC South
    df[teamCol].replace('Houston Texans','HOU',inplace=True)
    df[teamCol].replace('Jacksonville Jaguars','JAX',inplace=True)
    df[teamCol].replace('Tennessee Titans','TEN',inplace=True)
    df[teamCol].replace('Indianapolis Colts','IND',inplace=True)
    
    #AFC West
    df[teamCol].replace(['OAK','Oakland Raiders','Las Vegas Raiders'],'LV',inplace=True)
    df[teamCol].replace('Kansas City Chiefs','KC',inplace=True)
    df[teamCol].replace('Denver Broncos','DEN',inplace=True)
    df[teamCol].replace('Los Angeles Chargers','LAC',inplace=True)
    
    #NFC East
    df[teamCol].replace('New York Giants','NYG',inplace=True)
    df[teamCol].replace('Philadelphia Eagles','PHI',inplace=True)
    df[teamCol].replace('Dallas Cowboys','DAL',inplace=True)
    df[teamCol].replace(['Washington Redskins','Washington','WSH','Washington Football Team'],'WAS',inplace=True)
    
    #NFC North
    df[teamCol].replace('Green Bay Packers','GB',inplace=True)
    df[teamCol].replace('Minnesota Vikings','MIN',inplace=True)
    df[teamCol].replace('Detroit Lions','DET',inplace=True)
    df[teamCol].replace('Chicago Bears','CHI',inplace=True)
    
    #NFC South
    df[teamCol].replace(["NOR",'New Orleans Saints'],'NO',inplace=True)
    df[teamCol].replace('Atlanta Falcons','ATL',inplace=True)
    df[teamCol].replace('Carolina Panthers','CAR',inplace=True)
    df[teamCol].replace('Tampa Bay Buccaneers','TB',inplace=True)
    
    #NFC West
    df[teamCol].replace('San Francisco 49ers','SF',inplace=True)
    df[teamCol].replace('Seattle Seahawks','SEA',inplace=True)
    df[teamCol].replace('Los Angeles Rams','LAR',inplace=True)
    df[teamCol].replace('Arizona Cardinals','ARI',inplace=True)
    
    return df

def assignSeasonOU(team, df, currentSeason):
    """
    inputs:
    - team: Team Abbreviation
    - df: lookup toable for Season OU
    - currentSeason
    
    returns: Season OU
    """
    
    col_str = str(currentSeason) + ' Win Total'
    
    team_df = df[df['Team'] == team]
    
    OU = team_df[col_str].sum()
    
    return OU

def add_stat(playerID, stat, totalStats):
    return totalStats[totalStats['PlayerID'] == playerID][stat].sum()

def rename_positions(df):
    print('renaming positions')
    #Rename Running Backs
    df = df.replace('FB','RB')
    
    #Rename Wide Receivers
    df = df.replace(['RWR', 'LWR', 'SWR'],'WR')
    
    #Rename OL
    df = df.replace(['NT','G','C', 'LT','RT', 'OL', 'OT', 'RG','LG'],'OL')
    
    #Rename CB/SS
    df = df.replace(['SS','S','CB','FS','RCB','LCB'],'CB/SS')
    
    #DE / LB
    df = df.replace(['DB','DE','OLB','ILB','OLB','LOLB','LDE','ROLB','RDE','DT','LB'],'DE/LB')
    
    return df

def undict(eff_dict):
    return eff_dict

def process_team_stats_from_dict(holding_dict, stat):
    return holding_dict[stat]

def add_team_score(team, stat, season, week, teamStats):
    """
    Takes in Team Stats to Return a specific stat, typically score
    """
    try: 
        df = teamStats
        result = df[df.Team == team][stat].sum()
        return result
    except: return 0

def rename_team_str(team):
    """
    takes a string and renames to the NFL abbreviated name
    
    """
    teams_dict = {
        #AFC East
        'New England Patriots' : 'NE',
        'NE Patriots' : 'NE',
        'Miami Dolphins' : 'MIA',
        'MIA Dolphins' : 'MIA',
        'Buffalo Bills' : 'BUF',
        'BUF Bills': 'BUF',
        'New York Jets' : 'NYJ',
        'NY Jets' : 'NYJ',
        "NWE":"NE",
        
        #AFC North
        'Baltimore Ravens' : 'BAL',
        'Pittsburgh Steelers' : 'PIT',
        'Cleveland Browns' : 'CLE',
        'Cincinnati Bengals' : 'CIN',
        'BAL Ravens' : 'BAL',
        'PIT Steelers' : 'PIT',
        'CLE Browns' : 'CLE',
        'CIN Bengals' : 'CIN',
        
        #AFC South
        'Houston Texans' : 'HOU',
        'Jacksonville Jaguars' : 'JAX',
        'Tennessee Titans' : 'TEN',
        'Indianapolis Colts' : 'IND',
        'HOU Texans' : 'HOU',
        'JAX Jaguars' : 'JAX',
        'TEN Titans' : 'TEN',
        'IND Colts' : 'IND',
        
        #AFC West
        'OAK' : 'LV',
        'Oakland Raiders' : 'LV',
        'Las Vegas Raiders' : 'LV',
        'LV Raiders' : 'LV',
        'Kansas City Chiefs' : 'KC',
        'KC Chiefs' : 'KC',
        'Denver Broncos' : 'DEN',
        'Los Angeles Chargers' : 'LAC',
        'San Diego Chargers': 'LAC',
        'DEN Broncos' : 'DEN',
        'LA Chargers' : 'LAC',
        "KAN":"KC",
        "LVR":"LV",
        
        #NFC East
        'New York Giants' : 'NYG',
        'Philadelphia Eagles' : 'PHI',
        'Dallas Cowboys' : 'DAL',
        'NY Giants' : 'NYG',
        'PHI Eagles' : 'PHI',
        'DAL Cowboys' : 'DAL',
        'Washington Redskins': 'WAS',
        'Washington Commanders': 'WAS',
        'Washington' : 'WAS',
        'WSH' : 'WAS',
        'Washington Football Team' : 'WAS',
        'WAS Commanders' : 'WAS',
        'WAS Football Team' : 'WAS',
        
        #NFC North
        'Green Bay Packers' : 'GB',
        'Minnesota Vikings' : 'MIN',
        'Detroit Lions' : 'DET',
        'Chicago Bears' : 'CHI',
        'GB Packers' : 'GB',
        'MIN Vikings' : 'MIN',
        'DET Lions' : 'DET',
        'CHI Bears' : 'CHI',
        'GNB' : 'GB',
        
        #NFC South
        'New Orleans Saints' : 'NO',
        'Atlanta Falcons' : 'ATL',
        'Carolina Panthers' : 'CAR',
        'Tampa Bay Buccaneers' : 'TB',
        'NO Saints' : 'NO',
        'ATL Falcons' : 'ATL',
        'CAR Panthers' : 'CAR',
        'TB Buccaneers' : 'TB',
        "NOR":"NO",
        "TAM":"TB",
        
        #NFC West
        'San Francisco 49ers' : 'SF',
        'Seattle Seahawks' : 'SEA',
        'Los Angeles Rams' : 'LAR',
        'St. Louis Rams': 'LAR',
        'Arizona Cardinals' : 'ARI',
        'SF 49ers' : 'SF',
        'SEA Seahawks' : 'SEA',
        'LA Rams' : 'LAR',
        'ARI Cardinals' : 'ARI',
        "SFO":"SF",
    }

    new_team = team
    
    #iterate though the teams to reanme the string
    for value in teams_dict:
        new_team = new_team.replace(value, teams_dict[value])
    
    return new_team

def get_date_for_last_day(day, date):
    """
    Parameters: day (eg: THURSDAY)
    
    date: datetme
        datetime.date(2022, 11, 2)
    """
    # find day
    
    if day == 'THURSDAY':
        today = date
        offset = (today.weekday() - THURSDAY) % 7
        last_day = today - timedelta(days=offset)
        print('last THURSDAY', last_day)
    elif day == 'WEDNESDAY':
        today = date
        offset = (today.weekday() - WEDNESDAY) % 7
        last_day = today - timedelta(days=offset + 7)
        print('last WEDNESDAY', last_day)
    elif day == 'SATURDAY':
        today = date
        offset = (today.weekday() - SATURDAY) % 7
        last_day = today - timedelta(days=offset)
        print('last SATURDAY', last_day)
    elif day == 'SUNDAY':
        today = date
        offset = (today.weekday() - SUNDAY) % 7
        last_day = today - timedelta(days=offset)
        print('last SUNDAY', last_day)
    elif day == 'MONDAY':
        today = date
        offset = (today.weekday() - MONDAY) % 7
        last_day = today - timedelta(days=offset) #monday has a different offset because its techincally in this week
        print('last MONDAY', last_day)
        
    return last_day

# get latest version id
def get_lastest_versionID_for_day(bucket, key, day, date):
    """
    loops through versions for a given s3 file and returns the latest file key and versions
    fora given day
    
    Paramaters:
    -----------
        bucket: str
        key: str
        day: str (eg: THURDAY)
        date: datetime (eg: datetime.date(2022, 11, 2))
    
    Returns:
    ---------
        key: str
        versionID: str
        max_date: str
        
    
    """
    # get all versions of the file
    versions = list_all_objects_version("antelope",key)
    
    # find latest thursday
    last_day = get_date_for_last_day(day, date)  
    
    # create a list of day dates to get the latest thursday data
    day_files = []
    for i in range(0,len(versions['Versions'])):
        if versions['Versions'][i]['LastModified'].date() == last_day:
            day_files.append(
                (versions['Versions'][i]['LastModified'], # date last modified
                 versions['Versions'][i]['Key'], # key
                 versions['Versions'][i]['VersionId'] # versionID required for collecting specific version
                ))
        else:
            pass
        
    # parse thursday files    
    dates = []
    keys = []
    versionIDs = []
    for file in day_files:
        dates.append(file[0])
        keys.append(file[1])
        versionIDs.append(file[2])

    key = keys[dates.index(max(dates))]
    versionID = versionIDs[dates.index(max(dates))]
    
    max_date = max(dates)
    
    return key, versionID, max_date

season_start_dates =  {
        '2012': dt(2012, 9, 5),
        '2013': dt(2013, 9, 5),
        '2014': dt(2014, 9, 4),
        '2015': dt(2015, 9, 10),
        '2016': dt(2016, 9, 8),
        '2017': dt(2017, 9, 7),
        '2018': dt(2018, 9, 6),
        '2019': dt(2019, 9, 5),
        '2020': dt(2020, 9, 10),
        '2021': dt(2021, 9, 9),
        '2022': dt(2022, 9, 8),
        '2023': dt(2023, 9, 7),
        '2024': dt(2024, 9, 5),
        '2025': dt(2025, 9, 4)
    }

def convert_date_to_nfl_week(date):
    
    #date = dt.strptime(date, '%Y-%m-%d')
    if date.month in [1,2]:
        year = date.year - 1
    else: 
        year = date.year
    
    days_into_season = date - season_start_dates[str(year)]
    
    weeks_into_season = days_into_season.days / 7
    
    if weeks_into_season < 0:
        return 1
    else:
        return math.floor(weeks_into_season + 1)
    
def calculate_week_date(season, week):
    """
    returns date for last day in the NFL week(Wednesday)
    """
    
    first_day_of_season = season_start_dates[str(season)]
    
    # first day of season + 7 x weeks - 1 --> should we a wednesday
    end_of_week = first_day_of_season + timedelta(days=week * 7) - timedelta(days=1)
    
    return end_of_week.date()

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

def process_dates_from_football_reference(date_str):
    "Receives date in 'MonthDay' format and returns data in yyyy-mm-dd format"

    try:
        dt.strptime(date_str, "%Y-%m-%d")
        print("Valid format: YYYY-MM-DD")
        return date_str
    except ValueError:
        print("Invalid format") 

        # Add the current year
        current_year = dt.now().year
        date_str_with_year = date_str + str(current_year)
    
        # Parse the date string to a datetime object
        date_obj = dt.strptime(date_str_with_year, "%B%d%Y")
    
        # Convert to the desired format (yyyy-mm-dd)
        formatted_date = date_obj.strftime("%Y-%m-%d")
        
        return formatted_date


def adjust_date(date_str):
    # Convert the string to a datetime object
    date = dt.strptime(date_str, "%Y-%m-%d")
    
    # Check if the month is January (1) or February (2)
    if date.month in [1, 2]:
        # Subtract one year
        adjusted_date = date.replace(year=date.year - 1)
    else:
        adjusted_date = date
    
    # Convert back to a string if needed
    return adjusted_date.strftime("%Y-%m-%d")

def process_dates_from_football_reference_historic(date_str, season):
    "Receives date in 'MonthDay' format and returns data in yyyy-mm-dd format"
    

    # Add the current year
    current_year = season
    date_str_with_year = date_str.replace(" ", "") + str(current_year) 
    print("Date Str:", date_str_with_year)

    # Parse the date string to a datetime object
    date_obj = dt.strptime(date_str_with_year, "%B%d%Y")

    # Convert to the desired format (yyyy-mm-dd)
    formatted_date = date_obj.strftime("%Y-%m-%d")
    print(formatted_date)
    
    adjusted_date = adjust_date(formatted_date)
    print(adjusted_date)
    
    return adjusted_date