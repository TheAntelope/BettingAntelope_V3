import requests
from urllib.request import urlopen
from datetime import datetime as dt
from bs4 import BeautifulSoup
import Levenshtein
import pickle
import boto3
import time
import sys
import cloudscraper
from typing import Optional, Dict
import re
# append utils to sys path
sys.path.append("/antelope_utils")
from antelope_utils.data_prep_utils import rename_team_str
import pandas as pd


def get_player_metadata(name, increment):
    """# 
    Scrapes player metadata from player sites like https://www.pro-football-reference.com/players/H/HillTy00/gamelog/
    returns the meta data
    
    """
    LastInitial = get_last_initial(name=name)
    NameKey = get_name_key(name, increment)
    url_str = 'https://www.pro-football-reference.com/players/' + LastInitial + '/' + NameKey + '/gamelog/'
    
    #get soup from url to get meta data
    # page = requests.get(url_str)
    # print(page)
    # soup = BeautifulSoup(page.content, "html.parser")
    # print(soup)
    
    #scrape player meta data (old version)
    # player_dict = scrape_player_meta_data(soup, name, url_str)

    player_dict = extract_player_header(url_str)
    print('Scraped Player Meta Data from NFL Ref:', player_dict)

    player_dict['Team'] = rename_team_str(player_dict['Team'])
    
    # add url to the player log
    # player_dict['url'] = url_str
    
    return player_dict

# mark for deletion if everything works with new function
def scrape_player_meta_data(soup, name, url_str):   
    
    """
    scrapes a soup of text from football reference player record
    
    Paramters:
    -------
    - soup: soup
    - name:str
    
    Outputs:
    --------
    - player_dict: dict
        dictionary with the players name, position and team scraped
    
    
    """
    
    result = soup.find_all("div", id="info")
    print(result)
    
    
    try: 
        # get position text
        pos_text = result[0].find_all("p")[1].text.strip()

        # clean out spaces characters
        pos_text = pos_text.replace('\n' , '').replace('\t' , '')

        # clean out throws (if shown)
        pos_text = pos_text.replace('Throws:' , '').replace('Right' , '').replace('Left' , '')

        # remove position
        pos_text = pos_text.replace('Position: ', '')

        position = pos_text
    
    except:
        #print('No Position text found:', name, url_str)
        position = "NA"
        
    try:
        # get team
        team_text = result[0].find_all("p")[3].text.strip()

        # remove team
        team_text = team_text.replace('Team: ', '')

        team = team_text
        team = rename_team_str(team)

        
    except:
        #print('No Team text found:', name, url_str)
        team = "NA"
        
    try:
        # get player name
        name = result[0].find_all('h1')[0].text.strip()
    except:
        #print('No Player Name found', name, url_str)
        name = 'NA'
    
    player_dict = {}
    player_dict = {
        'Name' : name,
        'Position': position,
        'Team': team
    }
    
    return player_dict

def get_last_initial(name):

    # split the string into a list 
    lst = name.split()

    # take first letter of second word
    # this will burn me eventually with NFL names
    LastInitial = lst[1][0]

    return LastInitial

def get_name_key(name, number_str):
    
    # remove hyphens
    # see https://www.pro-football-reference.com/players/S/StJuBe00.htm
    name = name.replace('-','')
    
    # split the string into a list 
    lst = name.split()

    # get first four letters of last name
    lastName = lst[1][0:4]

    #get first two letter of last name
    firstName = lst[0][0:2]

    # assign the number (everything so far has been 01)
    number = number_str

    nameKey = lastName + firstName + str(number)

    return nameKey

# verify player meta data
def verify_player_data_source(roster_name, roster_position, roster_team, scraped_name, scraped_position, scraped_team):
    """
    Confirms that the correct url on football reference was scraped
    and corresponds the the player, name position combination
    returned from the espn roster
    
    Inputs:
    -------
    roster_name: str
    roster_position: str
    roster_team: str
    scraped_name:str
    scraped_position: str
    scraped_team: str
    
    Outputs:
    --------
    match_dict: dict
        True if the meta data is confirmed
    
    """
    
    match_dict = {
        'Status': False,
        'Name_Matched': False,
        'Position_Matched':False,
        'Team_Matched':False,
        'LevenshteinDistance':0
    }
    
    # check if the player name is the same
    levDistance = Levenshtein.distance(roster_name, scraped_name)
    if roster_name == scraped_name:
        match_dict['Name_Matched'] = True
    elif levDistance < 6:
        match_dict['Name_Matched'] = True
        match_dict['LevenshteinDistance'] = levDistance    
    
    # check if the position is the same
    if roster_position.strip() == scraped_position.strip():
        match_dict['Position_Matched'] = True
    # adding this condition for the cordarrelle Patterson Case where we was WR most of 
    # his career but then changed to beast RB for ATL in 2021
    elif roster_position.strip() == 'RB' and scraped_position.strip() == 'WR':
        match_dict['Position_Matched'] = True
    elif roster_position.strip() in scraped_position:
        match_dict['Position_Matched'] = True
   
    elif roster_position.strip() == 'CB' and scraped_position.strip() == 'DB': 
        match_dict['Position_Matched'] = True
    
    elif roster_position.strip() == 'SS' and scraped_position.strip() in ['S', 'FS','DB']: 
        match_dict['Position_Matched'] = True
        
    elif roster_position.strip() == 'DE' and scraped_position.strip() in ['LB', 'OLB', 'EDGE', 'DT', 'DL']: 
        match_dict['Position_Matched'] = True   
        
    elif roster_position.strip() == 'LB' and scraped_position.strip() in ['DT','DL', 'DE', 'EDGE']: 
        match_dict['Position_Matched'] = True   
    
    # check if the team is the same
    # remove "Unsigned free agent from text is available",
    scraped_team = scraped_team.replace('(Unsigned draft pick)','')
    
    if roster_team.strip() == scraped_team.strip():
        match_dict['Team_Matched'] = True
        
        
    # if all status are true
    if all(
        [match_dict['Name_Matched'] == True,
        match_dict['Position_Matched'] == True,
        match_dict['Team_Matched'] == True
        ]
    ) == True:
        match_dict['Status'] = True
        
    return match_dict['Status'], match_dict['Name_Matched'], match_dict['Position_Matched'], match_dict['Team_Matched'], match_dict['LevenshteinDistance']

def verify_roster(roster_df_in, TEAM_NAME, currentSeason, currentWeek):

    """
    Verifies roster collected from espn against player logs on football reference
    
    Logs files for verified and unverifed rosters in S3
    """

    scraped_names = []
    scraped_positions = []
    scraped_teams = []
    match_verifications = []
    matched_urls = []

    for col, row in roster_df_in.iterrows():
        player_dict = {}
        name = row['Name']
        position = row[0]

        match_verification = False
        increment = 0

        # if the match cannot be verified
        while match_verification == False:
            
            # look up player in table. if the player exists, use his football reference URL. 
            time.sleep(7)
            number_str = '0' + str(increment)
            increment += 1
            # get player meta data
            player_dict = get_player_metadata(name, number_str)

            # verify the scrape
            match_verification = verify_player_data_source(
                roster_name=row[1], roster_position=position, roster_team=row[3], 
                scraped_name=player_dict['Name'], scraped_position=player_dict['Position'], scraped_team=player_dict['Team'])[0]

            # break the while loop just in case
            if increment > 5:
                player_dict['Name'] = None
                player_dict['Position'] = None
                player_dict['Team'] = None
                player_dict['url'] = None
                print('Player Not Verified:', row['Team'], name, position, row['Status'])
                break


        scraped_names.append(player_dict['Name'])
        scraped_positions.append(player_dict['Position'])
        scraped_teams.append(player_dict['Team'])
        match_verifications.append(match_verification)
        matched_urls.append(player_dict['url'])
        if player_dict['Name'] == None:
            pass
        else:
            print('Verified Player Data for :',  player_dict['Team'],player_dict['Name'], player_dict['Position'], row['Status'])

    roster_df_in['ScrapedName'] = scraped_names
    roster_df_in['ScrapedPosition'] = scraped_positions
    roster_df_in['ScrapedTeam'] = scraped_teams
    roster_df_in['match_verification'] = match_verifications
    roster_df_in['url'] = matched_urls

    # save depth charts and upload them to s3

    # variables
    s3 = boto3.resource("s3")
    save_Str = dt.now().strftime("%b %d %Y %H:%M:%S")
    latest_save_str = TEAM_NAME + '_DepthChart_'+save_Str +'.pkl'

    # save unverified depth chart
    pickle_byte_obj = pickle.dumps(roster_df_in)
    key = 'dev/ExtractedStats/DepthCharts/Unverified/' + str(currentSeason) + '/' + str(currentWeek) + '/' + TEAM_NAME + "/"
    s3.Bucket('antelope').put_object(Key=(key+latest_save_str), Body=pickle_byte_obj)

    # filter out the unverified players
    roster_df_out = roster_df_in[roster_df_in['match_verification'] == True]

    # save verified depth chart
    pickle_byte_obj = pickle.dumps(roster_df_in)
    key = 'dev/ExtractedStats/DepthCharts/Verified/' + str(currentSeason) + '/' + str(currentWeek) + '/' + TEAM_NAME + "/"
    s3.Bucket('antelope').put_object(Key=(key+latest_save_str), Body=pickle_byte_obj)
    
    return roster_df_out

def extract_player_header(url: str, html: Optional[str] = None, timeout: int = 15) -> Dict[str, Optional[str]]:
    """
    Return {'Name', 'Position', 'Team', 'url'} from a PFR player gamelog URL.
    If html is provided it will be parsed directly; otherwise the page is fetched with cloudscraper.
    """
    if html is None:
        scraper = cloudscraper.create_scraper()
        r = scraper.get(url, timeout=timeout)
        r.raise_for_status()
        html = r.text

    soup = BeautifulSoup(html, "lxml")

    # Name: usually in <h1 itemprop="name"> or first <h1>
    name_tag = soup.find("h1", attrs={"itemprop": "name"}) or soup.find("h1")
    name = name_tag.get_text(strip=True) if name_tag else None

    position = None
    team = None

    # Meta block contains Position and Team info
    meta = soup.select_one("#meta")
    if meta:
        # Try paragraphs in #meta
        for p in meta.find_all("p"):
            text = p.get_text(" ", strip=True)
            # Position: ... (e.g. "Position: RB")
            m = re.search(r"Position[:\s]*([A-Za-z0-9/ ,]+)", text)
            if m and not position:
                position = m.group(1).strip()
            # Team: ... (there will usually be an <a> to the team page)
            if "Team" in text and not team:
                a = p.find("a")
                if a:
                    team = a.get_text(strip=True)
                else:
                    # fallback: take text after 'Team'
                    parts = text.split("Team")
                    if len(parts) > 1:
                        team = parts[-1].replace(":", "").strip()

    # Fallback: look for a link to /teams/ anywhere
    if not team:
        team_link = soup.select_one("a[href*='/teams/']")
        if team_link:
            team = team_link.get_text(strip=True)

    # Fallback for position: small labels or 'POSITION' text
    if not position:
        pos_tag = soup.find(string=re.compile(r"\b(Position|Pos)\b", re.I))
        if pos_tag:
            # try to grab following text
            parent = pos_tag.parent
            if parent:
                txt = parent.get_text(" ", strip=True)
                m = re.search(r"(Position|Pos)[:\s]*([A-Za-z0-9/ ,]+)", txt, re.I)
                if m:
                    position = m.group(2).strip()

    return {
        "Name": name or "NA",
        "Position": position or "NA",
        "Team": team or "NA",
        "url": url
    }