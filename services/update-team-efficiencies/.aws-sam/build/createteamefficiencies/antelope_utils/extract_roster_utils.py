import pandas as pd
import requests
from bs4 import BeautifulSoup
import Levenshtein
import time

def read_roster(PATH, TEAM):
    df = pd.read_html(PATH,header=0)

    # get the list of positions on the depth chart from espn
    positions_list = [i[0] for i in df[0].values]
    # need to insert QB at the beginning because of a blank header
    positions_list.insert(0,"QB")

    roster_df = pd.DataFrame()
    roster_df['Position'] = positions_list

    # join columsn
    roster_df = roster_df.join(df[1])
    
    # add team to dataframe
    roster_df['Team'] = TEAM
    
    return roster_df
    
def read_def_roster(PATH, TEAM):
    df = pd.read_html(PATH,header=0)
    # get the list of positions on the depth chart from espn
    positions_list = [i[0] for i in df[2].values]
    # need to insert QB at the beginning because of a blank header
    positions_list.insert(0,"LDE")

    roster_df = pd.DataFrame()
    roster_df['Position'] = positions_list

    # join columsn
    roster_df = roster_df.join(df[3])
    
    # add team to dataframe
    roster_df['Team'] = TEAM
    
    return roster_df

def insert_position(df, position, name, status, team):
    new_row = {
        'Position': position,
        'Name': name,
        'Status': status,
        'Team': team
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True, sort=False)
    return df

def check_status(name):
    # check for status
    # possible Statusses:
        # P : Probable
        # Q : Questionable
        # O : Out
        # PUP : Physically unable to perform
        # SUS : Suspended by NFL or current team
        # IR : Injured Reserve

    # if there is no player, no need for status    
    if name == '-':
        return None

    if name[-2:]== ' P':
        status = "Probable"
    elif name[-2:] == ' Q':
        status = "Questionable"
    elif name[-2:] == ' D':
        status = "Doubtful"
    elif name[-2:] == ' O':
        status = "Out"
    elif name[-4:] == ' PUP':
        status = 'Physically unable to perform'
    elif name[-4:] == ' SUS':
        status = 'Suspended by NFL or current team'
    elif name[-5:] == ' SUSP':
        status = 'Suspended by NFL or current team'
    elif name[-3:] == ' IR':
        status = "Injured Reserve"
    else:
        status = "Healthy"
    return status

def create_roster_df(TEAM):
    """
    reads, groups and returns a roster_df for a given team
    
    Params:
    -------
    - TEAM: str
    
    Returns:
    -------
    - roster_df: df
        data frame has 4 columns for a given team
        - Position
        - Name
        - Status
        - Team
    
    """
    # variables
    healthy_statusses = ['Healthy', 'Questionable']
    if TEAM == 'WAS':
        PATH = 'https://www.espn.com/nfl/team/depth/_/name/wsh/washington-commanders'
    else:
        PATH = 'https://www.espn.com/nfl/team/depth/_/name/' + TEAM
    
    # read_roster
    df = read_roster(PATH, TEAM)

    # read def roster
    def_df = read_def_roster(PATH, TEAM)

    # append o_df and d_df
    # DataFrame.append was removed in pandas 2.x — use pd.concat instead
    df = pd.concat([df, def_df], ignore_index=True, sort=False)
    
    grouped_df = df.groupby(['Position', 'Starter', '2nd', '3rd', '4th' , 'Team']).count()
    
    roster_df = pd.DataFrame(columns = ['Position', 'Name', 'Status'])
    for row, col in grouped_df.iterrows():

        name = row[1]
        position = row[0]
        status = check_status(name)
        team = row[5]

        # get the starting QB
        if position == "QB":
            names = list(row[1:5])
            healthy_names = [check_status(i) in healthy_statusses for i in names]
            healthy_idx = healthy_names.index(True)
            # Append the first healthy QB
            roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="QB", status=check_status(names[healthy_idx]),team=team)

        # get RB1 & RB2
        if position == 'RB':
            names = list(row[1:5])
            # while RBs < 2:
            RBs = []
            while len(RBs) < 2:
                print(len(RBs))
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                print("Healthy RBs:", names, healthy_names)
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy RB
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="RB", status=check_status(names[healthy_idx]),team=team)
                    RBs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break

        # get TE1 & TE2
        if position == 'TE':
            names = list(row[1:5])
            # while TEs < 2:
            TEs = []
            while len(TEs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                healthy_idx = healthy_names.index(True)
                # Append the first healthy TE
                roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="TE", status=check_status(names[healthy_idx]),team=team)
                TEs.append(names[healthy_idx])
                names.remove(names[healthy_idx])
                if names.count(True) == 0:
                    break

        # Get WR
        if position == "WR":
            names = list(row[1:5])
            # while WRs < 5:
            WRs = []
            while len(WRs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy WR
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="WR", status=check_status(names[healthy_idx]),team=team)
                    WRs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break
                
        # Get CB1 & CB2
        if position in ["LCB","RCB"]:
            names = list(row[1:5])
            CBs = []
            while len(CBs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy CB
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="CB", status=check_status(names[healthy_idx]),team=team)
                    CBs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break

        # Get SS
        if position in ["SS"]:
            names = list(row[1:5])
            
            SSs = []
            while len(SSs) < 2:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy SSs
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="SS", status=check_status(names[healthy_idx]),team=team)
                    SSs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break
                
        # Get LB
        if position in ["WLB", "LILB", "RILB", "SLB"]:
            names = list(row[1:5])
            
            LBs = []
            while len(LBs) < 4:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy Bss
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="LB", status=check_status(names[healthy_idx]),team=team)
                    LBs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break

        # Get DE
        if position in ["LDE", "RDE"]:
            names = list(row[1:5])
            
            DEs = []
            while len(DEs) < 4:
                healthy_names = [check_status(i) in healthy_statusses for i in names]
                # if we have healthy names left, we need to addthem
                if True in healthy_names:
                    healthy_idx = healthy_names.index(True)
                    # Append the first healthy Bss
                    roster_df = insert_position(df=roster_df, name=names[healthy_idx], position="DE", status=check_status(names[healthy_idx]),team=team)
                    DEs.append(names[healthy_idx])
                    names.remove(names[healthy_idx])
                    if len(names) == 0:
                        break
                # if we are out of names to add we need to break the while loop
                else:
                    break
                
    return roster_df

