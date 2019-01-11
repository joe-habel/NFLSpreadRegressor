"""
Here we're parsing through the individual sheets in each game spreadsheet into
the a more usable form for how we intend to make out training sets.

The data I'm currently scraping and schema can be found from a sheet in the 
Teams/ directory.

The main goal is to condense the information scraped from 20+ sheets per game
into a single row per game, based off of combined performance totals.
"""
import pandas as pd
from glob import iglob
import os
from multiprocessing import Pool, cpu_count, freeze_support
"""
A class that just initiates to a single dictionary representation isn't a good
use case for a class structure. The chose to represent this as an object only
since I would normally be making this dictionary a global variable to deal 
with the multiprocessing workflow.
"""
class Teams(object):
    def __init__(self):
        self.dict = {}
"""
Here we're going through each game file path and extracting the home team,
the away team, and the date. 
"""

def get_teams(path):
    teams = os.path.basename(path)
    vs_loc = teams.find('vs')
    file_loc = teams.find('.xlsx')
    hyph_loc = teams.find('-')
    away = teams[:vs_loc].strip()
    home = teams[vs_loc + 3:hyph_loc].strip()
    away = away.split(' ')[-1]
    home = home.split(' ')[-1]
    date = teams[hyph_loc+1:file_loc]
    return away, home, date

"""
Here we're getting the year and week that the game took place from the game 
file path.
"""
def get_year_week(path):
    week_path = os.path.abspath(os.path.join(path,os.pardir))
    year_path = os.path.abspath(os.path.join(week_path,os.pardir))
    week = os.path.basename(week_path).split(' ')[-1]
    year = os.path.basename(year_path)
    return year, week 

"""
The team dict allows us to understand how pro-football-reference abbreviates 
the team. I have a text file that cover every abbreviation the teams have had 
since 2003. I say every abbreviation to deal with the Rams and the Chargers.

pro-football-reference also uses some odd abbreviation for teams that would
normally have two letter abbrevations that would make sense.

i.e. SF is SFO and NE is NWE
"""
def get_team_dict():
    team_dict = {}
    with open('team_abb.txt', 'r') as abbs:
        teams = abbs.readlines()
    for line in teams:
        if line.find(',') < 0:
            continue
        team = line.split(',')
        team_dict[team[0].strip()] = team[1].strip().split(' ')
    return team_dict

"""
The following get_team funcitons get the sums of the desired_stats for each and deffensive sheet.
"""
def get_team_passing(offense_sheet,team,fumbles=False,desired_stats=['Player','Pass Att','Pass Cmp','Pass Int','Pass Sk','Pass Yds', 'Pass Sk Yds', 'Pass TD']):
    offense_sheet.fillna(0)
    passers = offense_sheet.loc[(offense_sheet['Tm'] == team) & (offense_sheet['Pass Att'] > 0)]
    try:
        return passers[desired_stats]
    except KeyError:
        for key in desired_stats:
            if 'Fumble' in key:
                desired_stats.remove(key)
        return passers[desired_stats]
    
def get_team_rushing(offense_sheet,team,fumbles=False,desired_stats=['Player','Rush Att', 'Rush TD', 'Rush Yds']):
    offense_sheet.fillna(0)
    rushers = offense_sheet.loc[(offense_sheet['Tm'] == team) & (offense_sheet['Rush Att'] > 0)]
    try:
        return rushers[desired_stats]
    except KeyError:
        for key in desired_stats:
            if 'Fumble' in key:
                desired_stats.remove(key)
        return rushers[desired_stats]
    
def get_team_receiving(offense_sheet,team,fumbles=False,desired_stats=['Player','Receive Rec', 'Receive Tgt', 'Receive Yds', 'Receive TD']):
    offense_sheet.fillna(0)
    receivers = offense_sheet.loc[(offense_sheet['Tm'] == team) & (offense_sheet['Receive Tgt'] > 0)]
    try:
        return receivers[desired_stats]
    except KeyError:
        for key in desired_stats:
            if 'Fumble' in key:
                desired_stats.remove(key)
        return receivers[desired_stats]

def get_team_kicking(kicking_sheet,team,desired_stats=['Player','Scoring FGA', 'Scoring FGM','Scoring XPA', 'Scoring XPM']):
    kicking_sheet.fillna(0)
    kickers = kicking_sheet.loc[(kicking_sheet['Tm'] == team) & ((kicking_sheet['Scoring FGA'] > 0) | (kicking_sheet['Scoring XPA'] > 0))]
    return kickers[desired_stats]
   
def get_team_punting(kicking_sheet,team,desired_stats=['Player','Scoring Pnt', 'Scoring Yds']):
    kicking_sheet.fillna(0)
    punters = kicking_sheet.loc[(kicking_sheet['Tm'] == team) & (kicking_sheet['Scoring Pnt'] > 0)]
    return punters[desired_stats]

def get_team_punt_returns(return_sheet,team,desired_stats=['Player','PR Ret', 'PR Yds', 'PR TD']):
    return_sheet.fillna(0)
    punt_return = return_sheet.loc[(return_sheet['Tm'] == team) & (return_sheet['PR Ret'] > 0)]
    return punt_return[desired_stats]

def get_team_kick_returns(return_sheet,team,desired_stats=['Player', 'KR Rt', 'KR Yds', 'KR TD']):
    return_sheet.fillna(0)
    kick_return = return_sheet.loc[(return_sheet['Tm'] == team) & (return_sheet['KR Rt'] > 0)]
    return kick_return[desired_stats]

def get_offensive_game_stats(sheets,team):
    stats = sheets['Team Stats - %s'%team]
    score = sheets['Scoring'][team].values[-1]
    fourth_downs = stats['Fourth Down Conv.']
    fourth_hyph = fourth_downs[0].find('-')
    fourth_down_cvt = fourth_downs[0][:fourth_hyph]
    fourth_down_atts = fourth_downs[0][fourth_hyph+1:]
    fumble_tots = stats['Fumbles-Lost']
    fumble_hyph = fumble_tots[0].find('-')
    fumbles = fumble_tots[0][:fumble_hyph]
    fumbles_lost = fumble_tots[0][fumble_hyph+1:]
    third_downs = stats['Third Down Conv.']
    third_hyph = third_downs[0].find('-')
    third_down_cvt = third_downs[0][:third_hyph]
    third_down_atts = third_downs[0][third_hyph+1:]
    offensive_stats = {}
    offensive_stats['First Downs'] = stats['First Downs'][0]
    offensive_stats['Third Down Att'] = third_down_atts
    offensive_stats['Third Down Cvt'] = third_down_cvt
    offensive_stats['Fourth Down Att'] = fourth_down_atts
    offensive_stats['Fourth Down Cvt'] = fourth_down_cvt 
    offensive_stats['Fumbles'] = fumbles
    offensive_stats['Fumbles Lost'] = fumbles_lost
    offensive_stats['Total Points'] = score
    return offensive_stats


def get_team_defensive_totals(defense_sheet,team,desired_stats=['DefInt Int', 'DefInt TD', 'Fumble TD', 'Sck&Ttl Sk']):
    defense_sheet.fillna(0)
    defense_sheet = defense_sheet.loc[defense_sheet['Tm'] == team]
    defense_tots = defense_sheet[desired_stats].sum(axis=0)
    defense_df = {}
    for key,val in defense_tots.items():
        defense_df[key] = val
    return defense_df
"""
These defensive compliments are what I would best describe as the 'Allowed' stats.
These are determined more by the performance of the opposing offense than
directly measured stats from the defense.
"""
def get_defensive_compliment_stats(offense,defense):
    passing,rushing,rec,punt_return,kick_return,game_stats, kicking, punting = offense
    total_pass_attempts = passing['Pass Att'].sum()
    total_pass_yards = passing['Pass Yds'].sum() - passing['Pass Sk Yds'].sum()
    passing_tds_allowed = passing['Pass TD'].sum()
    total_rush_attempts = rushing['Rush Att'].sum()
    total_rush_yards = rushing['Rush Yds'].sum()
    rushing_tds_allowed = rushing['Rush TD'].sum()
    punt_returns_defended = punt_return['PR Ret'].sum()
    punt_return_yards_allowed = punt_return['PR Yds'].sum()
    punt_return_tds = punt_return['PR TD'].sum()
    kick_returns_defended = kick_return['KR Rt'].sum()
    kick_return_yards = kick_return['KR Yds'].sum()
    kick_return_tds = kick_return['KR TD'].sum()
    punts_defended = punting['Scoring Pnt'].sum()
    defense['Punts Defended'] = punts_defended
    defense['Pass Attempts Defended'] = total_pass_attempts
    defense['Pass Yards Allowed'] = total_pass_yards
    defense['Pass TDs Allowed'] = passing_tds_allowed
    defense['Rush Attempts Defended'] = total_rush_attempts
    defense['Rush Yards Allowed'] = total_rush_yards
    defense['Rush TDs Allowed'] = rushing_tds_allowed
    defense['Punt Returns Defended'] = punt_returns_defended
    defense['Punt Return Yards Allowed'] = punt_return_yards_allowed
    defense['Punt Return TDs Allowed'] = punt_return_tds
    defense['Kick Returns Defended'] = kick_returns_defended
    defense['Kick Return Yards Allowed'] = kick_return_yards
    defense['Kick Return TDs Allowed'] = kick_return_tds
    defense['Fumble FF'] = game_stats['Fumbles']
    defense['Fumble FR'] = game_stats['Fumbles Lost']
    defense['First Downs Allowed'] = game_stats['First Downs']
    defense['Third Downs Defended'] = game_stats['Third Down Att']
    defense['Third Downs Stopped'] = int(game_stats['Third Down Att']) - int(game_stats['Third Down Cvt'])
    defense['Fourth Downs Defended'] = game_stats['Fourth Down Att']
    defense['Fourth Downs Stopped'] = int(game_stats['Fourth Down Att']) - int(game_stats['Fourth Down Cvt'])
    defense['Field Goals Allowed'] = kicking['Scoring FGM'].sum()
    defense['Field Goals Defended'] = kicking['Scoring FGA'].sum()
    defense['Points Allowed'] = game_stats['Total Points']
    return defense
"""
This wraps all of the get functions into a single process.
"""
def get_stat_sheets(sheets,team):
    sheet_dict = {}
    sheet_dict['Passing'] = get_team_passing(sheets['Offense'],team)
    sheet_dict['Rushing'] = get_team_rushing(sheets['Offense'],team)
    sheet_dict['Rec'] = get_team_receiving(sheets['Offense'],team)
    sheet_dict['Kicking'] = get_team_kicking(sheets['Kicking'],team)
    sheet_dict['Punting'] = get_team_punting(sheets['Kicking'],team)
    sheet_dict['Kick Return'] = get_team_kick_returns(sheets['Kick Return'],team)
    sheet_dict['Punt Return'] = get_team_punt_returns(sheets['Kick Return'],team)
    sheet_dict['Game Stats'] = get_offensive_game_stats(sheets,team)
    sheet_dict['Defense'] = get_team_defensive_totals(sheets['Defense'],team)
    return sheet_dict
"""
The converts all of the sheets to dicts, since appending to pandas is a
performance nightmare, but converting a list of dicts to a new dataframe
isn't an issue. 
"""
def to_dictionary_fix(sheets):
    sheets['Passing'] = sheets['Passing'].to_dict('records')
    sheets['Rushing'] = sheets['Rushing'].to_dict('records')
    sheets['Rec'] = sheets['Rec'].to_dict('records')
    sheets['Kicking'] = sheets['Kicking'].to_dict('records')
    sheets['Kick Return'] = sheets['Kick Return'].to_dict('records')
    sheets['Punting'] = sheets['Punting'].to_dict('records')
    sheets['Punt Return'] = sheets['Punt Return'].to_dict('records')
    sheets['Game Stats'] = [sheets['Game Stats']]
    sheets['Defense'] = [sheets['Defense']]
    return sheets

"""
This wraps the getting defensive complements for both teams, based on the 
oppents offensive stats.
"""
def update_defensive_sheets(away_sheets,home_sheets):
    away_offense = (away_sheets['Passing'], away_sheets['Rushing'], away_sheets['Rec'],
                    away_sheets['Punt Return'], away_sheets['Kick Return'], away_sheets['Game Stats'], away_sheets['Kicking'], away_sheets['Punting'])
    home_sheets['Defense'] = get_defensive_compliment_stats(away_offense,home_sheets['Defense'])
    home_offense = (home_sheets['Passing'], home_sheets['Rushing'], home_sheets['Rec'],
                    home_sheets['Punt Return'], home_sheets['Kick Return'], home_sheets['Game Stats'], home_sheets['Kicking'], home_sheets['Punting'])
    away_sheets['Defense'] = get_defensive_compliment_stats(home_offense,away_sheets['Defense'])
    return away_sheets, home_sheets
"""
We add in the week, year, and date to all of the sheets so we can easily
index and identify each row.
"""
def add_index(sheets,year,week,date):
    for sheetname, sheet in sheets.items():
        sheet['Week'] = week
        sheet['Year'] = year
        sheet['Date'] = date

"""
This is the callback wrapper for the multiprocessing return. It uses the Teams
global object to the dictionary into a list of dictionary sheets.

TODO: Update this to use a defualtdict(list) from collections
"""
def update_sheets(callback_tuple):
    away,away_sheets,home,home_sheets = callback_tuple
    if away not in teams.dict:
        teams.dict[away] = {}
        for sn, sheet in away_sheets.items():
            teams.dict[away][sn] = sheet
    else:
        for sn , sheet in away_sheets.items():
            teams.dict[away][sn].extend(sheet)
    if home not in teams.dict:
        teams.dict[home] = {}
        for sn, sheet in home_sheets.items():
            teams.dict[home][sn] = sheet
    else:
        for sn, sheet in home_sheets.items():
            teams.dict[home][sn].extend(sheet)

"""
Here we're writing the sheets at the end in their entirety.
"""
def write_sheets(teams_obj):
    for team, sheets, in teams_obj.dict.items():
        writer = pd.ExcelWriter('Teams/%s.xlsx'%team, engine='xlsxwriter')
        for sn, sheet in sheets.items():
            sheet = pd.DataFrame(sheet)
            sheet.to_excel(writer,sn,index=False)
        writer.close()

"""
This is the multiprocessing process, that parses each game sheet into a single
row for each team.
"""
def proc(game,team_dict):
    away, home, date = get_teams(game)
    year, week = get_year_week(game)
    print ('...%s Week %s, %s vs %s'%(year,week,away,home))
    sheets = pd.read_excel(game,sheet_name = None)
    scoring = sheets['Scoring']
    col_titles = list(scoring)
    for abb in team_dict[away]:
        if abb in col_titles:
            away_abb = abb
    for abb in team_dict[home]:
        if abb in col_titles:
            home_abb = abb
    home_sheets = get_stat_sheets(sheets,home_abb)
    away_sheets = get_stat_sheets(sheets,away_abb)
    add_index(home_sheets,year,week,date)
    add_index(away_sheets,year,week,date)
    away_sheets,home_sheets = update_defensive_sheets(away_sheets,home_sheets)
    away_sheets, home_sheets = to_dictionary_fix(away_sheets), to_dictionary_fix(home_sheets)
    return (away,away_sheets,home,home_sheets)

"""
This is the multiprocessing wrapper. The return will callback to the global
callback wrapper that updates each individual sheet, and then the teams will
all write sequentially.
"""
def make_team_game_sheets(game_dir='Games',gofast=True):
    global teams
    teams = Teams()
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ('Generating Training Data Using %s cores:'%cores)
    pool = Pool(cores)
    team_dict = get_team_dict()
    for game in iglob(game_dir + '/**/*.xlsx', recursive = True):
        pool.apply_async(proc,args = (game,team_dict),callback=update_sheets)
    pool.close()
    pool.join()
    write_sheets(teams)

if __name__ == '__main__':
    freeze_support()
    make_team_game_sheets()
    
