"""
Game Scraper scrapes every NFL game from 2003 onward. 
2003 since that's when  we moved to a consistent 21 week season.

I'm writing everything into sheets in XLSX files intsead of tables in a 
relational DB, just because I've worked on this from a single machine.
Using sheets in Excel files though is not going to scale nicely. Ideally 
if this process had to scale using a Database structure should take care of
that.

This file is rather long with individual table parsers that could probably
all be replaced by a slight change to the parse_game_logs function found in 
name_scraper.py.

There is also a possiblility to go back and reverse engineer the API
so I don't have to use selenium for this process, so leaving it as is
gives me a nice template to work with that.

TODO: Refactor most parse functions with a generalized parse_game_logs 
equivalent, or reverse engineer the API.
"""
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import os
from time import sleep, time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import TimeoutException
from multiprocessing import Pool, cpu_count, freeze_support

"""
We're going to initialze a game and table object, with each table being in a list
belonging to each individual game. Since there are 20+ tables per game, this makes
keeping track of the tables easier.
"""
class Game(object):
    def __init__(self,date,away,home,year,week):
        self.date = date
        self.away = away
        self.home = home
        self.year = year
        self.week = week
        self.tables = []


class Table(object):
    def __init__(self,main_title,subtitle=None):
        self.main_title = main_title
        self.subtitle = subtitle
        self.rows = []
        
    def create_df(self):
        self.df = pd.DataFrame(self.rows)
        self.rows = []
        
"""
We have to use selenium to let the page pull the JS requests to populate the 
tables unfortunately. To keep the memory performance down, I'd normally 
recomend sending the requests using a headless browser instance. What sucks in
this case is pro-football-reference sends a TON of ad requests. This makes
running a full browser instance with AdBlockPlus installed actually outperform
running it headless. The machine I wrote this on has 24gb of RAM, so I'm okay
with being able to spawn up tons of individual chrome browsers, but I wouldn't
reccomend this if you're not feeling comfortable with the amount of RAM on
your machine.
"""
def init_driver(headless=False,load='normal'):
    if headless == True:
        opts = Options()
        opts.add_argument('--headless')
        opts.add_argument("--proxy-server='direct://'")
        opts.add_argument("--proxy-bypass-list=*")
        caps = DesiredCapabilities().CHROME
        caps['pageLoadStrategy'] = load
        driver = webdriver.Chrome(desired_capabilites=caps,options=opts)
    else:
        opts = Options()
        opts.add_extension('Adblock-Plus_v3.3.2.crx')
        caps = DesiredCapabilities().CHROME
        caps['pageLoadStrategy'] = load
        driver = webdriver.Chrome(options=opts)
    
    return driver

"""
We're going through here and getting all of the links to the individual games
for each week using requests since these are direct HTML elements of the site. 
We're only going to have to use selenium for the actual game objects thank god.
"""
def link_proc(year,week,sleep_time,base_url):
    print ('...', year,week)
    game_links = []
    sleep(sleep_time)
    url = base_url + '/years/' +  str(year) + '/week_%s.htm'%week
    page = requests.get(url)
    soup = BeautifulSoup(page.text,'html.parser')
    for a in soup.find_all('a'):
        if a.text.strip() == 'Final':
            game_links.append((year,week,str(a['href'])))
    return game_links
    
"""
This just wraps the above process into a pool of workers. You can specify what
years or weeks you want to scrape for as well.
"""
def get_game_links(start_year,end_year,gofast=True,sleep_time=0.15,start_week=1, end_week = 22,base_url='https://www.pro-football-reference.com'):
    end_year = max(end_year,2003)
    years = range(start_year,end_year)
    game_links = []
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ('Getting Game Links Using %s cores:'%cores)
    pool = Pool(cores)
    for year in years:
        for week in range(start_week,end_week):
            pool.apply_async(link_proc, args=(year,week,sleep_time,base_url),callback=game_links.extend)
    pool.close()
    pool.join()
    return game_links

"""
This is going to pull the teams and the date out of the header of the game
"""
def parse_title(title):
    start_loc = 0
    at_loc = title.find(' at ')
    date_loc = title.find('-')
    if title.find('-', date_loc + 1) > 0:
        start_loc = date_loc + 1
        date_loc = title.find('-', date_loc + 1)
    if at_loc < 0:
        at_loc = title.find('vs')
        at_loc_start = at_loc + 4
    else:
        at_loc_start = at_loc + 3
    away = title[start_loc:at_loc].strip()
    home = title[at_loc_start:date_loc].strip()
    date = title[date_loc + 1:].strip()
    return date,away,home

"""
This section is just going to be individual parsers for each of the tables.
This is what is going to need a lot of refactoring, as a lot of the funcitons
feel a lot like other table parsers. The only nice thing about keeping these
as individual tables as it makes following the scraping process a lot easier.

TODO: Refactor a lot of these with the name_scraper.py general method.
"""

def parse_scoring_table(scoring_table,game):
    header = scoring_table.find('thead')
    index_dict = {}
    for i,th in enumerate(header.find_all('th')):
        index_dict[i] = th.text.strip()
    game.tables.append(Table('Scoring'))
    body = scoring_table.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        quarter = tr.find('th').text
        if not bool(quarter):
            quarter = game.tables[-1].rows[-1]['Quarter']
        row['Quarter'] = quarter.strip()
        for i,td in enumerate(tr.find_all('td')):
            row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

    
def parse_game_info(game_info,game):
    body = game_info.find('tbody')
    game_info = {}
    for tr in body.find_all('tr'):
        key = tr.find('th').text.strip()
        game_info[key] = tr.find('td').text.strip()
    game.tables.append(Table('Game Info'))
    game.tables[-1].rows.append(game_info)
    game.tables[-1].create_df()
    
def parse_officials(officials,game):
    body = officials.find('tbody')
    game_info = {}
    for tr in body.find_all('tr'):
        key = tr.find('th').text.strip()
        game_info[key] = tr.find('td').text.strip()
    game.tables.append(Table('Officials'))
    game.tables[-1].rows.append(game_info)
    game.tables[-1].create_df()
    
def parse_expected_points(expected_points,game):
    index_dict = {}
    game.tables.append(Table('Expected Points'))
    head = expected_points.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i,th in enumerate(tr.find_all()):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 6:
                index_dict[i] = 'O' + th.text.strip()
            elif 6 <= i < 10:
                index_dict[i] = 'D' + th.text.strip()
            else:
                index_dict[i] = 'ST' + th.text.strip()
    body = expected_points.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        th = tr.find('th')
        row[index_dict[0]] = th.text.strip()
        for i,td in enumerate(tr.find_all('td')):
            row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_team_stats(team_stats,game):
    team_one_dict = {}
    team_two_dict = {}
    head = team_stats.find('thead')
    for i,th in enumerate(head.find_all('th')):
        if i == 1:
            team_one = th.text.strip()
        if i == 2:
            team_two = th.text.strip()
    body = team_stats.find('tbody')
    for tr in body.find_all('tr'):
        key = tr.find('th').text.strip()
        for i,td in enumerate(tr.find_all('td')):
            if i == 0:
                team_one_dict[key] = td.text.strip()
            else:
                team_two_dict[key] = td.text.strip()
    game.tables.append(Table('Team Stats', team_one))
    game.tables[-1].rows.append(team_one_dict)
    game.tables[-1].create_df()
    game.tables.append(Table('Team Stats', team_two))
    game.tables[-1].rows.append(team_two_dict)
    game.tables[-1].create_df()
    
def parse_PRR(pRR_table,game):
    index_dict = {}
    head = pRR_table.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i,th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 11:
                if i == 8:
                    index_dict[i] = 'Pass Sk ' + th.text.strip()
                else:
                    index_dict[i] = 'Pass ' + th.text.strip()
            elif 11 <= i < 15:
                index_dict[i] = 'Rush ' + th.text.strip()
            elif 15 <= i < 20:
                index_dict[i] = 'Receive ' + th.text.strip()
            else:
                index_dict[i] = 'Fumble ' + th.text.strip()
    game.tables.append(Table('Offense'))
    body = pRR_table.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_defense(defense,game):
    index_dict = {}
    head = defense.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i,th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 6:
                index_dict[i] = 'DefInt ' + th.text.strip()
            elif 6 <= i < 9:
                index_dict[i] = 'Sck&Ttl ' + th.text.strip()
            else:
                index_dict[i] = 'Fumble ' + th.text.strip()
    game.tables.append(Table('Defense'))
    body = defense.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_kick_return(kick_return,game):
    index_dict = {}
    head = kick_return.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i,th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 7:
                index_dict[i] = 'KR ' + th.text.strip()
            else:
                index_dict[i] = 'PR ' + th.text.strip()
    game.tables.append(Table('Kick Return'))
    body = kick_return.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_kick(kick,game):
    index_dict = {}
    head = kick.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i,th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 8:
                index_dict[i] = 'Scoring ' + th.text.strip()
            else:
                index_dict[i] = 'Punting ' + th.text.strip()
    game.tables.append(Table('Kicking'))
    body = kick.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()


def parse_starters(starters,team,game):
    index_dict = {}
    head = starters.find('thead')
    for tr in head.find_all('tr'):
        for i,th in enumerate(tr.find_all('th')):
            index_dict[i] = th.text.strip()
    body = starters.find('tbody')
    game.tables.append(Table('Starters',team))
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        row[index_dict[1]] = tr.find('td').text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()


def parse_snap_counts(snap_counts,team,game):
    index_dict = {}
    head = snap_counts.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i, th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 4:
                index_dict[i] = 'O ' + th.text.strip()
            elif 4 <= i < 6:
                index_dict[i] = 'D ' + th.text.strip()
            else:
                index_dict[i] = 'ST ' + th.text.strip()
    game.tables.append(Table('Snap Count', team))
    body = snap_counts.find('tbody')
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()


def parse_pass_targets(pass_targets,game):
    index_dict = {}
    head = pass_targets.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i, th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 6:
                index_dict[i] = 'ShortL ' + th.text.strip()
            elif 6 <= i < 10:
                index_dict[i] =  'ShortM ' + th.text.strip()
            elif 10 <= i < 14:
                index_dict[i] =  'ShortR ' + th.text.strip()
            elif 14 <= i < 18:
                index_dict[i] =  'DeepL ' + th.text.strip()
            elif 18 <= i < 22:
                index_dict[i] =  'DeepM ' + th.text.strip()
            elif 22 <= i < 26:
                index_dict[i] =  'DeepR ' + th.text.strip()
            else:
                index_dict[i] = 'NoDir ' + th.text.strip()
    body = pass_targets.find('tbody')
    game.tables.append(Table('Pass Targets'))
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()


def parse_rush_directions(rush_directions,game):
    index_dict = {}
    head = rush_directions.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i, th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 5:
                index_dict[i] =  'L End ' + th.text.strip()
            elif 5 <= i < 8:
                index_dict[i] =  'L Tckl ' + th.text.strip()
            elif 8 <= i < 11:
                index_dict[i] =  'L Guard ' + th.text.strip()
            elif 11 <= i < 14:
                index_dict[i] =  'Mid ' + th.text.strip()
            elif 14 <= i < 17:
                index_dict[i] =  'R Guard ' + th.text.strip()
            elif 17 <= i < 20:
                index_dict[i] =  'R Tckl ' + th.text.strip()
            elif 20 <= i < 23:
                index_dict[i] =  'R End ' + th.text.strip()
            else:
                index_dict[i] = 'NoDir ' + th.text.strip()
    body = rush_directions.find('tbody')
    game.tables.append(Table('Rush Directions'))
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_pass_tackles(pass_tackles,game):
    index_dict = {}
    head = pass_tackles.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            continue
        for i, th in enumerate(tr.find_all('th')):
            if i < 2:
                index_dict[i] = th.text.strip()
            elif 2 <= i < 4:
                index_dict[i] = 'ShortR ' + th.text.strip()
            elif 4 <= i < 6:
                index_dict[i] =  'ShortM ' + th.text.strip()
            elif 6 <= i < 8:
                index_dict[i] =  'ShortL ' + th.text.strip()
            elif 8 <= i < 10:
                index_dict[i] =  'DeepR ' + th.text.strip()
            elif 10 <= i < 12:
                index_dict[i] =  'DeepM ' + th.text.strip()
            elif 12<= i < 14:
                index_dict[i] =  'DeepL ' + th.text.strip()
            else:
                index_dict[i] = 'NoDir ' + th.text.strip()
    body = pass_tackles.find('tbody')
    game.tables.append(Table('Pass Tackles'))
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_rush_tackles(rush_tackles,game):
    index_dict = {}
    head = rush_tackles.find('thead')
    for tr in head.find_all('tr'):
        for i, th in enumerate(tr.find_all('th')):
            index_dict[i] = th.text.strip()
    body = rush_tackles.find('tbody')
    game.tables.append(Table('Rush Tackles'))
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        row[index_dict[0]] = tr.find('th').text.strip()
        for i, td in enumerate(tr.find_all('td')):
            row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_drives(drives,team,game):
    index_dict = {}
    head = drives.find('thead')
    for tr in head.find_all('tr'):
        for i,th in enumerate(tr.find_all('th')):
            if i == 0:
                continue
            index_dict[i-1] = th.text.strip()
    body = drives.find('tbody')
    game.tables.append(Table('Drives', team))
    for tr in body.find_all('tr'):
        row = {}
        for i, td in enumerate(tr.find_all('td')):
            row[index_dict[i]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()

def parse_play_by_play(play_by_play,game):
    index_dict = {}
    head = play_by_play.find('thead')
    for tr in head.find_all('tr'):
        for i, th in enumerate(tr.find_all('th')):
            index_dict[i] = th.text.strip()
    game.tables.append(Table('Play by Play'))
    body = play_by_play.find('tbody')
    for tr in body.find_all('tr'):
        row = {}
        if tr.has_attr('colspan'):
            continue
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row[index_dict[0]] = tr.find('th').text.strip()
        for i,td in enumerate(tr.find_all('td')):
            if not bool(td.text.strip()):
                row[index_dict[i+1]] = None
            else:
                row[index_dict[i+1]] = td.text.strip()
        game.tables[-1].rows.append(row)
    game.tables[-1].create_df()
            
"""
This is the process for scraping an inididual game after it's loaded in from
selenium. Now I probably could have went in and reversed engineered the 
API from pro-football-reference to get where each of the tables is being called
from, and how the data comes back. One of the reasons that I'm a little hesistant
refactor is that exact reason. This provides me a very nice outline of the 
tables that I am scraping for each page.

TODO: Either refactor this into a more general process, or look into reversing 
the API PFR uses to populate these tables.
"""
def proc(chunk,base_url):
    driver = init_driver()
    timeout_links = []
    for val in chunk:
        year,week,link = val
        url = base_url + link
        start = time()
        try:
            driver.get(url)
        except TimeoutException:
            print ('Timed out', url)
            timeout_links.append(link)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        title = soup.find('h1').text
        date,away,home = parse_title(title)
        game = Game(date,away,home,year,week)
        print (date,away,home,time()- start)
        scoring_table = soup.find('table', attrs={'id':'scoring'})
        if bool(scoring_table):
            parse_scoring_table(scoring_table,game)
        game_info = soup.find('table', attrs={'id':'game_info'})
        if bool(game_info):
            parse_game_info(game_info,game)
        official_table = soup.find('table', attrs={'id' : 'officials'})
        if bool(official_table):
            parse_officials(official_table,game)
        expected_points = soup.find('table', attrs={'id' : 'expected_points'})
        if bool(expected_points):
            parse_expected_points(expected_points,game)
        team_stats = soup.find('table', attrs={'id' : 'team_stats'})
        if bool(team_stats):
            parse_team_stats(team_stats,game)
        pRR = soup.find('table', attrs={'id': 'player_offense'})
        if bool(pRR):
            parse_PRR(pRR,game)
        defense = soup.find('table', attrs={'id' : 'player_defense'})
        if bool(defense):
            parse_defense(defense,game)
        kick_return = soup.find('table', attrs={'id' : 'returns'})
        if bool(kick_return):
            parse_kick_return(kick_return,game)
        kicking = soup.find('table', attrs={'id' : 'kicking'})
        if bool(kicking):
            parse_kick(kicking,game)
        home_starter = soup.find('table' , attrs={'id' : 'home_starters'})
        if bool(home_starter):
            parse_starters(home_starter,'Home', game)
        vis_starter = soup.find('table' , attrs={'id' : 'vis_starters'})
        if bool(vis_starter):
            parse_starters(vis_starter,'Away', game)
        home_snap_counts = soup.find('table', attrs={'id' : 'home_snap_counts'})
        if bool(home_snap_counts):
            parse_snap_counts(home_snap_counts,'Home',game)
        vis_snap_counts = soup.find('table', attrs={'id' : 'vis_snap_counts'})
        if bool(vis_snap_counts):
            parse_snap_counts(vis_snap_counts,'Away',game)
        pass_targets = soup.find('table', attrs={'id' : 'targets_directions'})
        if bool(pass_targets):
            parse_pass_targets(pass_targets,game)
        rush_directions = soup.find('table' , attrs={'id' : 'rush_directions'})
        if bool(rush_directions):
            parse_rush_directions(rush_directions,game)
        pass_tackles = soup.find('table', attrs={'id' : 'pass_tackles'})
        if bool(pass_tackles):
            parse_pass_tackles(pass_tackles,game)
        rush_tackles = soup.find('table' , attrs={'id' : 'rush_tackles'})
        if bool(rush_tackles):
            parse_rush_tackles(rush_tackles,game)
        home_drives = soup.find('table', attrs={'id' : 'home_drives'})
        if bool(home_drives):
            parse_drives(home_drives,'Home',game)
        vis_drives = soup.find('table', attrs={'id' : 'vis_drives'})
        if bool(vis_drives):
            parse_drives(vis_drives, 'Away', game)
        pbp = soup.find('table', attrs={'id' : 'pbp'})
        if bool(pbp):
            parse_play_by_play(pbp,game)
        write_game(game)
    driver.close()
    return timeout_links
"""
This wraps the above process into a pool of workers. If a link timesout when 
being scraped with selenium (which will 100% happen). It will take note of that
and go ahead and run back through the process until there are no more games
that timed out.

NOTE: that a full CPU load for this I'm gonna cap at the floor of 90% of the 
amount of CPU cores present, because while we can limit the python workers to
the appropriate amount of usage, we can't do that with chrome.
"""
def scrape_games(game_links,gofast=True,timeout=False,base_url = 'https://www.pro-football-reference.com'):
    timeout_links = []
    if timeout is True:
        print ('Scraping Timeout Links')
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = int(cpu_count()*.9)
    print ('Scraping Game Data Using %s cores:'%cores)
    pool = Pool(cores)
    game_links = np.array_split(game_links,cores)
    for chunk in game_links:
        pool.apply_async(proc,args=(chunk,base_url),callback=timeout_links.extend)
    pool.close()
    pool.join()
    if bool(timeout_links):
        scrape_games(timeout_links,timeout=True)
"""
This will write all of the sheets associated with a game into it's own 
spreadsheet.
"""
def write_game(game):
    year = game.year
    if not os.path.exists('Games/%s'%year):
        os.mkdir('Games/%s'%year)
    week = game.week
    if not os.path.exists('Games/%s/Week %s'%(year,week)):
        os.mkdir('Games/%s/Week %s'%(year,week))
    writer = pd.ExcelWriter('Games/%s/Week %s/%s.xlsx'%(year,week,game.away + ' vs ' + game.home + ' - ' + game.date), engine='xlsxwriter')
    for table in game.tables:
        if table.subtitle is not None:
            sheet = table.main_title + ' - ' + table.subtitle
        else:
            sheet = table.main_title
        table.df.to_excel(writer,sheet_name=sheet,index=False)
    writer.close()
"""
These three functions that rename are just from when I wasn't including the
date in the title. I probably don't need them here, they don't get called and 
I've fixed the naming schema, but if I come across games I scraped previously
usign the old schema, I'd rather just have these here.
"""
def rename_game(game):
    year = game.year
    week = game.week
    if os.path.exists('Games/%s/Week %s/%s.xlsx'%(year,week,game.away + ' vs ' + game.home)):
        os.rename('Games/%s/Week %s/%s.xlsx'%(year,week,game.away + ' vs ' + game.home),'Games/%s/Week %s/%s.xlsx'%(year,week,game.away + ' vs ' + game.home + ' - ' + game.date))

def rename_proc(val,base_url):
    year,week,link = val
    url = base_url + link
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    title = soup.find('h1').text
    date,away,home = parse_title(title)
    print (date,away,home)
    game = Game(date,away,home,year,week)
    rename_game(game)
    sleep(0.1)
    

def rename_games(game_links,gofast=True,base_url='https://www.pro-football-reference.com'):
    if gofast==False:
        cores = int(cpu_count*0.8)
    else:
        cores = cpu_count()
    pool = Pool(cores)
    for val in game_links:
        pool.apply_async(rename_proc,args=(val,base_url))
    pool.close()
    pool.join()


if __name__ == '__main__':
    freeze_support()
    links = get_game_links(2010,2011)
    games = scrape_games(links,True)
