"""
The goal of name scraper is to get the history of individual players performance.
Running this the first time around is completely optional. When making a training 
set there is a function that can be called for each individual player that comes
up. But that is going to make making the first training set take quite a long 
time. So running this once will expediate making the first training set by a
lot.

NOTE: The gofast flags, if set to true will push the process through across
all avaible cores.
"""
import pandas as pd
import requests
import re
from glob import iglob
from time import sleep
from bs4 import BeautifulSoup
from collections import defaultdict
from multiprocessing import Pool, cpu_count, freeze_support

"""
We're initializing these classes just to make sense of the inheritance 
that comes from the data as we ingest it from each player object. We could
probably just as well do it from a function with a dictionary to track what's
going on here with the player and two different kinds of gamelogs. However,
I feel once scraping gets into full effect this feels a little more straight-
forward.
"""
class Player(object):
    def __init__(self,name,position):
        self.name = name
        self.position = position
        self.gamelogs = []

class Gamelog(object):
    def __init__(self,log_type):
        self.log_type = log_type
        self.rows = []
    
    def create_df(self):
        self.df = pd.DataFrame(self.rows)
        self.rows = []


"""
This is the individual process that we'll pass into a pool of multiprocessing
workers to parse through all the game files to find every player who played 
in this game.
"""
def first_proc(game_file):
    print ('...', game_file)
    players = []
    sheets = pd.read_excel(game_file,sheet_name=None)
    for sheet_name,df in sheets.items():
        if 'Player' in list(df):
            for name in df['Player'].values:
                players.append(name)
    return players
    
"""
Here is the overall process for getting all of the players who ever played in
the games that we have on record. We set the list at the end of this to make
sure that players aren't repeated. I feel like we could potentially swap the
initial player list to a set and use a players.update in the callback, but I 
don't know if calling the update thousands of times would be more costly than
just taking up some memory in a list of maybe 8000 strings.
"""
def get_player_names(game_dir='Games',gofast=True):
    players = []
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ('Getting Player Names from Sheets %s cores:'%cores)
    pool = Pool(cores)
    for game_file in iglob(game_dir + '/**/*.xlsx', recursive=True):
        pool.apply_async(first_proc, args=(game_file,), callback = players.extend)
    pool.close()
    pool.join()
    return list(set(players))


"""
The is the multiprocessing worker process that will go through each letter of
profootball-refernce's player name directory. You can call into their letter 
directories and get a list of players whose last names (some minor exceptions)
begin with that letter. This process will go into that letter and get the link
for the individual player. 

Arguably refactoring this to be able to take in 
all of the players' names who have that last letter would be more beneficial
for the first pass through, by a ton.

TODO: Refactor this to be able to pull every name in that letter.
"""
def link_proc(name,letter,sleep_time,url):
    print ('...', name)
    link_dict = {}
    pattern = re.compile(r'%s'%name)
    letter_url = url + letter + '/'
    r = requests.get(letter_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    names = soup.find_all('a', text=pattern)
    if len(names) > 1:
        for i,tag in enumerate(names):
            link_dict[name+str(i)] = tag['href']
    elif len(names) == 1:
        link_dict[name] = names[0]['href']
    sleep(sleep_time)
    return link_dict
    
"""
This wraps the above process into a pool of multiprocessing workers. It updates
a link dictionary that will contain the link associated with each of the players'
game stats.

TODO: Update what's passed to link_proc to be a grouping by the first letter
of last names instead of individual names in a loop. We're losing a ton of time
by making requests for each name instead of only making 26 reqeusts in total.
"""      
def get_player_links(player_names,gofast=True,sleep_time=0.15,url='https://www.pro-football-reference.com/players/'):
    name_dict = defaultdict(dict)
    links = {}
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ('Getting Links for the Players Using %s cores:'%cores)
    pool = Pool(cores)
    for name in player_names:
        if type(name) != str:
            print (name)
            continue
        try:
            name_dict[name.split(' ')[-1][0].upper()].append(name)
        except AttributeError:
            name_dict[name.split(' ')[-1][0].upper()] = [name]
    for letter, names in name_dict.items():
        for name in names:
            pool.apply_async(link_proc, args=(name,letter,sleep_time,url),callback = links.update)
    pool.close()
    pool.join()
    return links

"""
This process will find a single player link. This will be used in other modules
in case a name is come across that we don't have saved locally. This will take
into account that the letter directory provided by pro-football-reference might
not be the first letter of the last name. Ect. see Antwaan Randle El, is kept
in the /R directory and not /E. I'd argue it's quicker to come back to these 
edges cases instead of scraping through every name for every player.
"""
def get_single_link(player,sleep_time=0.15,url='https://www.pro-football-reference.com/players/'):
    print ('......Finding Links for %s'%player)
    letters = []
    link_dict = {}
    name_count = 0
    pattern = re.compile(r'%s'%player)
    for name in player.split(' '):
        letters.append(name[0])
    for letter in letters:
        letter_url = url + letter + '/'
        r = requests.get(letter_url)
        soup = BeautifulSoup(r.text, 'html.parser')
        names = soup.find_all('a', text=pattern)
        if len(names) > 1:
            for i, tag in enumerate(names):
                link_dict[player+str(name_count)] = tag['href']
                name_count += 1
        elif len(names) == 1:
            if name_count == 0:
                link_dict[player] = names[0]['href']
                name_count += 1
            else:
                link_dict[player+str(name_count)] = names[0]['href']
                name_count += 1
    return link_dict
 

"""
NOTE: Pro-football-reference's robots.txt does not want crawlers crawling
/gamelogs/, probably because you can pull the whole table in a request without
it having to use JS to load in the table dynamically. So use this at your own
risk, maybe give it a longer time to sleep between requests, or don't go
pulling this data across too many workers.

This will take a player link, and go into the individual player's gamelog to get
their entire history of how they performed. The regular season and playoff
logs are seperate tables on the page, so we're gonna scrape those two tables
seperately into two seperate sheets. This will return the players position as well
and if they are a QB if they are a righty or lefty.
"""       
def player_proc(name,link,sleep_time=0.15,url='https://www.pro-football-reference.com'):
    print ('.......Getting Data and Writing Sheet for %s'%name)
    loc = link.find('.htm')
    full_url = url + link[:loc] + '/gamelog/'
    try:
        r = requests.get(full_url)
    except:
        return link
    soup = BeautifulSoup(r.text,'html.parser')
    info = soup.find('div' ,attrs={'id' : 'info'})
    for i,p in enumerate(info.find_all('p')):
        if i == 1:
            position = p.text
            break
    loc = position.find(':')
    loc2 = position.find('Throws')
    if loc2 >= 0:
        position = position[loc+1:loc2].strip()
    else:
        position = position[loc+1:].strip()
    player = (Player(name,position))
    for table in soup.find_all('table'):
        parse_game_logs(table,player)
    
"""
This wraps the gamelog scraping and sheet writing process into a pool of workers.
If there is a timeout error for some reason, at the end of the process we'll
just go back and get the list of players that timedout.
"""
def parse_links(links,gofast=True,sleep_time=0.15,url='https://www.pro-football-reference.com'):
    timeouts = []
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ('Getting Career Game Logs Using %s cores:'%cores)
    pool = Pool(cores)
    for name,link in links.items():
        pool.apply_async(player_proc,args=(name,link,sleep_time,url),callback=timeouts.extend)
    pool.close()
    pool.join()
    if bool(timeouts):
        parse_links(timeouts)
"""
This will dynamically scrape out the table for an arbitrary table on 
pro-football-reference with an overheader. I wish we could just make a pandas
from_html call here, but these tables aren't as cleary defined in their structure
so this is our custom parser.

This will parse and write the playoff and the regular season table seperately
"""      
def parse_game_logs(game_logs,player):
    #Here we're finding if it's the regular season or the playoffs
    log_type = game_logs.find('caption').text.strip()
    player.gamelogs.append(Gamelog(log_type))
    #Here we're initializing dictionaries for what we're gonna combine down
    #to be a single header, instead  of two headers.
    ohead_index_dict = {}
    head_index_dict = {}
    head = game_logs.find('thead')
    for tr in head.find_all('tr'):
        if tr.has_attr('class'):
            #if this is the top header, we're gonna go in here and figure out
            #which columns it spans over to be able to add into our combined
            #header
            if 'over_header' in tr['class']:
                pos = 0
                for th in tr.find_all('th'):
                    if th.has_attr('colspan'):
                        #if it spans multiple columns, we're getting where
                        #it spans those new columns to
                        new_pos = pos + int(th['colspan'])
                        #there some empty overheaders
                        if not bool(th.text.strip()):
                            for i in range(pos,new_pos):
                                ohead_index_dict[i] = ''
                        #if there is an overheader, that is going to be the 
                        #overheader for the all the columns associated with it
                        else:
                            for i in range(pos,new_pos):
                                ohead_index_dict[i] = th.text.strip()
                        pos = new_pos
                    #if it doesn't span multiple columns, we're gonna use
                    #the overheader for just it's single column
                    else:
                        if not bool(th.text.strip()):
                            ohead_index_dict[pos] = ''
                        else:
                            ohead_index_dict[pos] = th.text.strip()
                        pos += 1
        #if it's just the plain header we're gonna come down here and add the 
        #overheader text in front of what would be the normal header
        else:
            for i,th in enumerate(tr.find_all('th')):
                if bool(ohead_index_dict):
                    #the proper header is going to be the overheader + the actual header
                    if bool(th.text.strip()):
                        head_index_dict[i] = (ohead_index_dict[i] + ' ' + th.text.strip()).strip()
                    else:
                        head_index_dict[i] = ohead_index_dict[i].strip()
                else:
                    if bool(th.text.strip()):
                        head_index_dict[i] = th.text.strip()
                    else:
                        head_index_dict[i] = ''
    #now we're gonna scrape all the table data
    body = game_logs.find('tbody')
    #they like to put a lot of headers in the center of the table for readability
    #we're just gonna skip those
    for tr in body.find_all('tr'):
        if tr.has_attr('class'):
            if 'thead' in tr['class']:
                continue
        row = {}
        th = tr.find('th')
        #the very first value in a row is going to be a th always
        row[head_index_dict[0]] = th.text.strip()
        #the rest will be tds so we'll just add one to that index to make things
        #work
        for i,td in enumerate(tr.find_all('td')):
            row[head_index_dict[i+1]] = td.text.strip()
        player.gamelogs[-1].rows.append(row)
    player.gamelogs[-1].create_df()
    write_player(player)

"""
Here we're writing the individual sheets associate with the player
"""
def write_player(player):
    if player.position.find('/') > 0:
        position = player.position.replace('/','-')
    else:
        position = player.position
    writer = pd.ExcelWriter('Players/%s-%s.xlsx'%(player.name,position),engine='xlsxwriter')
    for gamelog in player.gamelogs:
        gamelog.df.to_excel(writer,sheet_name=gamelog.log_type,index=False)
    writer.close()


if __name__ == "__main__":
    freeze_support()
    players = get_player_names(gofast=True)
    links = get_player_links(players,gofast=True)
    parse_links(links,gofast=True)


