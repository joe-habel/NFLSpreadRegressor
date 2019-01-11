"""
The generates training shees given the data from the team sheets and the player
sheets.

The idea behind utilizing both is that players abilities should have an influence
on game outcome and team/coaching actions should have an influence on other parts.

For example the ability to convert on 3rd downs, I'd describe more as a whole
team performance/play calling action than strictly on the QB.

Things though like how many rushing yards per attempt I'd argue are more indictive
of the ability of the RB more so than the team, at least on average. 

However the individual influence that a player has is also determined by how 
often they've contributed to the team. 

Especially things like more teams realizing that converting the back core to a 
team of RBs instead of a one man all star back. We can do this by adjusting 
how often in the past team memory that the back takes a rushing attempt, and
then their individual performance is weighted by that much to the combined
player influenced yards/rushing attempt.

I've split up the training into player weighted averages and team weighted averages.
The player weighted averages come from a players career performance over a specified
player memory of games, while the team weighted average comes from the team performance
over a specified team memory of games. In theory this should allows us to compensate
for trades and new players to the core, and their overall stats are what determine's
the players abilitly to the team.
"""
import pandas as pd
from glob import glob, iglob
import os
import pickle
from name_scraper import get_single_link, player_proc
from multiprocessing import Pool, cpu_count, freeze_support

"""
Again this tells us which PFR abbreviations correspond to what NFL team.
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
We're getting the vegas line from PFR to use as a baseline in our predicitive
performance.
"""
def get_vegas_spread(game,away):
    game_info = pd.read_excel(game,'Game Info')
    if game_info['Vegas Line'][0] == 'Pick':
        return 0.0
    if game_info['Vegas Line'][0].split(' ')[-2] == away:    
        return -float(game_info['Vegas Line'][0].split(' ')[-1])
    else:
        return game_info['Vegas Line'][0].split(' ')[-1]

"""
This gets the teams and date from the game file path.
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
This gets the week and year from the game file path.
"""
def get_year_week(path):
    week_path = os.path.abspath(os.path.join(path,os.pardir))
    year_path = os.path.abspath(os.path.join(week_path,os.pardir))
    week = os.path.basename(week_path).split(' ')[-1]
    year = os.path.basename(year_path)
    return year, week 

"""
This goes back and gets the previous N games from a player's career history.
If the memory value is none it gets all of the games the played in. This
will operate over the playoff and regular season sheets seperately.
"""
def get_back_ngames(player,date,player_memory,is_playoffs):
    if is_playoffs:
        sheet_name = 'Playoffs Table'
    else:
        sheet_name = 'Regular Season Table'
    if player_memory is None:
        try:
            fp = pd.read_excel(player, sheet_name = sheet_name)
            player_history = fp.loc[(pd.to_datetime(fp['Date']) < pd.to_datetime(date))]
        except:
            return pd.DataFrame()
    else:
        try:
            fp = pd.read_excel(player, sheet_name = sheet_name)
        except:
            return pd.DataFrame()
        player_history = fp.loc[(pd.to_datetime(fp['Date']) < pd.to_datetime(date))]
        player_history = player_history.sort_values('Date')
        player_history = player_history.tail(player_memory)
    return player_history
    
"""
This will get the player's carrer history from their game sheet, and well this
could probably do with some refactoring. 

This deals with whether or not there is a player memory, whether or not we care
about playoffs, whether or not if we do care about playoffs if the player has 
a playoff history, if the player has played in the playoffs what are the last
n games counting playoffs since they are stored in different sheets.

There's a lot going on here.

TODO: Refactor what's going on here. We can probably have a function for once
we have the right player, a function to get the right player, and a function
to filer out the number of dates
"""
def get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates):
    #Debug statement
    #print ('..... Getting %s career data'%player)
    players = glob(player_dir + '/%s*'%player)
    #If there's no player here we go back into name_scraper.py and get the link(s)
    #associated with that player, and write the sheets, and then recursively call
    #back to this function to restart the process.
    if len(players) < 1:
        links = get_single_link(player)
        for name, link in links.items():
            player_proc(name,link)
        return get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
    #If there are more than 1 players, we gotta figure out which player is the
    #right player. What we're doing is going into the player's history and seeing
    #if they played for the team we're interested in on the date we're interested
    #in.
    elif len(players) > 1:
        team_dict = get_team_dict()
        team_abb = team_dict[team]
        right_player = []
        for player_file in players:
            #The try statements are here to catch if the player doens't have
            #a regular season sheet or if they don't have a playoff sheet.
            try:
                season_player_history = pd.read_excel(player_file, 'Regular Season Table')
                check_player = season_player_history.loc[(pd.to_datetime(season_player_history['Date']).isin(pd.to_datetime(team_dates))) & (season_player_history['Tm'].isin(team_abb))]
            except:
                check_player = pd.DataFrame()
                pass
            if not check_player.empty:
                right_player.append(player_file)
                break
            if playoffs:
                try:
                    playoff_player_history = pd.read_excel(player_file, 'Playoffs Table')
                    check_player = playoff_player_history.loc[(pd.to_datetime(playoff_player_history['Date']).isin(pd.to_datetime(team_dates))) & (playoff_player_history['Tm'].isin(team_abb))]
                except:
                    pass
            if not check_player.empty:
                right_player.append(player_file)
                break
        #if we don't have the right player, we're gonna go back and scrape to get
        #the right player since we probably didn't scrape them and then recursively
        #call back to restart the process again.
        if not bool(right_player):
            links = get_single_link(player)
            for name, link in links.items():
                player_proc(name,link)
            return get_player_history(player,team,date,playoffs,player_memory,player_dir)
        #if there is a player that exists, we're gonna go back and try to get
        #the playoff and regular season history if applicable
        else:
            player = right_player[0]
            regular_season_history = get_back_ngames(player,date,player_memory,False)
            if playoffs:
                try:
                    playoff_history = get_back_ngames(player,date,player_memory,True)
                except:
                    playoff_history = None
    #if there is exactly one player, we can just go back and get that player's
    #history
    else:
        regular_season_history = get_back_ngames(players[0],date,player_memory,False)
        if playoffs:
            try:
                playoff_history = get_back_ngames(players[0],date,player_memory,True)
            except:
                playoff_history = None
    if playoffs:
        #if there is a player memory and we have playoffs in the way, we have
        #to get the last (player_memory) between the playoffs and regular season
        if player_memory is not None:
            if not playoff_history.empty:
                dates = pd.to_datetime(regular_season_history['Date']).values
                dates = list(dates)
                #we're getting these as datetimes so we can sort them
                playoff_dates = pd.to_datetime(playoff_history['Date']).values
                dates.extend(playoff_dates)
                #We're gonna use this list of dates to figure out the last
                #number of games they played in                
                dates = sorted(dates)
                #if they've only played the number of games or less, we're good
                if len(dates) <= player_memory:
                    return regular_season_history, playoff_history
                #or else we gotta pick the cutoff date and only select dates
                #from the regular season and playoff history that happen
                #before that date
                cutoff = dates[-player_memory]
                regular_season_history = regular_season_history.loc[(pd.to_datetime(regular_season_history['Date']) > pd.to_datetime(cutoff))]
                playoff_history = playoff_history.loc[(pd.to_datetime(playoff_history['Date']) > pd.to_datetime(cutoff))]
        return regular_season_history, playoff_history
    else:
        return regular_season_history

"""
Here we're getting the last (team_memory) from the team sheets in the Teams/
directory. It then returns all of the sheets that were in that memory range.

TODO: Change that while loop a sorted by dates and then just select the last
player memory
"""
def get_team_history(team,date,playoffs,team_memory,team_dir):
    sheets = pd.read_excel(os.path.join(team_dir,'%s.xlsx'%team),sheet_name=None)
    current_game = sheets['Game Stats'].loc[(pd.to_datetime(sheets['Game Stats']['Date']) == pd.to_datetime(date))]
    current_year, current_week = current_game['Year'].values[0], current_game['Week'].values[0]
    history_sheets = {}
    game_total = 0
    #With bye weeks being a thing, and teams not making the playoffs, we're
    #just gonna while loop throught the team memory to find the last number of 
    #games. We should just sort by date and select the last N up and to the 
    #current game
    while game_total < team_memory:
        if current_week == 1:
            if playoffs:
                current_week = 21
            else:
                current_week = 17
            current_year -= 1
        else:
            current_week -= 1
        current_game = sheets['Game Stats'].loc[(sheets['Game Stats']['Week'] == current_week) & (sheets['Game Stats']['Year'] == current_year)]
        if not current_game.empty:
            game_total += 1
            for sn, sheet in sheets.items():
                this_week = sheet.loc[(sheet['Week'] == current_week) & (sheet['Year'] == current_year)]
                try:
                    history_sheets[sn].append(this_week)
                except:
                    history_sheets[sn] = [this_week]
    for sn, sheet in history_sheets.items():
        history_sheets[sn] = pd.concat(sheet)
    return history_sheets

"""
There's a pattern here. These features functions are also a mess that's going
to need some refactoring. I'm handling all of the edge cases with try, excepts
intsead of using maybe a bunch of if/elifs, or maybe even functions.

You'll see this problem holds true for the rest of the player average features
that I catch the edge cases with a try except.

TODO: Refactor into elifs/functions
"""
def passing_features(passing_sheet,player_memory,team,date,player_dir,playoffs):
    passing_sheet = passing_sheet.fillna(0)
    players = passing_sheet['Player'].unique()
    sacks = passing_sheet['Pass Sk'].sum()
    team_dates = passing_sheet['Date'].unique()
    pass_completion = 0
    int_per_att = 0
    td_per_att = 0
    total_atts = 0
    pass_atts = {}
    pass_cmp = {}
    pass_ints = {}
    pass_tds = {}
    for player in players:
        #Here we get the passing stats for that indivdual player
        player_passing = passing_sheet.loc[passing_sheet['Player'] == player]
        #Here we figure out how many attempts that the player made for the
        #team in the team history
        pass_atts[player] = player_passing['Pass Att'].sum()
        #This is where it gets messy, and should probably be fixed into
        #some cleaner elifs
        if playoffs:
            reg, playoff = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            if playoff is not None:
                #We're first gonna try to see if they have a regular season
                #and a playoff history
                try:
                    player_atts = reg['Passing Att'].sum() + playoff['Passing Att'].sum()
                    #if they don't have any attempts these are going to be zero
                    #since we're normalizing for attempts
                    if player_atts == 0:
                        pass_cmp[player] = 0
                        pass_ints[player] = 0
                        pass_tds[player] = 0
                    #if they do, let's get the values we want to calculate and 
                    #normalize them with respect to their total atts
                    else:
                        pass_cmp[player] = (reg['Passing Cmp'].sum() + playoff['Passing Cmp'].sum())/(player_atts)
                        pass_ints[player] = (reg['Passing Int'].sum() + playoff['Passing Int'].sum())/(player_atts)
                        pass_tds[player] = (reg['Passing TD'].sum() + playoff['Passing TD'].sum())/(player_atts)            
                #If we get an error
                except KeyError:
                    #Is it because of the playoffs?
                    #if so basically do the same thing as above
                    try:
                        player_atts = reg['Passing Att'].sum()
                        if player_atts == 0:
                            pass_cmp[player] = 0
                            pass_ints[player] = 0
                            pass_tds[player] = 0
                        else:
                            pass_cmp[player] = reg['Passing Cmp'].sum()/(player_atts)
                            pass_ints[player] = reg['Passing Int'].sum()/(player_atts)
                            pass_tds[player] = reg['Passing TD'].sum()/(player_atts)
                    #maybe they've only played in the playoffs
                    except KeyError:
                        #if they did only play in the playoffs, again do 
                        #basically the same thing
                        try:
                            player_atts = playoff['Passing Att'].sum()
                            if player_atts == 0:
                                pass_cmp[player] = 0
                                pass_ints[player] = 0
                                pass_tds[player] = 0
                            else:
                                pass_cmp[player] = playoff['Passing Cmp'].sum()/(player_atts)
                                pass_ints[player] = playoff['Passing Int'].sum()/(player_atts)
                                pass_tds[player] = playoff['Passing TD'].sum()/(player_atts)
                        #if they didn't, they more than likely have a single
                        #attempt that went awry, so their stats are gonna 
                        #be zero
                        except KeyError:
                            pass_cmp[player] = 0
                            pass_ints[player] = 0
                            pass_tds[player] = 0
            #If they don't have any playoff history again basically do the same
            #thing for the regular season
            else:
                player_atts = reg['Passing Att'].sum()
                if player_atts == 0:
                    pass_cmp[player] = 0
                    pass_ints[player] = 0
                    pass_tds[player] = 0
                else:
                    pass_cmp[player] = reg['Passing Cmp'].sum()/(player_atts)
                    pass_ints[player] = reg['Passing Int'].sum()/(player_atts)
                    pass_tds[player] = reg['Passing TD'].sum()/(player_atts)
        #if we don't want playoffs just only do the regular season
        else:
            reg = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            player_atts = reg['Passing Att'].sum()
            if player_atts == 0:
                    pass_cmp[player] = 0
                    pass_ints[player] = 0
                    pass_tds[player] = 0
            else:
                pass_cmp[player] = reg['Passing Cmp'].sum()/(player_atts)
                pass_ints[player] = reg['Passing Int'].sum()/(player_atts)
                pass_tds[player] = reg['Passing TD'].sum()/(player_atts)
    #this is going to be the team total of attempts
    for player, atts in pass_atts.items():
        total_atts += atts
    #this is going to calculate how much the players own career performance in
    #theory affected the team in the past (team_memory) games. I.e. the 
    #starting QB threw 80% of the passes so their influence over the team would
    #have been .8 times their own stats
    for player, cmp in pass_cmp.items():
        if total_atts == 0:
            break
        pass_completion += cmp*(pass_atts[player]/total_atts)
        int_per_att += pass_ints[player]*(pass_atts[player]/total_atts)
        td_per_att += pass_tds[player]*(pass_atts[player]/total_atts)
    return {'Passing Completion':pass_completion,
            'Interceptions Per Passing Attempt':int_per_att,
            'Touchdowns Per Passing Attempts':td_per_att}, total_atts, sacks

"""
This is probably the worst way to document the rest of these functions 
but the pattern follows exactly like the passing pattern does above.
The try, excepts are all for the same cases. And the attempt player weights
is the same process for how the final values are calculated.

I hate to say refer to the passing_features to undertand how this works,
but refer to passing_features to see what the hell all of these nested
try/excepts, and if/elses are catching.

TODO: Refactor into elifs/functions
"""
def rushing_features(rushing_sheet,player_memory,team,date,player_dir,playoffs):
    rushing_sheet = rushing_sheet.fillna(0)
    players = rushing_sheet['Player'].unique()
    team_dates = rushing_sheet['Date'].unique()
    yds_per_att = 0
    tds_per_att = 0
    total_atts = 0
    rush_atts = {}
    rush_yds = {}
    rush_tds = {}
    for player in players:
        player_rushing = rushing_sheet.loc[rushing_sheet['Player'] == player]
        rush_atts[player] = player_rushing['Rush Att'].sum()
        if playoffs:
            reg, playoff = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            if playoff is not None:
                try:
                    player_atts = reg['Rushing Att'].sum() + playoff['Rushing Att'].sum()
                    if player_atts == 0:
                        rush_yds[player] = 0
                        rush_tds[player] = 0
                    else:
                        rush_yds[player] = (reg['Rushing Yds'].sum() + playoff['Rushing Yds'].sum())/(player_atts)
                        rush_tds[player] = (reg['Rushing TD'].sum() + playoff['Rushing TD'].sum())/(player_atts)
                except KeyError:
                    try:
                        player_atts = reg['Rushing Att'].sum()
                        if player_atts == 0:
                            rush_yds[player] = 0
                            rush_tds[player] = 0
                        else:
                            rush_yds[player] = reg['Rushing Att'].sum()/player_atts
                            rush_tds[player] = reg['Rushing TD'].sum()/player_atts   
                    except KeyError:
                        try:
                            player_atts = playoff['Rushing Att'].sum()
                            if player_atts == 0:
                                rush_yds[player] = 0
                                rush_tds[player] = 0
                            else:
                                rush_yds[player] = playoff['Rushing Att'].sum()/player_atts
                                rush_tds[player] = playoff['Rushing TD'].sum()/player_atts 
                        except KeyError:
                            rush_yds[player] = 0
                            rush_tds[player] = 0
            else:
                player_atts = reg['Rushing Att'].sum()
                if player_atts == 0:
                    rush_yds[player] = 0
                    rush_tds[player] = 0
                else:
                    rush_yds[player] = reg['Rushing Att'].sum()/player_atts
                    rush_tds[player] = reg['Rushing TD'].sum()/player_atts
        else:
            reg = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            player_atts = reg['Rushing Att'].sum()
            if player_atts == 0:
                rush_yds[player] = 0
                rush_tds[player] = 0
            else:
                rush_yds[player] = reg['Rushing Att'].sum()/player_atts
                rush_tds[player] = reg['Rushing TD'].sum()/player_atts
    for player, atts in rush_atts.items():
        total_atts += atts
    for player, yds in rush_yds.items():
        if total_atts == 0:
            break
        yds_per_att += yds*(rush_atts[player]/total_atts)
        tds_per_att += rush_tds[player]*(rush_atts[player]/total_atts)
    return {'Yards Per Rushing Attempt':yds_per_att, 
            'Touchdowns Per Rushing Attempt':tds_per_att}, total_atts
"""
I hate to say refer to the passing_features to undertand how this works,
but refer to passing_features to see what the hell all of these nested
try/excepts, and if/elses are catching.

TODO: Refactor into elifs/functions
"""
def receiving_features(receiving_sheet,player_memory,team,date,player_dir,playoffs):
    receiving_sheet = receiving_sheet.fillna(0)
    players = receiving_sheet['Player'].unique()
    team_dates = receiving_sheet['Date'].unique()
    catch_percent = 0
    yds_per_catch = 0
    tds_per_catch = 0
    total_tgts = 0
    rec_tgts = {}
    rec_catch = {}
    rec_yds = {}
    rec_tds = {}
    for player in players:
        player_rec = receiving_sheet.loc[receiving_sheet['Player'] == player]
        rec_tgts[player] = player_rec['Receive Tgt'].sum()
        if playoffs:
            reg, playoff = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            if playoff is not None:
                try:
                    player_catches = reg['Receiving Rec'].sum() + playoff['Receiving Rec'].sum()
                    if player_catches == 0:
                        rec_catch[player] = 0
                        rec_yds[player] = 0
                        rec_tds[player] = 0
                    else:
                        rec_catch[player] = player_catches/(reg['Receiving Tgt'].sum() + playoff['Receiving Tgt'].sum())
                        rec_yds[player] = (reg['Receiving Yds'].sum() + playoff['Receiving Yds'].sum())/player_catches
                        rec_tds[player] = (reg['Receiving TD'].sum() + playoff['Receiving TD'].sum())/player_catches
                except KeyError:
                     try:
                         player_catches = reg['Receiving Rec'].sum()
                         if player_catches == 0:
                            rec_catch[player] = 0
                            rec_yds[player] = 0
                            rec_tds[player] = 0
                         else:
                            rec_catch[player] = player_catches/reg['Receiving Tgt'].sum()
                            rec_yds[player] = reg['Receiving Yds'].sum()/player_catches
                            rec_tds[player] = reg['Receiving TD'].sum()/player_catches
                     except KeyError:
                         try:
                             player_catches = playoff['Receiving Rec'].sum()
                             if player_catches == 0:
                                rec_catch[player] = 0
                                rec_yds[player] = 0
                                rec_tds[player] = 0
                             else:
                                rec_catch[player] = player_catches/playoff['Receiving Tgt'].sum()
                                rec_yds[player] = playoff['Receiving Yds'].sum()/player_catches
                                rec_tds[player] = playoff['Receiving TD'].sum()/player_catches
                         except KeyError:
                            rec_catch[player] = 0
                            rec_yds[player] = 0
                            rec_tds[player] = 0
            else:
                try:
                    player_catches = reg['Receiving Rec'].sum()
                    if player_catches == 0:
                        rec_catch[player] = 0
                        rec_yds[player] = 0
                        rec_tds[player] = 0
                    else:
                        rec_catch[player] = player_catches/reg['Receiving Tgt'].sum()
                        rec_yds[player] = reg['Receiving Yds'].sum()/player_catches
                        rec_tds[player] = reg['Receiving TD'].sum()/player_catches
                except KeyError:
                    rec_catch[player] = 0
                    rec_yds[player] = 0
                    rec_tds[player] = 0
        else:
            reg = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            player_catches = reg['Receiving Rec'].sum()
            if player_catches == 0:
                    rec_catch[player] = 0
                    rec_yds[player] = 0
                    rec_tds[player] = 0
            else:
                rec_catch[player] = player_catches/reg['Receiving Tgt'].sum()
                rec_yds[player] = reg['Receiving Yds'].sum()/player_catches
                rec_tds[player] = reg['Receiving TD'].sum()/player_catches
    for player, tgts in rec_tgts.items():
        total_tgts += tgts
    for player, catches in rec_catch.items():
        if total_tgts == 0:
            break
        catch_percent += catches*(rec_tgts[player]/total_tgts)
        yds_per_catch += rec_yds[player]*(rec_tgts[player]/total_tgts)
        tds_per_catch += rec_tds[player]*(rec_tgts[player]/total_tgts)
    return {'Catch Percent':catch_percent, 
            'Yards Per Catch':yds_per_catch, 
            'Touchdowns Per Catch':tds_per_catch}
"""
I hate to say refer to the passing_features to undertand how this works,
but refer to passing_features to see what the hell all of these nested
try/excepts, and if/elses are catching.

This one is a little different though in the sense we're doing these try/except
and if/else catches for two independent stats instead of just one.

TODO: Refactor into elifs/functions
"""
def kicking_features(kicking_sheet,player_memory,team,date,player_dir,playoffs):
    kicking_sheet = kicking_sheet.fillna(0)
    players = kicking_sheet['Player'].unique()
    team_dates = kicking_sheet['Date'].unique()
    fgm = 0
    xpm = 0
    total_fga = 0
    total_xpa = 0
    team_fga = {}
    team_xpa = {}
    kick_fgm = {}
    kick_xpm = {}
    for player in players:
        player_kick = kicking_sheet.loc[kicking_sheet['Player'] == player]
        team_fga[player] = player_kick['Scoring FGA'].sum()
        team_xpa[player] = player_kick['Scoring XPA'].sum()
        if playoffs:
            reg, playoff = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            if playoff is not None:
                try:
                    fgas = reg['Scoring FGA'].sum() + playoff['Scoring FGA'].sum()
                    if fgas == 0:
                        kick_fgm[player] = 0
                    else:
                        kick_fgm[player] = (reg['Scoring FGM'].sum() + playoff['Scoring FGM'].sum())/fgas
                except KeyError:
                    try:
                        fgas = reg['Scoring FGA'].sum()
                        if fgas == 0:
                            kick_fgm[player] = 0
                        else:
                            kick_fgm[player] = reg['Scoring FGM'].sum()/fgas
                    except KeyError:
                        try:
                            fgas = playoff['Scoring FGA'].sum()
                            if fgas == 0:
                                kick_fgm[player] = 0
                            else:
                                kick_fgm[player] = playoff['Scoring FGM'].sum()/fgas
                        except KeyError:
                            kick_fgm[player] = 0
                try:
                    xpas = reg['Scoring XPA'].sum() + playoff['Scoring XPA'].sum()
                    if xpas == 0:
                        kick_xpm[player] = 0
                    else:
                        kick_xpm[player] = (reg['Scoring XPM'].sum() + playoff['Scoring XPM'].sum())/xpas
                except KeyError:
                    try:
                        xpas = reg['Scoring XPA'].sum()
                        if xpas == 0:
                            kick_xpm[player] = 0
                        else:
                            kick_xpm[player] = reg['Scoring XPM'].sum()/xpas
                    except KeyError:
                        try:
                            xpas = playoff['Scoring XPA'].sum()
                            if xpas == 0:
                                kick_xpm[player] = 0
                            else:
                                kick_xpm[player] = playoff['Scoring XPM'].sum()/xpas
                        except KeyError:
                            kick_xpm[player] = 0
            else:
                fgas = reg['Scoring FGA'].sum()
                xpas = reg['Scoring XPA'].sum()
                if fgas == 0:
                    kick_fgm[player] = 0
                else:
                    kick_fgm[player] = reg['Scoring FGM'].sum()/fgas
                if xpas == 0:
                    kick_xpm[player] = 0
                else:
                    kick_xpm[player] = reg['Scoring XPM'].sum()/xpas
        else:
            reg = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            fgas = reg['Scoring FGA'].sum()
            xpas = reg['Scoring XPA'].sum()
            if fgas == 0:
                kick_fgm[player] = 0
            else:
                kick_fgm[player] = reg['Scoring FGM'].sum()/fgas
            if xpas == 0:
                kick_xpm[player] = 0
            else:
                kick_xpm[player] = reg['Scoring XPM'].sum()/xpas
    for player, fga in team_fga.items():
        total_fga += fga
        total_xpa += team_xpa[player]
    for player, kickfgm in kick_fgm.items():
        if total_fga != 0:
            try:
                fgm += kickfgm*(team_fga[player]/total_fga)
            except KeyError:
                pass
        if total_xpa != 0:
            try:
                xpm += kick_xpm[player]*(team_xpa[player]/total_xpa)
            except KeyError:
                pass
    return {'FGM Percentage':fgm, 
            'XPM Percentage':xpm}, total_fga
"""
I hate to say refer to the passing_features to undertand how this works,
but refer to passing_features to see what the hell all of these nested
try/excepts, and if/elses are catching.

TODO: Refactor into elifs/functions
"""
def kr_features(kr_sheet,player_memory,team,date,player_dir,playoffs):
    kr_sheet = kr_sheet.fillna(0)
    players = kr_sheet['Player'].unique()
    team_dates = kr_sheet['Date'].unique()
    total_atts = 0
    yds_per_kr = 0
    tds_per_kr = 0
    kr_atts = {}
    kr_yds = {}
    kr_tds = {}
    for player in players:
        player_kr = kr_sheet.loc[kr_sheet['Player'] == player]
        kr_atts[player] = player_kr['KR Rt'].sum()
        if playoffs:
            reg, playoff = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            if playoff is not None:
                try:
                    player_atts = reg['Kick Returns Rt'].sum() + playoff['Kick Returns Rt'].sum()
                    if player_atts == 0:
                        kr_yds[player] = 0
                        kr_tds[player] = 0
                    else:
                        kr_yds[player] = (reg['Kick Returns Yds'].sum() + playoff['Kick Returns Yds'].sum())/player_atts
                        kr_tds[player] = (reg['Kick Returns TD'].sum() + playoff['Kick Returns TD'].sum())/player_atts
                except KeyError:
                    try:
                        player_atts = reg['Kick Returns Rt'].sum()
                        if player_atts == 0:
                            kr_yds[player] = 0
                            kr_tds[player] = 0
                        else:
                            kr_yds[player] = reg['Kick Returns Yds'].sum()/player_atts
                            kr_tds[player] = reg['Kick Returns TD'].sum()/player_atts
                    except KeyError:
                        try:
                            player_atts = playoff['Kick Returns Rt'].sum()
                            if player_atts == 0:
                                kr_yds[player] = 0
                                kr_tds[player] = 0
                            else:
                                kr_yds[player] = playoff['Kick Returns Yds'].sum()/player_atts
                                kr_tds[player] = playoff['Kick Returns TD'].sum()/player_atts
                        except KeyError:
                            kr_yds[player] = 0
                            kr_tds[player] = 0
            else:
                player_atts = reg['Kick Returns Rt'].sum()
                if player_atts == 0:
                    kr_yds[player] = 0
                    kr_tds[player] = 0
                else:
                    kr_yds[player] = reg['Kick Returns Yds'].sum()/player_atts
                    kr_tds[player] = reg['Kick Returns TD'].sum()/player_atts
        else:
            reg = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            player_atts = reg['Kick Returns Rt'].sum()
            if player_atts == 0:
                kr_yds[player] = 0
                kr_tds[player] = 0
            else:
                kr_yds[player] = reg['Kick Returns Yds'].sum()/player_atts
                kr_tds[player] = reg['Kick Returns TD'].sum()/player_atts
    for player, att in kr_atts.items():
        total_atts += att
    for player, yds in kr_yds.items():
        if total_atts == 0:
            break
        yds_per_kr += yds*(kr_atts[player]/total_atts)
    return {'Yards Per Kick Return':yds_per_kr, 
            'Touchdowns Per Kick Return':tds_per_kr}, total_atts
"""
I hate to say refer to the passing_features to undertand how this works,
but refer to passing_features to see what the hell all of these nested
try/excepts, and if/elses are catching.

TODO: Refactor into elifs/functions
"""
def pr_features(pr_sheet,player_memory,team,date,player_dir,playoffs):
    pr_sheet = pr_sheet.fillna(0)
    players = pr_sheet['Player'].unique()
    team_dates = pr_sheet['Date'].unique()
    total_atts = 0
    yds_per_pr = 0
    tds_per_pr = 0
    pr_atts = {}
    pr_yds = {}
    pr_tds = {}
    for player in players:
        player_pr = pr_sheet.loc[pr_sheet['Player'] == player]
        pr_atts[player] = player_pr['PR Ret'].sum()
        if playoffs:
            reg, playoff = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            if playoff is not None:
                try:
                    player_atts = reg['Punt Returns Ret'].sum()  + playoff['Punt Returns Ret'].sum()
                    if player_atts == 0:
                        pr_yds[player] = 0
                        pr_tds[player] = 0
                    else:
                        pr_yds[player] =(reg['Punt Returns Yds'].sum()  + playoff['Punt Returns Yds'].sum())/player_atts
                        pr_tds[player] = (reg['Punt Returns TD'].sum()  + playoff['Punt Returns TD'].sum())/player_atts
                except KeyError:
                    try:
                        player_atts = reg['Punt Returns Ret'].sum()
                        if player_atts == 0:
                            pr_yds[player] = 0
                            pr_tds[player] = 0
                        else:
                            pr_yds[player] = reg['Punt Returns Yds'].sum()/player_atts
                            pr_tds[player] = reg['Punt Returns TD'].sum()/player_atts
                    except KeyError:
                        try:
                            player_atts = playoff['Punt Returns Ret'].sum()
                            if player_atts == 0:
                                pr_yds[player] = 0
                                pr_tds[player] = 0
                            else:
                                pr_yds[player] = playoff['Punt Returns Yds'].sum()/player_atts
                                pr_tds[player] = playoff['Punt Returns TD'].sum()/player_atts
                        except KeyError:
                            pr_yds[player] = 0
                            pr_tds[player] = 0
            else:
                player_atts = reg['Punt Returns Ret'].sum()
                if player_atts == 0:
                    pr_yds[player] = 0
                    pr_tds[player] = 0
                else:
                    pr_yds[player] = reg['Punt Returns Yds'].sum()/player_atts
                    pr_tds[player] = reg['Punt Returns TD'].sum()/player_atts
        else:
            reg = get_player_history(player,team,date,playoffs,player_memory,player_dir,team_dates)
            player_atts = reg['Punt Returns Ret'].sum()
            if player_atts == 0:
                    pr_yds[player] = 0
                    pr_tds[player] = 0
            else:
                pr_yds[player] = reg['Punt Returns Yds'].sum()/player_atts
                pr_tds[player] = reg['Punt Returns TD'].sum()/player_atts
    for player, att in pr_atts.items():
        total_atts += att
    for player, yds in pr_yds.items():
        if total_atts == 0:
            break
        yds_per_pr += yds*(pr_atts[player]/total_atts)
        tds_per_pr += pr_tds[player]*(pr_atts[player]/total_atts)
    return {'Yards Per Punt Return':yds_per_pr, 
            'Touchdowns Per Punt Return':tds_per_pr}, total_atts

"""
Just getting the total number of punts
"""
def punt_totals(punt_sheet):
    return punt_sheet['Scoring Pnt'].sum()

"""
Here we're getting the offensive stats that would be team influenced.
We're gonna do a zero check before normalizing, so we don't get divide by zero
errors for things that never happened in the time frame
"""
def offense_features(game_stats,pass_atts,rush_atts,kr_atts,pr_atts,fga,punts,sacks,team_memory):
    fumbles_per_plays = game_stats['Fumbles'].sum()/(pass_atts+rush_atts+kr_atts+pr_atts+fga+punts)
    if game_stats['Fumbles'].sum() == 0:
        fumbles_lost_per_fumble=0
    else:
        fumbles_lost_per_fumble = game_stats['Fumbles Lost'].sum()/game_stats['Fumbles'].sum()
    third_down_cnv = game_stats['Third Down Cvt'].sum()/game_stats['Third Down Att'].sum()
    if game_stats['Fourth Down Att'].sum() == 0:
        fourth_down_cnv = 0
    else:
        fourth_down_cnv = game_stats['Fourth Down Cvt'].sum()/game_stats['Fourth Down Att'].sum()
    go_for_it_perc = game_stats['Fourth Down Att'].sum()/(game_stats['Fourth Down Att'].sum() + fga + punts)
    punt_perc = punts/(game_stats['Fourth Down Att'].sum() + fga + punts)
    fg_perc = fga/(game_stats['Fourth Down Att'].sum() + fga + punts)
    first_downs_perc = game_stats['First Downs'].sum()/(pass_atts+rush_atts+fga+punts)
    sacks_per_pass = sacks/pass_atts
    pass_atts = pass_atts/team_memory
    rush_atts = rush_atts/team_memory
    fga = fga/team_memory
    punts = punts/team_memory
    return {'Pass Attempts Per Game':pass_atts,
            'Rush Attempts Per Game':rush_atts,
            'FGA Per Game':fga,
            'Punts Per Game':punts,
            'Fumbles Per Play':fumbles_per_plays,
            'Fumbles Lost Percentage':fumbles_lost_per_fumble,
            'Third Down Conversion':third_down_cnv,
            'Fourth Down Conversion':fourth_down_cnv,
            'Fourth Down Attempt Rate':go_for_it_perc,
            'Punt Rate':punt_perc,
            'FGA Rate':fg_perc,
            'First Downs per Offensive Play':first_downs_perc,
            'Sacks Per Passing Attempt':sacks_per_pass}

"""
Here we're getting the defenseive features from the team sheets, defensive
features are all going to be team influenced, since PFR does such a poor job
of recording defensive stats, and they didn't start recording snap counts until
I think it's 2014ish. So we can't really measure what influence these players
really have per game.
"""
def defensive_features(defense):
    opp_pass_atts = defense['Pass Attempts Defended'].sum()
    opp_rush_atts = defense['Rush Attempts Defended'].sum()
    opp_krs = defense['Kick Returns Defended'].sum()
    opp_prs = defense['Punt Returns Defended'].sum()
    opp_fga = defense['Field Goals Defended'].sum()
    opp_punts = defense['Punts Defended'].sum()
    
    pass_yds_per_att = defense['Pass Yards Allowed'].sum()/opp_pass_atts
    pass_tds_per_att = defense['Pass TDs Allowed'].sum()/opp_pass_atts
    ints_per_pass_att = defense['DefInt Int'].sum()/opp_pass_atts
    sacks_per_pass_att = defense['Sck&Ttl Sk'].sum()/opp_pass_atts
    if defense['DefInt Int'].sum() == 0:
        tds_per_int = 0
    else:
        tds_per_int = defense['DefInt TD'].sum()/defense['DefInt Int'].sum()
    rush_yds_per_att = defense['Rush Yards Allowed'].sum()/opp_rush_atts
    rush_tds_per_att = defense['Rush TDs Allowed'].sum()/opp_rush_atts
    fumbles_per_att = defense['Fumble FF'].sum()/float(opp_pass_atts + opp_rush_atts + opp_krs + opp_prs)
    if defense['Fumble FF'].sum() == 0:
        fumbles_recovered = 0
    else:
        fumbles_recovered = defense['Fumble FR'].sum()/defense['Fumble FF'].sum()
    if defense['Fumble FR'].sum() == 0:
        tds_per_fr = 0
    else:
        tds_per_fr = defense['Fumble TD'].sum()/defense['Fumble FR'].sum()
    third_down_stopped = defense['Third Downs Stopped'].sum()/defense['Third Downs Defended'].sum()
    if defense['Fourth Downs Defended'].sum() == 0:
        fourth_down_stopped = 0
    else:
        fourth_down_stopped = defense['Fourth Downs Stopped'].sum()/defense['Fourth Downs Defended'].sum()
    fga_per_fourth = opp_fga/(opp_fga + defense['Fourth Downs Defended'].sum() + opp_punts)
    punts_per_fourth = opp_punts/(opp_fga + defense['Fourth Downs Defended'].sum() + opp_punts)
    yds_per_kr = defense['Kick Return Yards Allowed'].sum()/opp_krs
    tds_per_kr = defense['Kick Return TDs Allowed'].sum()/opp_krs
    if opp_prs == 0:
        yds_per_pr = 0
        tds_per_pr = 0
    else:
        yds_per_pr = defense['Punt Return Yards Allowed'].sum()/opp_prs
        tds_per_pr = defense['Punt Return TDs Allowed'].sum()/opp_prs
    return [{'Pass Yards Allowed Per Opp Pass Att':pass_yds_per_att, 
            'Pass TDS Allowed Per Opp Pass Att':pass_tds_per_att,
            'Interceptions Per Opp Pass Att':ints_per_pass_att, 
            'Sacks Per Opp Pass Att':sacks_per_pass_att, 
            'Touchdowns Per Interception':tds_per_int,
            'Rush Yards Allowed Per Opp Rush Att':rush_yds_per_att,
            'Rush TDS Allowed Per Opp Rush Att':rush_tds_per_att,
            'Fumbles Forced Per Play':fumbles_per_att, 
            'Fumbles Recovered Per Fumbles Forced':fumbles_recovered,
            'Touchdowns Per Fumbles Recovered':tds_per_fr, 
            'Third Down Stoppage Rate':third_down_stopped,
            'Fourth Down Stoppage Rate':fourth_down_stopped, 
            'Opp FGA per Fourth Down':fga_per_fourth,
            'Opp Punts Per Fourth Down':punts_per_fourth,
            'KR Yards Allowed Per Opp KR Att':yds_per_kr, 
            'Touchdowns Allowed Per Opp KR Att':tds_per_kr,
            'PR Yards Allowed Per Opp PR Att':yds_per_pr, 
            'Touchdowns Allowed Per Opp PR':tds_per_pr}]

"""
Here we're wrapping all of the offesive features into a nice clean funciton.
"""
def get_offensive_features(sheets,player_memory,date,team,player_dir,playoffs,team_memory):
    dict_list = []
    #print ('.... Passing')
    passing,pass_atts,sacks = passing_features(sheets['Passing'],player_memory,team,date,player_dir,playoffs)
    #print ('.... Rushing')
    rushing, rush_atts = rushing_features(sheets['Rushing'],player_memory,team,date,player_dir,playoffs) 
    #print ('.... Rec')
    receiving = receiving_features(sheets['Rec'],player_memory,team,date,player_dir,playoffs)
    #print ('.... Kicking')
    kicking, fga = kicking_features(sheets['Kicking'],player_memory,team,date,player_dir,playoffs)
    #print ('.... Kr')
    kr, kr_atts = kr_features(sheets['Kick Return'],player_memory,team,date,player_dir,playoffs)
    #print ('.... Pr')
    pr, pr_atts = pr_features(sheets['Punt Return'],player_memory,team,date,player_dir,playoffs)
    punts = punt_totals(sheets['Punting'])
    #print ('Supplemental')
    other_offense = offense_features(sheets['Game Stats'],pass_atts,rush_atts,kr_atts,pr_atts,fga,punts,sacks,team_memory)
    dict_list.extend([passing,rushing,receiving,kicking,kr,pr,other_offense])
    return dict_list
"""
Here we're getting our predictor variable from the game sheet.
Since a winning line is normally a negative home value, we're gonna subtract
away-home to get this predictor.
"""
def get_score_difference(game,away,home):
    scoring = pd.read_excel(game,sheet_name='Scoring')
    team_dict = get_team_dict()
    for team, abbs in team_dict.items():
        if team == home:
            home_abbs = abbs
        if team == away:
            away_abbs = abbs
    for col in scoring.columns:
        if col in home_abbs:
            home_abb = col
        if col in away_abbs:
            away_abb = col
    return scoring[away_abb].values[-1] - scoring[home_abb].values[-1]

"""
Here is our multiprocessing worker process for getting the home and away teams 
training data. The X is our feature array, the Y is the differential we're 
trying to predict, and the Z, is our baseline we're comparing against.
"""
def get_xy(game,player_memory,team_memory,playoffs,team_dir,player_dir):
    away, home, date = get_teams(game)
    print ('...',away, home, date)
    away_history = get_team_history(away,date,playoffs,team_memory,team_dir)
    home_history = get_team_history(home,date,playoffs,team_memory,team_dir)    
    home_features = get_offensive_features(home_history,player_memory,date,home,player_dir,playoffs,team_memory)
    away_features = get_offensive_features(away_history,player_memory,date,away,player_dir,playoffs,team_memory)
    home_features.extend(defensive_features(home_history['Defense']))
    away_features.extend(defensive_features(away_history['Defense']))
    y = get_score_difference(game,away,home)
    home_features = [{'Home ' + key : val for key, val in feature.items()} for feature in home_features]
    away_features = [{'Away '+  key : val for key, val in feature.items()} for feature in away_features]
    home_features.extend(away_features)
    x = {}
    for feature in home_features:
        x.update(feature)
    z = get_vegas_spread(game,away)
    return x,y,z

"""
This is wrapping the callback to append to each of the global handlers.
"""
def xy_callback(xyz):
    x,y,z = xyz
    x_data.append(x)
    y_data.append(y)
    z_data.append(z)
    
"""
This is going to tell us any errors that happen in our worker processes. Since
by default multiprocessing won't raise them. We'll also append them to a file
so we don't have to watch the callback for thousands of players.
"""
def error_handler(e):
    print('error')
    print(dir(e), "\n")
    print("-->{}<--".format(e.__cause__))
    with open('error_log.txt', 'a') as log:
        log.write("-->{}<--".format(e.__cause__))
"""
This makes the calls to the multiprocessing calls to the worker processes.
"""
def make_initial_training(player_memory=None,team_memory=10,playoffs=True,gofast=True,game_dir='Games',player_dir='Players',team_dir='Teams',start_year=2003):
    if not gofast:
        cores = int(cpu_count()*.75)
    else:
        cores = int(cpu_count()*0.9)
    pool = Pool(cores)
    print ("Generating training data using %s cores:"%cores)
    for game in iglob(game_dir + '/**/*.xlsx', recursive = True):
        year, week = get_year_week(game)
        skip_game = False
        if int(year) == start_year:
            if int(week) < team_memory + 2:
                skip_game = True
        if skip_game == True:
            continue
        #xy_callback(get_xy(game,player_memory,team_memory,playoffs,team_dir,player_dir))
        pool.apply_async(get_xy,args=(game,player_memory,team_memory,playoffs,team_dir,player_dir),callback=xy_callback,error_callback=error_handler)
    pool.close()
    pool.join()
"""
For once I have a messy main that should be cleaned up.
This will declare the global XYZ, initiate the player_memory, team_memory,
and if we care about playoff performance. It will also write the training 
data to pickled python object and to an excel file.

TODO: Write an argparser for player_memory, team_memory, and playoffs. Write
writing functiosn.
"""
if __name__ == '__main__':
    global x_data
    x_data = []
    global y_data
    y_data = []
    global z_data
    z_data = []
    freeze_support()
    player_memory = 8
    team_memory = 4
    playoffs = True
    make_initial_training(player_memory,team_memory,playoffs)
    training_data = (x_data,y_data,z_data)
    x_data = pd.DataFrame(x_data)
    x_data['Score Differential'] = y_data
    x_data['Vegas Baseline'] = z_data
    if player_memory is None:
        player_memory = 'All'
    with open('Training/Player-%s,Team-%s,Playoffs-%s.pckl'%(player_memory,team_memory,playoffs), 'wb') as train:
        pickle.dump(training_data,train)
    writer = pd.ExcelWriter('Training/Player-%s,Team-%s,Playoffs-%s.xlsx'%(player_memory,team_memory,playoffs), engine='xlsxwriter')
    x_data.to_excel(writer,index=None)
    writer.close()
    