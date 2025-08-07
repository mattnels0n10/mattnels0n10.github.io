import requests
import pandas as pd
from datetime import datetime,timedelta
import time
from sqlalchemy import create_engine
import numpy as np
import concurrent.futures
import pyodbc

############## Check lines 63 and 69 ---> changed exit() to pass

# Define retry decorator
def retry_on_failure(retries=3, delay=5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error in {func.__name__}: {e}")
                    if attempt < retries:
                        print(f"Retrying ({attempt}/{retries}) in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print(f"Exceeded retries for {func.__name__}.")
                        raise
        return wrapper
    return decorator


connection_string = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=YEMEN;"  # Update with your server name
    "DATABASE=MLB App;"
    "Trusted_Connection=yes;"
)

# Create an SQLAlchemy engine
engine = create_engine(connection_string)


# Function to fetch game logs for a single sportId
#@retry_on_failure(retries=3, delay=5)
def fetch_game_logs_for_sport(sport_id, start_date, end_date):
    
    #sport_ids = [1,11,12,13,14,16]
    
    # Convert string dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    game_pk_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId={sport_id}&startDate={start_date_str}&endDate={end_date_str}"
    print(f"Game PK URL: {game_pk_url}")
    response = requests.get(game_pk_url)

    
    if response.status_code != 200:
        print(f"Failed to fetch data for the date range {end_date_str} to {end_date_str} with sportId {sport_id}")
        pass
       
    schedule_data = response.json()
    
    if not schedule_data.get('dates'):
        print(f"No schedule data available for the date range {end_date_str} to {end_date_str}.")
        pass
    
    # Fetch game PKs for the date and sportId
    game_pks_all_dates = []  # Initialize a list to store game PKs for all dates
    game_types = []  # Initialize a list for game types
    game_dates = []
    
    for date_data in schedule_data['dates']:
        date_str = date_data['date']  # Extract the date string
        print(f"Processing data for date: {date_str}")
    
        games = date_data['games']  # Access games for this date
        
        # Filter games where 'date_str' matches 'officialDate'
        filtered_games = [game for game in games if game['officialDate'] == date_str]
        
        # Print results
        print(f"Filtered games count: {len(filtered_games)}")
        for game in filtered_games:
            print(f"GamePk: {game['gamePk']} matches date_str {date_str}")
            
            # Append the game pk and game type
            game_pks_all_dates.append(game['gamePk'])
            game_types.append(game.get('gameType', 'Unknown'))  # Defaulting to 'Unknown' if gameType is missing
            game_dates.append(date_str)
            
    # Now create a DataFrame for the filtered games
    df = pd.DataFrame({'game_pk': game_pks_all_dates,
                       'game_type': game_types,
                       'game_date': game_dates})
    
    game_logs = pd.DataFrame()
    
    for i, row in df.iterrows():
        game_pk = row['game_pk']
        game_type = row['game_type']
        game_date = row['game_date']
        
        # Fetch boxscore data
        boxscore_url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
        response = requests.get(boxscore_url)
        boxscore_data = response.json()
        
        home_id = boxscore_data['teams']['home']['team']['id']
        away_id = boxscore_data['teams']['away']['team']['id']
        
        home_away = ['home', 'away']
        
        # Process batters and pitchers
        for game_num, team in enumerate(home_away, 1):  # Adding a game_num counter starting from 1
            #print(f"Processing game {game_num} for team: {team} on {date_str}")  # Include game number and date in the print statement
            
            # Batters
            batters = boxscore_data['teams'][team]['batters']
            
            #print(f"Found {len(batters)} batters for team {team}")
            
            for batter in batters:
                player_key = f"ID{batter}"
                try:
                    batting_stats = boxscore_data['teams'][team]['players'][player_key]['stats']['batting']
                    if batting_stats:
                        
                        #print(f"Batting stats found for batter: {batter}") 
                        
                        
                        batting_stats_df = pd.DataFrame([batting_stats])
                        batting_stats_df['stolenBasePercentage'] = pd.to_numeric(batting_stats_df['stolenBasePercentage'], errors='coerce')
                        batting_stats_df['atBatsPerHomeRun'] = pd.to_numeric(batting_stats_df['atBatsPerHomeRun'], errors='coerce')

                        batting_stats_df['player_id'] = batter
                        batting_stats_df['game_pk'] = game_pk
                        batting_stats_df['team'] = team
                        batting_stats_df['sportId'] = sport_id
                        batting_stats_df['role'] = 'batter'
                        batting_stats_df['game_date'] = game_date
                        batting_stats_df['game_year'] = pd.to_datetime(batting_stats_df['game_date']).dt.year
                        batting_stats_df['game_type'] = game_type
                        batting_stats_df['home_id'] = home_id
                        batting_stats_df['away_id'] = away_id
                        
                        game_logs = pd.concat([game_logs, batting_stats_df], ignore_index=True)                        
                        #print(f"Added stats for batter: {batter}")
                        
                        
                except Exception as e:
                    print(f"Error processing batter: {batter} - {e}")
            
            # Pitchers
            pitchers = boxscore_data['teams'][team]['pitchers']
            
            #print(f"Found {len(pitchers)} pitchers for team {team}")
            
            for pitcher in pitchers:
                player_key = f"ID{pitcher}"
                try:
                    pitching_stats = boxscore_data['teams'][team]['players'][player_key]['stats']['pitching']
                    if pitching_stats:
                        
                        #print(f"Pitching stats found for pitcher: {pitcher}") 
                        
                        pitching_stats_df = pd.DataFrame([pitching_stats])
                        
                        pitching_stats_df['stolenBasePercentage'] = pd.to_numeric(pitching_stats_df['stolenBasePercentage'], errors='coerce')
                        pitching_stats_df['strikePercentage'] = pd.to_numeric(pitching_stats_df['strikePercentage'], errors='coerce')
                        pitching_stats_df['runsScoredPer9'] = pd.to_numeric(pitching_stats_df['runsScoredPer9'], errors='coerce')
                        pitching_stats_df['homeRunsPer9'] = pd.to_numeric(pitching_stats_df['homeRunsPer9'], errors='coerce')

                        pitching_stats_df['player_id'] = pitcher
                        pitching_stats_df['game_pk'] = game_pk
                        pitching_stats_df['team'] = team
                        pitching_stats_df['sportId'] = sport_id
                        pitching_stats_df['role'] = 'pitcher'
                        pitching_stats_df['game_date'] = game_date
                        pitching_stats_df['game_year'] = pd.to_datetime(pitching_stats_df['game_date']).dt.year
                        pitching_stats_df['game_type'] = game_type
                        pitching_stats_df['home_id'] = home_id
                        pitching_stats_df['away_id'] = away_id
                        
                        # Convert inningsPitched to a numeric value
                        pitching_stats_df['inningsPitched'] = pitching_stats_df['inningsPitched'].apply(pd.to_numeric, errors='coerce')
                        pitching_stats_df['integer_part'] = np.floor(pitching_stats_df['inningsPitched'])
                        pitching_stats_df['inningsPitched'] = pitching_stats_df['integer_part'] + \
                                                                (pitching_stats_df['inningsPitched'] - pitching_stats_df['integer_part']) / 0.3
                        
                        # Add calculated fields (e.g., FIP, xFIP)
                        pitching_stats_df['k_percent'] = np.where(pitching_stats_df['battersFaced'] == 0, np.nan,
                                                                  pitching_stats_df['strikeOuts'] / pitching_stats_df['battersFaced'])
                        pitching_stats_df['bb_percent'] = np.where(pitching_stats_df['battersFaced'] == 0, np.nan,
                                                                  (pitching_stats_df['baseOnBalls'] + pitching_stats_df['intentionalWalks']) / pitching_stats_df['battersFaced'])
                        pitching_stats_df['WHIP'] = np.where(pitching_stats_df['inningsPitched'] == 0, np.nan,
                                                             (pitching_stats_df['hits'] + pitching_stats_df['baseOnBalls'] +
                                                              pitching_stats_df['intentionalWalks']) / pitching_stats_df['inningsPitched'])
                        pitching_stats_df['ERA'] = np.where(pitching_stats_df['inningsPitched'] == 0, np.nan,
                                                            pitching_stats_df['earnedRuns'] * 9 / pitching_stats_df['inningsPitched'])
                        
                        game_logs = pd.concat([game_logs, pitching_stats_df], ignore_index=True)
                        
                        game_logs['Opponent'] = np.where(
                            game_logs['team'] == 'home', 
                            game_logs['away_id'], 
                            game_logs['home_id']
                        )
                        
                        game_logs['team_id'] = np.where(
                            game_logs['team'] == 'away', 
                            game_logs['away_id'], 
                            game_logs['home_id']
                        )
                        
                        # Define the mapping of game_type values
                        game_type_mapping = {
                            'R': 'Regular Season',
                            'W': 'Playoffs',
                            'F': 'Playoffs',
                            'L': 'Playoffs',
                            'D': 'Playoffs',
                            'A': 'Regular Season',
                            'S': 'Spring Training'
                        }
                        
                        # Apply the mapping to the game_type column
                        game_logs['game_type'] = game_logs['game_type'].replace(game_type_mapping)
                                                
                        
                        
                    
                except Exception as e:
                    print(f"Error processing pitcher: {pitcher} - {e}")
    
    return game_logs

# Function to run fetch_game_logs_for_sport in parallel
def fetch_game_logs_parallel(start_date, end_date, sport_ids):
    all_game_logs = pd.DataFrame()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit tasks for each sportId
        future_to_sport = {
            executor.submit(fetch_game_logs_for_sport, sport_id, start_date, end_date): sport_id
            for sport_id in sport_ids
        }
        
        for future in concurrent.futures.as_completed(future_to_sport):
            sport_id = future_to_sport[future]
            try:
                sport_game_logs = future.result()
                all_game_logs = pd.concat([all_game_logs, sport_game_logs], ignore_index=True)
                print(f"Completed fetching data for sportId {sport_id}")
            except Exception as e:
                print(f"Error fetching data for sportId {sport_id}: {e}")
    
    return all_game_logs



# Main Execution
if __name__ == "__main__":

    # =============================================================================
    # # Define the start and end dates
    func_query = "SELECT MAX(game_date) AS max_date FROM Gamelogs"
    max_date_df = pd.read_sql(func_query, engine)
    
    # Extract the max date and convert it to a datetime object
    max_date = pd.to_datetime(max_date_df['max_date'][0])
    
    # Calculate start_date by adding 1 day to max_date
    start_date_input = max_date + timedelta(days=1)
    
    # Calculate end_date by adding 7 days to start_date
    end_date_input = pd.Timestamp.now() - pd.Timedelta(days=1)  #start_date_input  + timedelta(days=21)
    
    # Format the dates as strings
    start_ = start_date_input.strftime("%Y-%m-%d")
    end_ = end_date_input.strftime("%Y-%m-%d")
    
    # Convert end_ back to Timestamp for comparison
    end_timestamp = pd.to_datetime(end_)
    # =============================================================================

    
    #start_ = "2022-03-06"
    #end_ = "2022-03-26"
    sport_ids = [1, 11,12,13,14,16]
    
    # =============================================================================
    # # Fetch data
    
    if max_date >= end_timestamp:
        print("Gamelogs is up to date")
    
    else:
        print(f"Fetching game logs from {start_} to {end_} for sport_ids: {sport_ids}")  # Debug print

        game_logs = fetch_game_logs_parallel(start_, end_, sport_ids)
     
        # Insert data into database
        try:
            game_logs.to_sql('Gamelogs', con=engine, if_exists='append', index=False)
            print("Data successfully written to the database.")
        except Exception as e:
            print(f"Error writing to database: {e}")
        # =============================================================================
        
        # Calculate league-level metrics dynamically
        try:
            query = """
            SELECT game_year, 
                   SUM(homeRuns) AS hr,
                   SUM(intentionalWalks + baseOnBalls + hitByPitch) AS bb,
                   SUM(strikeOuts) AS k,
                   SUM(earnedRuns) * 9.0 / SUM(inningsPitched) AS era,
                   SUM(inningsPitched) AS ip,
                   SUM(homeRuns) * 1.0 / NULLIF(SUM(airOuts), 0) AS league_hr_per_fb
            FROM Gamelogs
            WHERE sportId = 1 
              AND role = 'pitcher' 
              AND game_type = 'Regular Season' 
              AND game_year = (SELECT MAX(game_year) FROM Gamelogs WHERE sportId = 1 AND role = 'pitcher' AND game_type = 'Regular Season')
            GROUP BY game_year;
            """
            current_year_metrics_update = pd.read_sql(query, engine)
            
            # Calculate FIP constant
            current_year_metrics_update['fip_const'] = current_year_metrics_update['era'] - \
                ((13 * current_year_metrics_update['hr']) + (3 * current_year_metrics_update['bb']) - \
                (2 * current_year_metrics_update['k'])) / current_year_metrics_update['ip']
                       
            # Get the values for league_hr_per_fb and fip_const
            league_hr_per_fb_value = float(current_year_metrics_update['league_hr_per_fb'][0])
            fip_const_value = float(current_year_metrics_update['fip_const'][0])
            
            mydb = pyodbc.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=Yemen;"  # Update with your server name
                "DATABASE=MLB App;"
                "Trusted_Connection=yes;"
            )
            
            mycursor = mydb.cursor()
            
            try:
                sql = """UPDATE Gamelogs
                SET league_hr_per_fb = ?,
                fip_const = ?
                WHERE game_year = (
                    SELECT MAX(game_year)
                    FROM Gamelogs
                    WHERE sportId = 1 
                    AND role = 'pitcher' 
                    AND game_type = 'Regular Season'
                );"""
                
                
                mycursor.execute(sql, (league_hr_per_fb_value, fip_const_value))
                
                mydb.commit()    
                    
                # Update FIP and xFIP query
                update_fip_query = """
                UPDATE Gamelogs
                SET fip = ((13.0 * homeRuns) + (3.0 * (baseOnBalls + intentionalWalks + hitByPitch)) - (2.0 * strikeOuts)) / NULLIF(inningsPitched, 0) + fip_const,
                    xfip = ((13.0 * (airOuts * league_hr_per_fb)) + (3.0 * (baseOnBalls + intentionalWalks + hitByPitch)) - (2.0 * strikeOuts)) / NULLIF(inningsPitched, 0) + fip_const
                WHERE game_year = (
                    SELECT MAX(game_year)
                    FROM Gamelogs
                );
                """
                
                mycursor.execute(update_fip_query)
                
                mydb.commit()
                print("FIP and xFIP updated.")
            
            except Exception as e:
                print(f"Error Updating FIP and xFIP: {e}")
                
            finally:
                mycursor.close()  # Ensure cursor is closed
                mydb.close()  # Ensure DB connection is closed
        
        except Exception as e:
            print(f"league metrics -- likely spring training: {e}")

        finally:
            engine.dispose()  # Ensure SQLAlchemy engine is properly disposed
        
        print("Database Closed.")