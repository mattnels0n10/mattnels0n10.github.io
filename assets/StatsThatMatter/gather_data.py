import time
from datetime import datetime, timedelta
import io
import pandas as pd
import requests
from sqlalchemy import create_engine

# Create a connection string for SQL Server
# Adjust the driver and server details according to your setup
connection_string = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=YEMEN;"  # Update with your server name
    "DATABASE=MLB App;"
    "Trusted_Connection=yes;"
)

# Create an SQLAlchemy engine
engine = create_engine(connection_string)

# Constants for the script
RETRIES = 5
DELAY = 120  # Delay in seconds between retries

#Helper Function for Retries
def get_with_retries(url):
    attempt = 0
    while attempt < RETRIES:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an error for 4XX/5XX responses
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error occurred: {e}")
            attempt += 1
            print(f"Retrying... Attempt {attempt} of {RETRIES}")
            time.sleep(DELAY)
    return None

def main():

    # Get the current date and time
    current_datetime = datetime.now() - timedelta(days=1)
    current_date_str = current_datetime.strftime('%Y-%m-%d')
    
    # Run the SQL query to find the maximum MLB game_date
# =============================================================================
    major_query = "SELECT MAX(game_date) AS max_date FROM MajorLeagueRawData;"
    major_date_df = pd.read_sql(major_query, engine)
    major_game_date = major_date_df['max_date'].iloc[0]
    major_game_date_str = major_game_date.strftime('%Y-%m-%d')
# =============================================================================
    
    # Run the SQL query to find the maximum Minors game_date
# =============================================================================
    minor_query = "SELECT MAX(game_date) AS max_date FROM MinorLeagueRawData;"
    minor_date_df = pd.read_sql(minor_query, engine)
    minor_game_date = minor_date_df['max_date'].iloc[0]
    minor_game_date_str = minor_game_date.strftime('%Y-%m-%d')
# =============================================================================

    
    #Create Yesterday's Date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d') 

    
# =============================================================================
#     select_na_remove=['release_speed','release_pos_x','release_pos_z','batter',
#           'pitcher','description','game_type','stand','p_throws','balls',
#           'strikes','pfx_x','pfx_z','plate_x','plate_z','sz_top','sz_bot','release_spin_rate','release_extension',
#           'release_pos_y','spin_axis','vx0','vy0','vz0','ax','ay','az']
# =============================================================================
    
    field_names=['pitch_type','game_date','release_speed','release_pos_x','release_pos_z','player_name','batter',
              'pitcher','description','game_type','stand','p_throws','home_team','away_team','hit_location','balls',
              'strikes','game_year','pfx_x','pfx_z','plate_x','plate_z','inning','hc_x','hc_y','sz_top','sz_bot',
              'launch_speed','launch_angle','effective_speed','release_spin_rate','release_extension','game_pk',
              'release_pos_y','estimated_ba_using_speedangle','estimated_woba_using_speedangle','woba_value',
              'babip_value','at_bat_number','pitch_number','pitch_name','home_score','away_score','spin_axis',
              'delta_home_win_exp','delta_run_exp','vx0','vy0','vz0','ax','ay','az','bat_speed','swing_length','outs_when_up',
              'on_1b','on_2b','on_3b','inning_topbot','arm_angle','n_thruorder_pitcher', 'n_priorpa_thisgame_player_at_bat',
              'pitcher_days_since_prev_game','batter_days_since_prev_game',	'pitcher_days_until_next_game'
              ]
    
   # batter_fields=['game_date','player_name','inning_topbot', 'home_team','away_team']
    
    
    def statcast_retrieve(start_date, end_date):
        if major_game_date_str == current_date_str:

            print('Major League Database Up to Date')
        
        else:
            current_date = datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=1)
            finish =  datetime.strptime(end_date, '%Y-%m-%d').date()

            
            # Initialize an empty DataFrame to store all the data
            major_league_statcast_final = pd.DataFrame()
            
            while current_date <= finish:
        
                remaining_days = (finish - current_date).days
        
                if remaining_days < 5:
        
                    day_iterate = current_date + timedelta(days = remaining_days)
        
                else:
        
                    day_iterate = current_date + timedelta(days = 5)
        
                url=f"https://baseballsavant.mlb.com/statcast_search/csv?hfPT=&hfAB=&hfGT=&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2025%7C2024%7C2023%7C2022%7C2021%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={current_date.strftime('%Y-%m-%d')}&game_date_lt={day_iterate.strftime('%Y-%m-%d')}&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&type=details&all=true&minors=false"
        
                #use requests to pull HTTP information
                response = get_with_retries(url)
        
                #read into a dataframe
                df = pd.read_csv(io.StringIO(response.text),on_bad_lines='skip')
                
        
                if not df.empty:
                    print(f'Majors {df.iloc[2,1]}')          
                
                    major_league_statcast_final = df
                    
                    major_league_statcast_final = major_league_statcast_final[field_names] 
                    
                    #major_league_statcast_final = major_league_statcast_final.dropna(subset=select_na_remove)
                    # Ensure you're working on a copy to avoid warnings
                    major_league_statcast_final = major_league_statcast_final.copy()
                    
                    #Add Team Name Abbrev
                    major_league_statcast_final['Team'] = major_league_statcast_final.apply(lambda row: row['away_team'] if row['inning_topbot'] == 'Bot' else row['home_team'], axis=1)
                    
                    #pitchers first name
                    major_league_statcast_final['first_name'] = major_league_statcast_final['player_name'].str.split(',').str[1].str.strip()
                    
                    #pitchers last name
                    major_league_statcast_final['last_name'] = major_league_statcast_final['player_name'].str.split(',').str[0].str.strip()
                    major_league_statcast_final['player_name'] = major_league_statcast_final['first_name'] + ' ' + major_league_statcast_final['last_name']
                    major_league_statcast_final = major_league_statcast_final[major_league_statcast_final['outs_when_up'].isin([0, 1, 2])]
                    
                    
                    #drop first and last name columns                   
                    major_league_statcast_final = major_league_statcast_final.drop(['first_name','last_name','inning_topbot'], axis=1)
                    
                    try:
                        major_league_statcast_final.to_sql('MajorLeagueRawData', con=engine, if_exists='append', index=False)
                        print("Data uploaded successfully.")
                    except Exception as e:
                        print(f"Error uploading data: {e}")
                 
                current_date += timedelta(days = 6)
                    
        return None
    
    statcast_retrieve(major_game_date_str, yesterday_str)
    #statcast_retrieve(major_game_date_str,yesterday_str)
    
    #Gather Minor League Data ##
    
    def statcast_minor_league_retrieve(start_date, end_date):
        
        if minor_game_date_str == current_date_str:
            print('Minor League Database Up to Date')
        
        else:
        
            current_date = datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=1)
            finish =  datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # Initialize an empty DataFrame to store all the data
            minor_league_statcast_final = pd.DataFrame()
        
            while current_date <= finish:
        
                remaining_days = (finish - current_date).days
        
                if remaining_days < 5:

                    day_iterate = current_date + timedelta(days = remaining_days)
        
                else:
        
                    day_iterate = current_date + timedelta(days = 5)
                
                url=f"https://baseballsavant.mlb.com/statcast-search-minors/csv?hfPT=&hfAB=&hfGT=R%7CS%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2025%7C2024%7C2023%7C2022%7C2021%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={current_date.strftime('%Y-%m-%d')}&game_date_lt={day_iterate.strftime('%Y-%m-%d')}&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInn=&hfBBT=&hfFlag=is%5C.%5C.tracked%7C&hfLevel=&metric_1=&hfTeamAffiliate=&hfOpponentAffiliate=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&chk_is..tracked=on&type=details&all=true&minors=true"
        
                #use requests to pull HTTP information
                response = get_with_retries(url)
        
                #read into a dataframe
                df = pd.read_csv(io.StringIO(response.text),on_bad_lines='skip')
        
                if not df.empty:
                    print(f'Minors {df.iloc[2,1]}')           
                
                minor_league_statcast_final = df
            
                minor_league_statcast_final = minor_league_statcast_final[field_names] 
                
                #minor_league_statcast_final = minor_league_statcast_final.dropna(subset=select_na_remove)
                
                # Ensure you're working on a copy to avoid warnings
                minor_league_statcast_final = minor_league_statcast_final.copy()
                
                minor_league_statcast_final['home_team'] = minor_league_statcast_final['home_team'].replace('COL', 'CLMB')
                minor_league_statcast_final['away_team'] = minor_league_statcast_final['away_team'].replace('COL', 'CLMB')
                
                #Add Team Name Abbrev
                minor_league_statcast_final['Team'] = minor_league_statcast_final.apply(lambda row: row['away_team'] if row['inning_topbot'] == 'Bot' else row['home_team'], axis=1)
                
                #pitchers first name
                minor_league_statcast_final['first_name'] = minor_league_statcast_final['player_name'].str.split(',').str[1].str.strip()
                
                #pitchers last name
                minor_league_statcast_final['last_name'] = minor_league_statcast_final['player_name'].str.split(',').str[0].str.strip()
                minor_league_statcast_final['player_name'] = minor_league_statcast_final['first_name'] + ' ' + minor_league_statcast_final['last_name']
                minor_league_statcast_final = minor_league_statcast_final[minor_league_statcast_final['outs_when_up'].isin([0, 1, 2])]

                
                #drop first and last name columns
                minor_league_statcast_final = minor_league_statcast_final.drop(['first_name','last_name','inning_topbot'], axis=1)
                
              
                # Write the DataFrame to SQL Server
                try:
                    minor_league_statcast_final.to_sql('MinorLeagueRawData', con=engine, if_exists='append', index=False)
                    print("Data uploaded successfully.")
                except Exception as e:
                    print(f"Error uploading data: {e}")
                    
                current_date += timedelta(days = 6)
                
        return None
    
    statcast_minor_league_retrieve(minor_game_date_str, yesterday_str)

# Retry mechanism
attempt = 0
while attempt < RETRIES:
    try:
        main()  # Attempt to run the main function
        break  # Exit the loop if successful
    except Exception as e:
        print(f"Error occurred: {e}")
        attempt += 1
        print(f"Retrying... Attempt {attempt} of {RETRIES}")
        time.sleep(DELAY)
else:
    print("Failed to complete script after multiple attempts.")
    
engine.dispose()
print("Database connection closed.")
