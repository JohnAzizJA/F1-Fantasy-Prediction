import pandas as pd
import fastf1 as f1
import requests
import os

f1.Cache.enable_cache('fastf1cache')

base_url = "https://api.jolpi.ca/ergast/f1"

def get_drivers(season):
    try:
        url = f"{base_url}/{season}/drivers.json"
        response = requests.get(url)
        data = response.json()
        
        drivers = data['MRData']['DriverTable']['Drivers']
        
        rows = []
        for driver in drivers:
            rows.append({
                "first_name": driver["givenName"],
                "last_name": driver["familyName"],
                "driver": driver["code"],
                "car_number": driver["permanentNumber"],
                "nationality": driver["nationality"]
            })
            
        return pd.DataFrame(rows)
    
    except Exception as e:
        print(f"Skipping drivers {season}: {e}")
        return None

def get_constructors(season):
    try:
        url = f"{base_url}/{season}/constructors.json"
        response = requests.get(url)
        data = response.json()
        
        constructors = data['MRData']['ConstructorTable']['Constructors']
        
        rows = []
        for constructor in constructors:
            rows.append({
                "constructor": constructor["name"],
                "nationality": constructor["nationality"]
            })
            
        return pd.DataFrame(rows)
    
    except Exception as e:
        print(f"Skipping constructors {season}: {e}")
        return None

def get_schedule_info(season):
    try:
        df = f1.get_event_schedule(season)
        
        df = df[['RoundNumber', 'EventName', 'Country', 'Location', 'EventDate', 'EventFormat']]
        df.rename(columns={
            'RoundNumber': 'round',
            'EventName': 'event',
            'Country': 'country',
            'Location': 'location',
            'EventDate': 'date',
            'EventFormat': 'format'
        }, inplace=True)
        
        df['season'] = season
        
        return df
    
    except Exception as e:
        print(f"Skipping schedule info {season}: {e}")
        return None

def get_session_results(season, event, session_type):
    try:
        session = f1.get_session(season, event, session_type)
        session.load()
        df = session.results.copy()
        
        if session_type in ['R', 'S']:
            df = df[['DriverNumber', 'Abbreviation', 'TeamName', 'Position', 'GridPosition', 'Status']]
            df.rename(columns={
                'DriverNumber': 'car_number',
                'Abbreviation': 'driver',
                'TeamName': 'constructor',
                'Position': 'finish_pos',
                'GridPosition': 'grid_pos',
                'Status': 'status'
            }, inplace=True)
            
        elif session_type in ['Q', 'SQ', 'SS']:
            df = df[['DriverNumber', 'Abbreviation', 'TeamName', 'Q1', 'Q2', 'Q3', 'Position']]
            df.rename(columns={
                'DriverNumber': 'car_number',
                'Abbreviation': 'driver',
                'TeamName': 'constructor',
                'Position': 'finish_pos'
            }, inplace=True)
            
        df['season'] = season
        df['event'] = event
        df['session_type'] = session_type
        
        return df
            
    except Exception as e:
        print(f"Skipping {season} {event} {session_type}: {e}")
        return None

def get_driver_standings(season):
    try:
        url = f"{base_url}/{season}/driverstandings.json"
        response = requests.get(url)
        data = response.json()
        
        standings = data['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings']
        
        rows = []
        for driver in standings:
            rows.append({
                "season": season,
                "driver": driver["Driver"]["code"],
                "car_number": driver["Driver"]["permanentNumber"],
                "constructor": driver["Constructors"][0]["name"],
                "position": driver["position"],
                "points": driver["points"],
                "wins": driver["wins"],
            })
        
        return pd.DataFrame(rows)
    
    except Exception as e:
        print(f"Skipping driver standings {season}: {e}")
        return None

def get_constructor_standings(season):
    try:
        url = f"{base_url}/{season}/constructorstandings.json"
        response = requests.get(url)
        data = response.json()
        
        standings = data['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings']
        
        rows = []
        for constructor in standings:
            rows.append({
                "season": season,
                "constructor": constructor["Constructor"]["name"],
                "position": constructor["position"],
                "points": constructor["points"],
                "wins": constructor["wins"],
            })
            
        return pd.DataFrame(rows)
    
    except Exception as e:
        print(f"Skipping constructor standings {season}: {e}")
        return None

def get_laptimes(season, event):
    try:
        session = f1.get_session(season, event, 'R')
        session.load()
        df = session.laps.copy()
        
        df = df[['Driver', 'DriverNumber', 'Team', 'LapNumber', 'LapTime', 'Position', 'Sector1Time', 'Sector2Time', 'Sector3Time', 'Stint', 'Compound']]
        df.rename(columns={
            'Driver': 'driver',
            'DriverNumber': 'car_number',
            'Team': 'constructor',
            'LapNumber': 'lap_number',
            'LapTime': 'lap_time',
            'Position': 'position',
            'Sector1Time': 'sector1',
            'Sector2Time': 'sector2',
            'Sector3Time': 'sector3',
            'Stint': 'stint',
            'Compound': 'tyre_compound'
        }, inplace=True)
        
        df['season'] = season
        df['event'] = event
        df['session_type'] = 'R'
        
        return df
    
    except Exception as e:
        print(f"Skipping lap times {season} {event} {session}: {e}")
        return None
    
def get_pitstops(season):
    try:
        race_count = f1.get_event_schedule(season).RoundNumber.max()
        
        stops = []
        
        for rnd in range(1, race_count + 1):
            url = f"{base_url}/{season}/{rnd}/pitstops.json"
            response = requests.get(url)
            data = response.json()

            races = data['MRData']['RaceTable']['Races']
            
            if not races:
                continue
            
            race = races[0]
            
            for stop in race.get('PitStops', []):
                stops.append({
                    "season": season,
                    "race": race['raceName'],
                    "driver": stop["driverId"],
                    "stop": stop["stop"],
                    "lap_number": stop["lap"],
                    "duration": stop["duration"],
                })
        
        return pd.DataFrame(stops)
    
    except Exception as e:
        print(f"Skipping pitstops {season}: {e}")
        return None

def get_weather_data(season, event, session_type):
    try:
        session = f1.get_session(season, event, session_type)
        session.load()
        
        df = session.weather_data.copy()
        
        df.rename(columns={
            'Time': 'time',
            'AirTemp': 'air_temp_c',
            'TrackTemp': 'track_temp_c',
            'Humidity': 'humidity_pct',
            'Pressure': 'pressure_mbar',
            'Rainfall': 'rainfall',
            'WindSpeed': 'wind_speed_kph',
            'WindDirection': 'wind_dir_deg'
        }, inplace=True)
        
        df['season'] = season
        df['event'] = event
        df['session_type'] = session_type
        
        return df
        
    except Exception as e:
        print(f"Skipping weather {season} {event} {session_type}: {e}")
        return None
    
def get_race_events(season, event):
    try:
        session = f1.get_session(season, event, 'R')
        session.load()
        
        df = session.race_control_messages.copy()
        df = df[['Time', 'Category', 'Message', 'Flag', 'Scope', 'Lap']]
        
        df.rename(columns = {
            'Time': 'time',
            'Category': 'category',
            'Message': 'message',
            'Flag': 'flag',
            'Scope': 'scope',
            'Lap': 'lap_number'
        }, inplace=True)
        
        df['season'] = season
        df['event'] = event
        df['session_type'] = 'R'

        return df
    
    except Exception as e:
        print(f"Skipping race events {season} {event}: {e}")
        return None

def collect_data(seasons, base_dir = "data/raw"):
    for season in seasons:
        season_dir = os.path.join(base_dir, str(season))
        os.makedirs(season_dir, exist_ok=True)
        
        # SEASON-LEVEL DATA (Drivers, Constructors, Schedule, Standings, Pit Stops)
        
        df = get_drivers(season)
        df.to_csv(os.path.join(season_dir, "drivers.csv"), index=False)
        
        df = get_constructors(season)
        df.to_csv(os.path.join(season_dir, "constructors.csv"), index=False)
        
        df = get_schedule_info(season)
        df.to_csv(os.path.join(season_dir, "schedule.csv"), index=False)
        
        df = get_driver_standings(season)
        df.to_csv(os.path.join(season_dir, "driver_standings.csv"), index=False)
        
        df = get_constructor_standings(season)
        df.to_csv(os.path.join(season_dir, "constructor_standings.csv"), index=False)
        
        df = get_pitstops(season)
        df.to_csv(os.path.join(season_dir, "pitstops.csv"), index=False)
        
        # EVENT-LEVEL DATA (Results, Lap Times, Weather, Race Events)
        
        schedule = f1.get_event_schedule(season)
        
        laptimes = []
        race_events = []
        weather_data = []
        quali_results = []
        sprint_results = []
        race_results = []
        
        for _, event in schedule.iterrows():
            event_name = event['EventName']
            event_format = event['EventFormat']
            
            df = get_laptimes(season, event_name)
            laptimes.append(df)
            
            df = get_race_events(season, event_name)
            race_events.append(df)
            
            if event_format == 'sprint_shootout':
                session_types = ['SS', 'S', 'Q', 'R']
            elif event_format == 'sprint_qualifying':
                session_types = ['SQ', 'S', 'Q', 'R']
            elif event_format == 'conventional':
                session_types = ['Q', 'R']
                
            for session_type in session_types:
                df = get_session_results(season, event_name, session_type)
                if df is not None:
                    if session_type in ['Q', 'SQ', 'SS']:
                        quali_results.append(df)
                    elif session_type == 'S':
                        sprint_results.append(df)
                    elif session_type == 'R':
                        race_results.append(df)
                        
                df = get_weather_data(season, event_name, session_type)
                weather_data.append(df)
                
        pd.concat(laptimes).to_csv(os.path.join(season_dir, "laptimes.csv"), index=False)
        pd.concat(race_events).to_csv(os.path.join(season_dir, "race_events.csv"), index=False)
        pd.concat(weather_data).to_csv(os.path.join(season_dir, "weather.csv"), index=False)
        pd.concat(quali_results).to_csv(os.path.join(season_dir, "quali_results.csv"), index=False)
        pd.concat(sprint_results).to_csv(os.path.join(season_dir, "sprint_results.csv"), index=False)
        pd.concat(race_results).to_csv(os.path.join(season_dir, "race.csv"), index=False)