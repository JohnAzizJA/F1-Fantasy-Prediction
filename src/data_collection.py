import pandas as pd
import numpy as np
import fastf1 as f1
import requests
import os

f1.Cache.enable_cache('fastf1cache')

base_url = "https://api.jolpi.ca/ergast/f1"

def get_drivers(season):
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

def get_constructors(season):
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

def get_schedule_info(season):
    try:
        schedule = f1.get_event_schedule(season)
        rows = []
        
        for _, event in schedule.iterrows():
                session = f1.get_session(season, event['EventName'], 'R')
                session.load()
                
                rows.append({
                    "season": season,
                    "round": event['RoundNumber'],
                    "event": event['EventName'],
                    "country": event['Country'],
                    "location": event['Location'],
                    "date": event['EventDate'],
                    "format": event['EventFormat'],
                    "race_laps": session.total_laps,
                })
                
        return pd.DataFrame(rows)
    
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
            
        elif session_type in ['Q', 'SQ']:
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

def get_constructor_standings(season):
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

def get_laptimes(season, event, session_type):
    try:
        session = f1.get_session(season, event, session_type)
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
        df['session_type'] = session
        
        return df
    
    except Exception as e:
        print(f"Skipping lap times {season} {event} {session}: {e}")
        return None
    
def get_pitstops(season):
    race_count = f1.get_event_schedule(season).RoundNumber.max()
    
    stops = []
    
    for rnd in range(1, race_count + 1):
        url = f"{base_url}/{season}/{rnd}/pitstops.json"
        response = requests.get(url)
        data = response.json()

        race = data['MRData']['RaceTable']['Races'][0]
        
        for stop in race.get('PitStops', []):
            stops.append({
                "season": season,
                "race": race['raceName'],
                "driver": stop["driverId"],
                "stop": int(stop["stop"]),
                "lap": int(stop["lap"]),
                "duration": float(stop["duration"])
            })
    
    return pd.DataFrame(stops)

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
        
        df.rename(columns = {
            'Time': 'time',
            'Category': 'category',
            'Message': 'message',
            'Flag': 'flag',
            'Scope': 'scope',
            'Lap': 'lap'
        }, inplace=True)
        
        df['season'] = season
        df['event'] = event
        df['session_type'] = 'R'

        return df
    
    except Exception as e:
        print(f"Skipping race events {season} {event}: {e}")
        return None