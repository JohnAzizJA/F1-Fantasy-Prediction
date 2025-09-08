import pandas as pd
import numpy as np
import fastf1 as f1
import requests

f1.Cache.enable_cache('fastf1cache')

base_url = "https://api.jolpi.ca/ergast/f1"

def get_session_results(season, event, session):
    try:
        session = f1.get_session(season, event, session)
        session.load()
        df = session.results.copy()
        
        if session == 'R' or session == 'S':
            df = df[['DriverNumber', 'Abbreviation', 'TeamName', 'Position', 'GridPosition', 'Status']]
            df.rename(columns={
                'DriverNumber': 'car_number',
                'Abbreviation': 'driver',
                'TeamName': 'constructor',
                'Position': 'finish_pos',
                'GridPosition': 'grid_pos',
                'Status': 'status'
            }, inplace=True)
            
        elif session == 'Q' or session == 'SQ':
            df = df[['DriverNumber', 'Abbreviation', 'TeamName', 'Q1', 'Q2', 'Q3', 'Position']]
            df.rename(columns={
                'DriverNumber': 'car_number',
                'Abbreviation': 'driver',
                'TeamName': 'constructor',
                'Position': 'finish_pos'
            }, inplace=True)
            
        df['season'] = season
        df['event'] = event
        df['session_type'] = session
        
        return df
            
    except Exception as e:
        print(f"Skipping {season} {event} {session}: {e}")
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
            "driver": driver["code"],
            "car_number": driver["permanentNumber"],
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
            "constructor": constructor["name"],
            "position": constructor["position"],
            "points": constructor["points"],
            "wins": constructor["wins"],
        })
        
    return pd.DataFrame(rows)