import pandas as pd
import numpy as np
import fastf1 as f1
import requests

f1.Cache.enable_cache('fastf1cache')

base_url = "https://api.jolpi.ca/f1"

def get_results(season, event, session):
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

