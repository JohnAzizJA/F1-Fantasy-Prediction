import pandas as pd
import numpy as np
import os

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def load_inspect(filename, season):
    path = os.path.join(RAW_DIR, str(season), filename)
    df = pd.read_csv(path)
    
    print(f"INSPECTING DATA FROM: {filename}\n")
    
    print(f"Dataset Shape: {df.shape}\n")
    print(f"Columns: {df.columns.tolist()}\n")
    print(f"First few rows:\n")
    print(df.head())
    
    print(f"\nData Types:\n")
    print(df.dtypes)
    
    print(f"\nMissing Values:\n")
    print(df.isnull().sum())
    
    return df

def map_driver_constructor():
    # TODO: Map drivers to constructors
    pass

def standardize_driver_names(df, driver_col = 'driver'):
    if driver_col not in df.columns:
        print(f"Warning: Column '{driver_col}' not found")
        return df
    
    df[driver_col] = df[driver_col].str.upper().str.strip()
    
    corrections = {
        # TODO: Add actual driver name corrections here
    }
    
    for code, variations in corrections.items():
        for var in variations:
            df[driver_col] = df[driver_col].replace(var, code)
            
    return df

def standardize_constructor_names(df, constructor_col = 'constructor'):
    if constructor_col not in df.columns:
        print(f"Warning: Column '{constructor_col}' not found")
        return df
    
    df[constructor_col] = df[constructor_col].str.upper().str.strip()
    
    corrections = {
        # TODO: Add actual constructor name corrections here
    }
    
    for code, variations in corrections.items():
        for var in variations:
            df[constructor_col] = df[constructor_col].replace(var, code)
            
    return df

def clean_standings(df, type):
    if df is None or df.empty:
        print(f"No {type} standings data to clean")
        return None
    
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['points'] = pd.to_numeric(df['points'], errors='coerce')
    df['wins'] = pd.to_numeric(df['wins'], errors='coerce')
    
    if type == 'driver':
        df = standardize_driver_names(df, 'driver')
    elif type == 'constructor':
        df = standardize_constructor_names(df, 'constructor')
        
    return df

def clean_schedule_info(df):
    if df is None or df.empty:
        print("No schedule data to clean")
        return None
    
    # TODO: Clean schedule information
    
def clean_results(df, type):
    if df is None or df.empty:
        print(f"No {type} results data to clean")
        return None
    
    df = standardize_constructor_names(df, 'constructor')
    df = standardize_driver_names(df, 'driver')
    
    df['finish_pos'] = pd.to_numeric(df['finish_pos'], errors='coerce')
    df['event'] = df['event'].str.strip()
    df['car_number'] = pd.to_numeric(df['car_number'], errors='coerce')
    
    if type in ['R', 'S']:
        df['grid_pos'] = pd.to_numeric(df['grid_pos'], errors='coerce')
        df['Status'] = df['Status'].str.strip()
    elif type in ['Q', 'SQ', 'SS']:
        df['finish_pos'] = pd.to_numeric(df['finish_pos'], errors='coerce')
        for q_col in ['Q1', 'Q2', 'Q3']:
            if q_col in df.columns:
                df[f'{q_col}_seconds'] = pd.to_timedelta(df[q_col], errors='coerce').dt.total_seconds()
                df = df.drop(columns=[q_col])
                
    return df
    
def clean_laptimes(df):
    if df is None or df.empty:
        print("No lap times data to clean")
        return None

    df = standardize_constructor_names(df, 'constructor')
    df = standardize_driver_names(df, 'driver')
    
    df['car_number'] = pd.to_numeric(df['car_number'], errors='coerce')
    df['lap_number'] = pd.to_numeric(df['lap_number'], errors='coerce')
    
    for col in ['lap_time', 'sector1', 'sector2', 'sector3']:
        if col in df.columns:
            df[f'{col}_seconds'] = pd.to_timedelta(df[col], errors='coerce').dt.total_seconds()
            
        df['position'] = pd.to_numeric(df['position'], errors='coerce')
        df['stint'] = pd.to_numeric(df['stint'], errors='coerce')
    
    if 'tyre_compound' in df.columns:
        df['tyre_compound'] = df['tyre_compound'].str.upper().str.strip()
        
    if 'lap_time_seconds' in df.columns:
        outliers = df['lap_time_seconds'] > 200
        print(f"\nRemoving {outliers.sum()} outlier laps (>200s)")
        df = df[~outliers]
        
    return df

def clean_pitstops(df):
    if df is None or df.empty:
        print("No pit stops data to clean")
        return None
    
    df = standardize_driver_names(df, 'driver')
    df['stop'] = pd.to_numeric(df['stop'], errors='coerce')
    df['lap_number'] = pd.to_numeric(df['lap_number'], errors='coerce')
    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')
    df['race'] = df['race'].str.strip()
    
    return df
    
def clean_weather_data(df):
    if df is None or df.empty:
        print("No weather data to clean")
        return None

    df['time'] = pd.to_timedelta(df['time'], errors='coerce')
    
    weather_cols = ['air_temp_c', 'track_temp_c', 'humidity_pct', 'pressure_mbar', 'wind_speed_kph', 'wind_dir_deg']
    
    for col in weather_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df['rainfall'] = df['rainfall'].astype(bool)
    
    return df
    
def clean_race_events(df):
    if df is None or df.empty:
        print("No race events data to clean")
        return None
    
    # TODO: Clean race events