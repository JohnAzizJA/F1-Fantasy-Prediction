import pandas as pd
import numpy as np
import os

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def load_inspect(filename, season):
    path = os.path.join(RAW_DIR, str(season), filename)
    df = pd.read_csv(path)
    
    print(f"INSPECTING DATA FROM: {filename}\n")
    
    print(f"Total Rows: {len(df)}\n")
    print(f"Total Columns: {len(df.columns)}\n")
    print(f"Columns: {df.columns.tolist()}\n")
    print(f"First few rows: -\n")
    print(df.head())
    
    print(f"\nData Types: -\n")
    print(df.dtypes)
    
    print(f"\nMissing Values:\n")
    print(f"Columns with missing values: {df.isnull().any().sum()}\n")
    print(f"Total missing values: {df.isnull().sum().sum()}\n")
    
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
    
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['round'] = pd.to_numeric(df['round'], errors='coerce')
    df['race_laps'] = pd.to_numeric(df['race_laps'], errors='coerce')
    df['event'] = df['event'].str.strip()
    df['country'] = df['country'].str.strip()
    df['location'] = df['location'].str.strip()
    df['format'] = df['format'].str.strip()
    
    return df
    
def clean_results(df, type):
    if df is None or df.empty:
        print(f"No {type} results data to clean")
        return None
    
    df = standardize_constructor_names(df, 'constructor')
    df = standardize_driver_names(df, 'driver')
    
    # Handle position status
    df['finish_pos_numeric'] = pd.to_numeric(df['finish_pos'], errors='coerce')
    df['did_not_finish'] = df['finish_pos'].isin(['DNF', 'DNS', 'DSQ', 'Retired'])
    df['lapped'] = df['finish_pos'].str.contains('Lapped', na=False)
    df['finished'] = df['finish_pos'].str.contains('Finished', na=False)
    
    df['event'] = df['event'].str.strip()
    df['car_number'] = pd.to_numeric(df['car_number'], errors='coerce')
    
    if type in ['R', 'S']:
        # Handle grid positions
        df['grid_pos'] = pd.to_numeric(df['grid_pos'], errors='coerce')
        df['pit_lane_start'] = df['grid_pos'] <= 0
        df['grid_pos'] = df['grid_pos'].clip(lower=1)
        df['status'] = df['status'].str.strip()
    elif type in ['Q', 'SQ', 'SS']:
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
    
    # Handle time/duration values
    for col in ['lap_time', 'sector1', 'sector2', 'sector3']:
        if col in df.columns:
            df[f'{col}_seconds'] = pd.to_timedelta(df[col], errors='coerce').dt.total_seconds()
            # Remove invalid times (negative, zero, or unrealistic)
            df[f'{col}_valid'] = (df[f'{col}_seconds'] > 0) & (df[f'{col}_seconds'] < 300)
            df.loc[~df[f'{col}_valid'], f'{col}_seconds'] = np.nan
            
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['stint'] = pd.to_numeric(df['stint'], errors='coerce')
    
    if 'tyre_compound' in df.columns:
        df['tyre_compound'] = df['tyre_compound'].str.upper().str.strip()
        
    return df

def clean_pitstops(df):
    if df is None or df.empty:
        print("No pit stops data to clean")
        return None
    
    df = standardize_driver_names(df, 'driver')
    df['stop'] = pd.to_numeric(df['stop'], errors='coerce')
    df['lap_number'] = pd.to_numeric(df['lap_number'], errors='coerce')
    
    # Handle pit stop duration outliers
    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')
    df['duration_valid'] = (df['duration'] >= 1) & (df['duration'] <= 60)
    df.loc[~df['duration_valid'], 'duration'] = np.nan
    
    df['race'] = df['race'].str.strip()
    
    return df
    
def clean_weather_data(df):
    if df is None or df.empty:
        print("No weather data to clean")
        return None

    df['time'] = pd.to_timedelta(df['time'], errors='coerce')
    
    # Handle weather data with realistic ranges
    if 'air_temp_c' in df.columns:
        df['air_temp_c'] = pd.to_numeric(df['air_temp_c'], errors='coerce')
        df['air_temp_c'] = df['air_temp_c'].clip(-50, 80)
    
    if 'track_temp_c' in df.columns:
        df['track_temp_c'] = pd.to_numeric(df['track_temp_c'], errors='coerce')
        df['track_temp_c'] = df['track_temp_c'].clip(-50, 100)
    
    if 'humidity_pct' in df.columns:
        df['humidity_pct'] = pd.to_numeric(df['humidity_pct'], errors='coerce')
        df['humidity_pct'] = df['humidity_pct'].clip(0, 100)
    
    if 'wind_dir_deg' in df.columns:
        df['wind_dir_deg'] = pd.to_numeric(df['wind_dir_deg'], errors='coerce')
        df['wind_dir_deg'] = df['wind_dir_deg'] % 360
    
    for col in ['pressure_mbar', 'wind_speed_kph']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df['rainfall'] = df['rainfall'].astype(bool)
    
    return df
    
def clean_race_events(df):
    if df is None or df.empty:
        print("No race events data to clean")
        return None
    
    df['time'] = pd.to_timedelta(df['time'], errors='coerce')
    df['lap_number'] = pd.to_numeric(df['lap_number'], errors='coerce')
    df['category'] = df['category'].str.strip()
    df['flag'] = df['flag'].str.strip()
    df['scope'] = df['scope'].str.strip()
    
    return df

def handle_missing_values(df):
    pass