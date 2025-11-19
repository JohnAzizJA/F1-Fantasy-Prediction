import pandas as pd
import numpy as np
import os

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

DRIVER_CORRECTIONS = {
    'VER': ['VER', 'VERSTAPPEN', 'MAX VERSTAPPEN'],
    'PER': ['PER', 'PEREZ', 'SERGIO PEREZ'],
    'HAM': ['HAM', 'HAMILTON', 'LEWIS HAMILTON'],
    'RUS': ['RUS', 'RUSSELL', 'GEORGE RUSSELL'],
    'LEC': ['LEC', 'LECLERC', 'CHARLES LECLERC'],
    'SAI': ['SAI', 'SAINZ', 'CARLOS SAINZ'],
    'NOR': ['NOR', 'NORRIS', 'LANDO NORRIS'],
    'PIA': ['PIA', 'PIASTRI', 'OSCAR PIASTRI'],
    'ALO': ['ALO', 'ALONSO', 'FERNANDO ALONSO'],
    'STR': ['STR', 'STROLL', 'LANCE STROLL'],
    'OCO': ['OCO', 'OCON', 'ESTEBAN OCON'],
    'GAS': ['GAS', 'GASLY', 'PIERRE GASLY'],
    'TSU': ['TSU', 'TSUNODA', 'YUKI TSUNODA'],
    'RIC': ['RIC', 'RICCIARDO', 'DANIEL RICCIARDO'],
    'HUL': ['HUL', 'HULKENBERG', 'NICO HULKENBERG'],
    'MAG': ['MAG', 'MAGNUSSEN', 'KEVIN MAGNUSSEN'],
    'ALB': ['ALB', 'ALBON', 'ALEXANDER ALBON'],
    'SAR': ['SAR', 'SARGEANT', 'LOGAN SARGEANT'],
    'BOT': ['BOT', 'BOTTAS', 'VALTTERI BOTTAS'],
    'ZHO': ['ZHO', 'ZHOU', 'ZHOU GUANYU'],
    'LAW': ['LAW', 'LAWSON', 'LIAM LAWSON'],
    'BEA': ['BEA', 'BEARMAN', 'OLIVER BEARMAN'],
    'COL': ['COL', 'COLAPINTO', 'FRANCO COLAPINTO'],
    'VET': ['VET', 'VETTEL', 'SEBASTIAN VETTEL'],
    'LAT': ['LAT', 'LATIFI', 'NICHOLAS LATIFI'],
    'MSC': ['MSC', 'SCHUMACHER', 'MICK SCHUMACHER'],
    'DEV': ['DEV', 'DE VRIES', 'NYCK DE VRIES'],
}

CONSTRUCTOR_CORRECTIONS = {
    'RED BULL': [
        'RED BULL RACING',
        'RED BULL',
        'ORACLE RED BULL RACING',
        'RED BULL RACING HONDA RBPT',
    ],
    'MERCEDES': [
        'MERCEDES',
        'MERCEDES-AMG PETRONAS F1 TEAM',
        'MERCEDES-AMG PETRONAS',
    ],
    'FERRARI': [
        'FERRARI',
        'SCUDERIA FERRARI',
        'SCUDERIA FERRARI HP',
    ],
    'MCLAREN': [
        'MCLAREN',
        'MCLAREN F1 TEAM',
        'MCLAREN RACING',
    ],
    'ASTON MARTIN': [
        'ASTON MARTIN',
        'ASTON MARTIN F1 TEAM',
        'ASTON MARTIN ARAMCO COGNIZANT F1 TEAM',
        'ASTON MARTIN ARAMCO',
    ],
    'ALPINE': [
        'ALPINE',
        'ALPINE F1 TEAM',
        'BWT ALPINE F1 TEAM',
    ],
    'VCARB': [
        'ALPHATAURI',
        'SCUDERIA ALPHATAURI',
        'RB',
        'VISA CASH APP RB F1 TEAM',
        'RB F1 TEAM',
        'RACING BULLS',
    ],
    'ALFA ROMEO': [
        'ALFA ROMEO',
        'ALFA ROMEO F1 TEAM STAKE',
        'ALFA ROMEO RACING',
    ],
    'SAUBER': [
        'SAUBER',
        'STAKE F1 TEAM KICK SAUBER',
        'KICK SAUBER',
    ],
    'HAAS': [
        'HAAS',
        'HAAS F1 TEAM',
        'MONEYGRAM HAAS F1 TEAM',
    ],
    'WILLIAMS': [
        'WILLIAMS',
        'WILLIAMS RACING',
    ],
}

def load_inspect(filename, season):
    path = os.path.join(RAW_DIR, str(season), filename)
    
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return None
    
    try:
        df = pd.read_csv(path)
        
        print(f"\n{'='*60}")
        print(f"INSPECTING: {filename} (Season {season})")
        print(f"{'='*60}")
        print(f"Rows: {len(df)} | Columns: {len(df.columns)}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nFirst 3 rows:")
        print(df.head(3))
        print(f"\nData Types:")
        print(df.dtypes)
        print(f"\nMissing Values:")
        print(f"  Columns with missing: {df.isnull().any().sum()}")
        print(f"  Total missing: {df.isnull().sum().sum()}")
    
        return df
    
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return None

def standardize_driver_names(df, driver_col = 'driver'):
    if driver_col not in df.columns:
        print(f"Warning: Column '{driver_col}' not found")
        return df
    
    df[driver_col] = df[driver_col].str.upper().str.strip()
    
    for standard_code, variations in DRIVER_CORRECTIONS.items():
        for variation in variations:
            df[driver_col] = df[driver_col].replace(variation, standard_code)
            
    return df

def standardize_constructor_names(df, constructor_col = 'constructor'):
    if constructor_col not in df.columns:
        print(f"Warning: Column '{constructor_col}' not found")
        return df
    
    df[constructor_col] = df[constructor_col].str.upper().str.strip()
    
    for standard_name, variations in CONSTRUCTOR_CORRECTIONS.items():
        for variation in variations:
            df[constructor_col] = df[constructor_col].replace(variation, standard_name)
            
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
        if 'constructor' in df.columns:
            df = standardize_constructor_names(df, 'constructor')
    elif type == 'constructor':
        df = standardize_constructor_names(df, 'constructor')
        
    return df

def clean_schedule_info(df):
    if df is None or df.empty:
        print("No schedule data to clean")
        return None
    
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['round'] = pd.to_numeric(df['round'], errors='coerce')
    df['event'] = df['event'].str.strip()
    df['country'] = df['country'].str.strip()
    df['location'] = df['location'].str.strip()
    df['format'] = df['format'].str.strip()
    
    if 'season' in df.columns:
        df = df.sort_values(['season', 'round']).reset_index(drop=True)
    
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
        df['pit_lane_start'] = (df['grid_pos'] <= 0) | df['grid_pos'].isna()
        df['grid_pos'] = df['grid_pos'].clip(lower=1)
        if 'status' in df.columns:
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
            # Handle NaT and invalid times
            df[f'{col}_valid'] = (df[f'{col}_seconds'].notna()) & (df[f'{col}_seconds'] > 0) & (df[f'{col}_seconds'] < 300)
            df.loc[~df[f'{col}_valid'], f'{col}_seconds'] = np.nan
            
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['stint'] = pd.to_numeric(df['stint'], errors='coerce')
    
    if 'tyre_compound' in df.columns:
        df['tyre_compound'] = df['tyre_compound'].str.upper().str.strip()
        df['tyre_compound'] = df['tyre_compound'].fillna(df['tyre_compound'])
        
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
        df['air_temp_c'] = df['air_temp_c'].clip(0, 60)
    
    if 'track_temp_c' in df.columns:
        df['track_temp_c'] = pd.to_numeric(df['track_temp_c'], errors='coerce')
        df['track_temp_c'] = df['track_temp_c'].clip(0, 70)
    
    if 'humidity_pct' in df.columns:
        df['humidity_pct'] = pd.to_numeric(df['humidity_pct'], errors='coerce')
        df['humidity_pct'] = df['humidity_pct'].clip(0, 100)
    
    if 'wind_dir_deg' in df.columns:
        df['wind_dir_deg'] = pd.to_numeric(df['wind_dir_deg'], errors='coerce')
        df['wind_dir_deg'] = df['wind_dir_deg'] % 360
    
    for col in ['pressure_mbar', 'wind_speed_kph']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    if 'rainfall' in df.columns:
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
    
    if 'event' in df.columns:
        df['event'] = df['event'].str.strip()
    
    return df

def handle_missing_values(df):
    if df is None or df.empty:
        return df
    
    critical_cols = ['season', 'event']
    if all(col in df.columns for col in critical_cols):
        df = df.dropna(subset=critical_cols)
        
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    position_cols = ['finish_pos_numeric', 'grid_pos', 'position']
    numeric_cols = [col for col in numeric_cols if col not in position_cols]
    
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            # Fill with median
            median_val = df[col].median()
            if pd.notna(median_val):
                df[col] = df[col].fillna(median_val)
                
    return df

def validate_dataframe(df, name):
    if df is None:
        print(f"{name}: None")
        return False
    
    if df.empty:
        print(f"{name}: Empty")
        return False
    
    missing = df.isnull().sum().sum()
    print(f"{name}: {len(df)} rows, {missing} missing values")
    
    return True

def preprocess_season_data(season):
    print(f"\n{'='*70}")
    print(f"PREPROCESSING SEASON {season}")
    print(f"{'='*70}")
    
    processed_season_dir = os.path.join(PROCESSED_DIR, str(season))
    os.makedirs(processed_season_dir, exist_ok=True)
    
    results = {}
    
    datasets = [
        ('driver_standings', 'driver_standings.csv', clean_standings, {'standings_type': 'driver'}),
        ('constructor_standings', 'constructor_standings.csv', clean_standings, {'standings_type': 'constructor'}),
        ('schedule', 'schedule.csv', clean_schedule_info, {}),
        ('race_results', 'race.csv', clean_results, {'session_type': 'R'}),
        ('quali_results', 'quali_results.csv', clean_results, {'session_type': 'Q'}),
        ('sprint_results', 'sprint_results.csv', clean_results, {'session_type': 'S'}),
        ('laptimes', 'laptimes.csv', clean_laptimes, {}),
        ('pitstops', 'pitstops.csv', clean_pitstops, {}),
        ('weather', 'weather.csv', clean_weather_data, {}),
        ('race_events', 'race_events.csv', clean_race_events, {}),
    ]
    
    for name, filename, clean_func, kwargs in datasets:
        print(f"\n{'─'*70}")
        print(f"Processing: {name}")
        print(f"{'─'*70}")
        
        df = load_inspect(filename, season)
        
        if df is not None:
            df = clean_func(df, **kwargs)
            
            df = handle_missing_values(df)
            
            validate_dataframe(df, name)
            
            output_path = os.path.join(processed_season_dir, f"{name}.csv")
            df.to_csv(output_path, index=False)
            print(f"Saved to: {output_path}")
            
            results[name] = df
        else:
            print(f"Skipping {name} - no data loaded")
            results[name] = None
    
    print(f"\n{'='*70}")
    print(f"PREPROCESSING COMPLETED FOR SEASON {season}")
    print(f"{'='*70}")