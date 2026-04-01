import pandas as pd
from dotenv import load_dotenv
import os
from datetime import date, timedelta
import time
import json
import ijson
from sqlalchemy import create_engine
from tqdm import tqdm
from pathlib import Path

import sys
sys.path.append('./lib')

import library_st_data_processing as lsdp

# load .env from the current directory (or specify a path)
load_dotenv(dotenv_path=".env")
api_key = os.getenv("ST_API_KEY")

base_url = 'https://api.sensortower.com'

today_str = date.today().strftime("%Y-%m-%d")
timestamp = int(time.time())

# Read config variables
CONFIG_PATH = Path(__file__).resolve().parent / "config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

path_top_game_annual_performance = cfg["TOP_GAME_ANNUAL_PERFORMANCE"]
path_mapping_publisher_to_apps_top_down = cfg["MAPPING_PUBLISHER_TO_APPS_TOP_DOWN"]
path_mapping_publisher_to_publisherids_revenue_multiplier = cfg["MAPPING_PUBLISHER_TO_PUBLISER_ID_AND_REVENUE_MULTIPLIER_BOTTOM_UP"]
path_mapping_app_to_revenue_multiplier_special_case = cfg["MAPPING_APP_TO_REVENUE_MULTIPLIER_SPECIAL_CASES"]
path_game_info_adjustment = cfg["GAME_INFO_ADJUSTMENT"]


### DATA FETCHING ###


#--------------- Get Raw File: Game Full Info ---------------#
print("GETTING FILE: GAME FULL INFO...")

# Load Top Game Annual Performance file
df_top_game_annual_performance = pd.read_csv(path_top_game_annual_performance)

# Get the unified_app_ids
unified_app_ids = set(list(df_top_game_annual_performance["unified_id"]))

# Retrieve Game Full Info file
df_game_full_info = lsdp.retrieve_full_info_game_table(api_key, base_url, unified_app_ids)

# Export the file to csv and save to base layer folder
path_game_full_info = 'data/base/st_api_game_full_info_{}.csv'.format(timestamp)
df_game_full_info.to_csv(path_game_full_info, index=False)

print(f"FILE SAVED SUCCESSFULLY: GAME FULL INFO: {path_game_full_info}")


#--------------- Get Raw File: App Full Info ---------------#
print("GETTING FILE: APP FULL INFO (NDJSON)...")

# Read file Game Full Info
df_game_full_info = pd.read_csv(path_game_full_info)

# Retrieve file App Full Info into NDJSON output file
lsdp.get_local_app_info_from_game_full_info_table_ndjson_version(api_key, base_url, df_game_full_info, timestamp, "data/base")
path_app_full_info_ndjson = f"data/base/st_api_app_full_info_{timestamp}.ndjson"

print(f"FILE SAVED SUCCESSFULLY: APP FULL INFO (NDJSON): {path_app_full_info_ndjson}")


#--------------- Get Raw File: Mapping Publisher to Apps and Publisher IDs (Top Down) ---------------#
print("GETTING FILE: MAPPING PUBLISHER TO APPS AND PUBLISHER IDS (TOP DOWN)...")

# Retrieve the publisherid from ST
df_mapping_publisher_to_apps_and_publisherids = lsdp.create_mapping_publisher_to_apps_publisherids(path_mapping_publisher_to_apps_top_down, api_key, base_url)

# Export the file to csv and save to base layer folder
path_mapping_publisher_to_apps_and_publisherids = f"data/base/st_api_mapping_publisher_to_apps_publisherids_top_down_{timestamp}.csv"
df_mapping_publisher_to_apps_and_publisherids.to_csv(path_mapping_publisher_to_apps_and_publisherids, index=False)

print(f"FILE SAVED SUCCESSFULLY: MAPPING PUBLISHER TO APPS AND PUBLISHER IDS (TOP DOWN): {path_mapping_publisher_to_apps_and_publisherids}")


#--------------- Get Raw File: App Performance (Daily) ---------------#
print("GETTING FILE: APP PERFORMANCE (DAILY)...")

# Read file App Full Info
df_app_full_info = pd.read_json(path_app_full_info_ndjson, lines=True)

# Run the function to fetch the data and export to json
app_performance_data = lsdp.create_table_app_performance_grouped_by_game_daily(
    api_key, 
    base_url, 
    str_start_date = '2014-01-01', 
    str_end_date = (date.today() - timedelta(days=3)).isoformat(), 
    df_app_full_info = df_app_full_info, 
    json_export_path = 'data/base')

path_app_performance_daily = f"data/base/st_app_performance_daily_{timestamp}.json"

print(f"FILE SAVED SUCCESSFULLY: APP PERFORMANCE (DAILY): {path_app_performance_daily}")


#--------------- Get Staging File: Mapping Publisher to Publisher ID and Revenue Multiplier (Full) ---------------#
print("GETTING FILE: MAPPING PUBLISHER TO PUBLISHER ID AND REVENUE MULTIPLIER (FULL)...")

# Read file Mapping Publisher to Apps and Publisher IDs (Top Down)

data_types = {
    "os_x": "string",
    "cleaned_publisher_name": "string",
    "game_name": "string",
    "sensor_tower_link": "string",
    "app_id_trimmed": "string",
    "publisher_id": "string",
    "publisher_name": "string",
}

df_mapping_publisher_to_apps_and_publisherids_top_down = pd.read_csv(
    path_mapping_publisher_to_apps_and_publisherids,
    dtype = data_types
)

# Read file Mapping Publisher to Publisher ID and Revenue Multiplier (Bottom Up)

data_types = {
    "cleaned_publisher_name": "string",
    "publisher_id": "string",
    "publisher_name": "string",
    "revenue_multiplier": "int64"
}

df_mapping_publisher_to_publisherid_revenue_multiplier_bottom_up = pd.read_csv(
    path_mapping_publisher_to_publisherids_revenue_multiplier,
    dtype = data_types
)

# Merge the top-down and bottom-up tables into a full mapping

df_mapping_publisher_to_publisherid_revenue_multiplier_full = lsdp.create_mapping_publisher_to_publisherids_revenue_multiplier(
    df_mapping_publisher_to_apps_and_publisherids_top_down,
    df_mapping_publisher_to_publisherid_revenue_multiplier_bottom_up
)

# Export the file to csv and save to staging layer folder
path_mapping_publisher_to_publiserid_and_revenue_multiplier_full = f"data/staging/st_mapping_publisher_to_publisherids_revenue_multiplier_full_{timestamp}.csv"
df_mapping_publisher_to_publisherid_revenue_multiplier_full.to_csv(path_mapping_publisher_to_publiserid_and_revenue_multiplier_full,index=False)

print(f"FILE SAVED SUCCESSFULLY: MAPPING PUBLISHER TO PUBLISHER ID AND REVENUE MULTIPLIER (FULL): {path_mapping_publisher_to_publiserid_and_revenue_multiplier_full}")


#--------------- Get Staging File: App Full Info (Adjusted) ---------------#
print("GETTING FILE: APP FULL INFO (ADJUSTED) (NDJSON)...")

# Specify the output file path
path_app_full_info_adjusted_ndjson = f"data/staging/st_app_full_info_adjusted_{timestamp}.ndjson"

# Streaming and adjusting to create new App Full Info (Adjusted)
lsdp.adjust_ndjson_app_full_info(
    input_ndjson_path=path_app_full_info_ndjson,
    output_ndjson_path=path_app_full_info_adjusted_ndjson,
    publisher_mapping_csv_path=path_mapping_publisher_to_publiserid_and_revenue_multiplier_full,
    special_case_csv_path=path_mapping_app_to_revenue_multiplier_special_case,
)

print(f"FILE SAVED SUCCESSFULLY: APP FULL INFO (ADJUSTED): {path_app_full_info_adjusted_ndjson}")


#--------------- Get Staging File: App Performance (Daily) - Revenue Adjusted ---------------#
print("GETTING FILE: APP PERFORMANCE (DAILY) - REVENUE ADJUSTED...")

# Read file App Full Info (Adjusted)
df_app_full_info_adjusted = pd.read_json(path_app_full_info_adjusted_ndjson, lines=True)

# Specify the path of input and output
file_timestamp = os.path.basename(path_app_performance_daily).split("_")[-1].split(".")[0]
path_app_performance_daily_revenue_adjusted = "data/staging/st_app_performance_daily_{}_revenue_adjusted.json".format(file_timestamp)

# Run the function for adjusting and streaming
lsdp.stream_and_adjust_app_performance_daily_json_file(
    path_app_performance_daily,
    path_app_performance_daily_revenue_adjusted,
    df_app_full_info_adjusted
)

print(f"FILE SAVED SUCCESSFULLY: APP PERFORMANCE (DAILY) - REVENUE ADJUSTED: {path_app_performance_daily_revenue_adjusted}")


#--------------- Get Staging File: Game Full Info (Adjusted) ---------------#
print("GETTING FILE: GAME FULL INFO (ADJUSTED)...")

# Load Game Full Info file
df_game_full_info = pd.read_csv(path_game_full_info, low_memory=False)

# Load Game Info Adjustment file
df_game_info_adjustment = pd.read_csv(path_game_info_adjustment, low_memory=False)

# Apply the function to modify the metadata of the games that need modification
df_game_full_info_adjusted = lsdp.apply_game_info_adjustments(
    df_game_full_info,
    df_game_info_adjustment,
    key="unified_app_id"
)

# Export the file to csv
file_timestamp_game_full_info = os.path.basename(path_game_full_info).split("_")[-1].split(".")[0]
path_game_full_info_adjusted = f"data/staging/st_api_game_full_info_{file_timestamp_game_full_info}_adjusted.csv"
df_game_full_info_adjusted.to_csv(path_game_full_info_adjusted, index=False)