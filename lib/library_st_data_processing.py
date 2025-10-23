import pandas as pd
import numpy as np
import requests
from pprint import pprint
from math import ceil
import copy
import random
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import ast
import json

pd.set_option('display.float_format', '{:.0f}'.format)

def chunk_list(lst, chunk_size):
    """
    Splits a list into chunks of specified size.
    
    Parameters:
        lst (list): The list to split.
        chunk_size (int): The maximum size of each chunk.
        
    Returns:
        generator: A generator that yields chunks of the list.
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def parse_string_to_list(input_string):
    """
    Converts a string representation of a list into an actual Python list.

    Args:
        input_string (str): The input string representing a list, e.g., 
                            "['US', 'AU', 'CA']".

    Returns:
        list: A Python list parsed from the input string.
    """
    # Use eval to safely parse the string into a list
    try:
        result = eval(input_string)
        if isinstance(result, list):
            return result
        else:
            raise ValueError("Input string does not represent a valid list.")
    except Exception as e:
        raise ValueError(f"Failed to parse input string: {e}")
    
def string_to_timestamp(date_string):
    """
    Converts an ISO 8601 date string into a Unix timestamp.
    
    Args:
        date_string (str): The date string in ISO 8601 format, e.g., "2010-05-29T00:00:00Z".
    
    Returns:
        int: The Unix timestamp (seconds since epoch).
    """
    # Parse the date string to a datetime object
    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
    
    # Convert to a timestamp
    return int(dt.timestamp())

def string_to_timestamp_v2(date_string):
    """
    Converts a date string in 'YYYY/MM/DD' format into a Unix timestamp.
    
    Args:
        date_string (str): The date string in 'YYYY/MM/DD' format, e.g., "2013/02/25".
    
    Returns:
        int: The Unix timestamp (seconds since epoch).
    """
    # Parse the date string to a datetime object
    dt = datetime.strptime(date_string, "%Y/%m/%d")
    
    # Convert to a timestamp
    return int(dt.timestamp())

def timestamp_to_string(timestamp):
    """
    Converts a Unix timestamp into a date string in 'YYYY/MM/DD' format.
    
    Args:
        timestamp (int): The Unix timestamp to convert.
    
    Returns:
        str: The formatted date string, e.g., "2013/02/25".
    """
    # Convert the timestamp to a datetime object
    dt = datetime.fromtimestamp(timestamp)
    
    # Format the datetime object as a string
    date_string = dt.strftime("%Y/%m/%d")
    
    return date_string

def add_days_to_date(date_str, days_to_add):
    """
    Adds a certain number of days to a given date string.

    Args:
        date_str (str): The input date string in the format "YYYY-MM-DD".
        days_to_add (int): The number of days to add.

    Returns:
        str: The resulting date string in the format "YYYY-MM-DD".
    """
    # Parse the input date string to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    # Add the specified number of days
    new_date_obj = date_obj + timedelta(days=days_to_add)
    # Convert back to a string and return
    return new_date_obj.strftime("%Y-%m-%d")

def get_unified_app_data(api_key, base_url, app_ids):
    """
    Fetch unified app data from the SensorTower API in batches of 100 IDs per request.
    
    Parameters:
        api_key (str): Your SensorTower API key.
        base_url (str): The base URL for the API.
        app_ids (list[str]): List of unified app IDs to fetch.
        
    Returns:
        list[dict]: Combined response data from all requests.
    """
    
    # API endpoint
    endpoint = "/v1/unified/apps"
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    all_responses = []
    chunk_size = 100
    count = 1
    
    # Split the list of app_ids into chunks of 100
    for app_id_chunk in chunk_list(app_ids, chunk_size):
        params = {
            'app_id_type': 'unified',
            'app_ids': ','.join(app_id_chunk)
        }
        
        print('making call for chunk number {}'.format(count))
        
        # Make the API request for this chunk
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            print('call successful!')
            all_responses.extend(response.json().get('apps', []))
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        count += 1
    
    return all_responses

def get_required_tags(api_key, base_url, app_ids):


    # API endpoint
    endpoint = "/v1/app_tag/tags_for_apps"
    url = f"{base_url}{endpoint}"

    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    
    all_responses = []
    chunk_size = 100
    count = 1
    
    # Split the list of app_ids into chunks of 100
    for app_id_chunk in chunk_list(app_ids, chunk_size):
        # params = {
        #     'app_ids': 'eu.nordeus.topeleven.android',
        #     'field_categories[]':'gaming',  
        #     'field_categories[]':'release_info',  
        # }

        # params = {
        #     'app_ids': 'eu.nordeus.topeleven.android',
        #     'field_categories[]':['gaming', 'release_info']  
        # }

        params = {
            'app_ids': ','.join(app_id_chunk),
            'fields[]':['Game Class', 
                        'Game Genre', 
                        'Game Sub-genre',
                        'Game Art Style',
                        'Game Camera POV',
                        'Game Setting',
                        'Game Theme',
                        'Game Product Model',
                        'IP: Corporate Parent',
                        'IP: IP Operator',
                        'IP: Media Type',
                        'IP: Licensed IP',
                        'Earliest Release Date',
                        'Release Date (WW)',
                        'Release Date (US)',
                        'Release Date (JP)',
                        'Release Date (CN)'
                       ]  
        }
        
        print('making call for chunk number {}'.format(count))
        
        # Make the API request for this chunk
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            print('call successful!')
            all_responses.extend(response.json().get('data', []))
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        count += 1
    
    return all_responses

def find_row_cell_value(row_name, tag_list):
    try:
        return list(filter(lambda x: x['name'] == row_name, tag_list))[0]['exact_value']
    except:
        return None
    
def get_tags_table(tags_data):
    
    tags_table = []

    for raw_row in tags_data:

        current_tag_list = raw_row['tags']

        new_row = dict()
        new_row['canonical_app_id'] = raw_row['app_id']
        new_row['game_class'] = find_row_cell_value('Game Class', current_tag_list)
        new_row['game_genre'] = find_row_cell_value('Game Genre', current_tag_list)
        new_row['game_subgenre'] = find_row_cell_value('Game Sub-genre', current_tag_list)
        new_row['game_art_style'] = find_row_cell_value('Game Art Style', current_tag_list)
        new_row['game_camera_pov'] = find_row_cell_value('Game Camera POV', current_tag_list)
        new_row['game_setting'] = find_row_cell_value('Game Setting', current_tag_list)
        new_row['game_theme'] = find_row_cell_value('Game Theme', current_tag_list)
        new_row['game_product_model'] = find_row_cell_value('Game Product Model', current_tag_list)
        new_row['game_ip_corporate_parent'] = find_row_cell_value('IP: Corporate Parent', current_tag_list)
        new_row['game_ip_operator'] = find_row_cell_value('IP: IP Operator', current_tag_list)
        new_row['game_ip_media_type'] = find_row_cell_value('IP: Media Type', current_tag_list)
        new_row['game_licensed_ip'] = find_row_cell_value('IP: Licensed IP', current_tag_list)
        new_row['game_earliest_release_date'] = find_row_cell_value('Earliest Release Date', current_tag_list)
        new_row['game_release_date_ww'] = find_row_cell_value('Release Date (WW)', current_tag_list)
        new_row['game_release_date_us'] = find_row_cell_value('Release Date (US)', current_tag_list)
        new_row['game_release_date_jp'] = find_row_cell_value('Release Date (JP)', current_tag_list)
        new_row['game_release_date_cn'] = find_row_cell_value('Release Date (CN)', current_tag_list) 
        tags_table.append(new_row)
    
    return tags_table

def extract_app_ids(list_of_dict):
    return list(map(lambda d: d['app_id'], list_of_dict))

def get_app_id_lists(unified_app_data):
#     also known as canonical data, cuz it also contains canonical id thingy
    itunes_app_id_list = []
    android_app_id_list = []
    
    for app in unified_app_data:
        current_app_itunes_app_ids = app['itunes_apps']
        current_app_android_app_ids = app['android_apps']
        itunes_app_id_list.extend(extract_app_ids(current_app_itunes_app_ids))
        android_app_id_list.extend(extract_app_ids(current_app_android_app_ids))
    
#     ios app id list needs to be adjusted (but not for android app id list)
    itunes_app_id_list_adjusted = list(map(lambda x: str(x),itunes_app_id_list))
    
    result_list = []
    
    for app_id in itunes_app_id_list_adjusted:
        result_list.append(
            {
                'os':'ios',
                'app_id':app_id
            }
        
        )

    for app_id in android_app_id_list:
        result_list.append(
            {
                'os':'android',
                'app_id':app_id
            }
        
        )
    
    return result_list

def get_local_app_info(api_key, base_url, os, app_ids):
    
    # API endpoint
    endpoint = "/v1/{}/apps".format(os)
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    all_responses = []
    chunk_size = 100
    count = 1
    
    # Split the list of app_ids into chunks of 100
    for app_id_chunk in chunk_list(app_ids, chunk_size):
        params = {
            'app_ids': ','.join(app_id_chunk)
        }
        
        print('making call for chunk number {}'.format(count))
        
        # Make the API request for this chunk
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            print('call successful!')
            all_responses.extend(response.json().get('apps', []))
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        count += 1
    
    return all_responses

def get_unified_id(local_app_id):
    return list(filter(lambda x: x['app_id'] == local_app_id, local_id_to_unified_id_mapping))[0]['unified_app_id']

def get_apps_performance(api_key, base_url, os, app_ids, country_code, granularity, max_periods, start_date, end_date):
    
    def split_date_range(start_date, end_date, granularity, max_periods):
        
        current_start = datetime.strptime(start_date, '%Y-%m-%d')
        final_end = datetime.strptime(end_date, '%Y-%m-%d')
        date_ranges = []
        
        day_count_per_granularity = {
            'daily': 1,
            'weekly': 7,
            'monthly': 30,
            'quarterly': 91
        }

        while current_start < final_end:
            
            # Calculate the end date for the current range
            current_end = current_start + timedelta(days=max_periods * day_count_per_granularity[granularity])
            # Ensure current_end does not exceed final_end
            current_end = min(current_end, final_end)
            
            if granularity == 'daily':
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = current_end + timedelta(days=1)
            elif granularity == 'weekly':
                current_end = current_end + timedelta(days=(6 - current_end.weekday()))
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = current_end + timedelta(days=1)
            elif granularity == 'monthly':
                current_end = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = (current_end + timedelta(days=1)).replace(day=1)
            elif granularity == 'quarterly':
                # Move current_end to the last day of the quarter
                quarter = (current_end.month - 1) // 3 + 1  # Calculate current quarter (1, 2, 3, or 4)
                next_quarter_start_month = quarter * 3 + 1  # Month of the next quarter's first day
                if next_quarter_start_month > 12:
                    next_quarter_start = current_end.replace(year=current_end.year + 1, month=1, day=1)
                else:
                    next_quarter_start = current_end.replace(month=next_quarter_start_month, day=1)
                current_end = next_quarter_start - timedelta(days=1)  # Move to the last day of the current quarter
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = (current_end + timedelta(days=1)).replace(day=1)
#                 print('current_start: {}'.format(current_start))

        return date_ranges

    
    # API endpoint   
    endpoint = "/v1/{}/sales_report_estimates".format(os)
    url = f"{base_url}{endpoint}"
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    date_ranges = split_date_range(start_date, end_date, granularity, max_periods)
    print("Date ranges: {}".format(date_ranges))
    
    all_results = []
    chunk_size = 100

    for start, end in date_ranges:
        
        print("Requesting Data for Range:", start, "to", end)
        count = 1
        
        # Split the list of app_ids into chunks of 100
        for app_id_chunk in chunk_list(app_ids, chunk_size):

            params = {
                'os': os,
                'app_ids': ','.join(app_id_chunk),
                'countries': country_code,
                'date_granularity': granularity,
                'start_date': start,
                'end_date': end,
            }

            print('making call for chunk number {}'.format(count))

            request = requests.Request("GET", url, headers=headers, params=params)
            prepared_request = request.prepare()

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                print(f"Data retrieved for {start} to {end}")
                json_data = response.json()
                if isinstance(json_data, list):  # If the response is a list
                    all_results.extend(json_data)
                elif isinstance(json_data, dict):  # If the response is a dictionary
                    all_results.extend(json_data.get('data', []))
                else:
                    print(f"Unexpected response format for {start} to {end}: {json_data}")
            else:
                print(f"Failed for {start} to {end}. Status Code: {response.status_code}")
                print(response.text)

            count += 1
        
    return all_results 

def get_monthly_app_performance_single_app_id(api_key, base_url, os, app_id, country_code, start_date, end_date):
    """
    Query monthly app performance data in 12-month segments if the range exceeds 12 months.
    """
    
    def split_date_range(start_date, end_date, max_months=12):
        """
        Split a date range into non-overlapping segments of up to `max_months` months.
        """
        current_start = datetime.strptime(start_date, '%Y-%m-%d')
        final_end = datetime.strptime(end_date, '%Y-%m-%d')
        date_ranges = []

        while current_start < final_end:
            # Calculate the end date for the current range
            current_end = current_start + timedelta(days=max_months * 30)  # Approximate 12 months
            # Ensure current_end does not exceed final_end
            current_end = min(current_end, final_end)
            # Adjust current_end to the last day of the month
            current_end = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            # Add the range to the list
            date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))

            # Move current_start to the first day of the next month after current_end
            current_start = (current_end + timedelta(days=1)).replace(day=1)

        return date_ranges
    
    print("Getting performance data for app id {}".format(app_id))

    endpoint = "/v1/{}/sales_report_estimates".format(os)
    url = f"{base_url}{endpoint}"

    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    date_ranges = split_date_range(start_date, end_date)
    
#     print("Date Ranges")
#     print(date_ranges)
    
    
    all_results = []

    for start, end in date_ranges:
        params = {
            'os': os,
            'app_ids': app_id,
            'countries': country_code,
            'date_granularity': 'monthly',
            'start_date': start,
            'end_date': end,
        }

        request = requests.Request("GET", url, headers=headers, params=params)
        prepared_request = request.prepare()

#         print("Requesting Data for Range:", start, "to", end)
#         print("Full Request URL:", prepared_request.url)

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            print(f"Data retrieved for {start} to {end}")
            json_data = response.json()
            if isinstance(json_data, list):  # If the response is a list
                all_results.extend(json_data)
            elif isinstance(json_data, dict):  # If the response is a dictionary
                all_results.extend(json_data.get('data', []))
            else:
                print(f"Unexpected response format for {start} to {end}: {json_data}")
        else:
            print(f"Failed for {start} to {end}. Status Code: {response.status_code}")
            print(response.text)
    
    return all_results

def get_performance_streams_from_list_of_app_ids(api_key, base_url, country_code, start_date, end_date, list_app_ids):
#     currently only get monthly performance
    result = []
    
    for app in list_app_ids:
        os = app["os"]
        app_id = app["app_id"]
        app_performance = get_monthly_app_performance_single_app_id(
            api_key, 
            base_url, 
            os, 
            app_id, 
            country_code,
            start_date, 
            end_date
        )
        result.append(
            {
                'app_id': app_id,
                'os': os,
                'app_performance': app_performance
            }
        )

    return result

def get_unique_dates(performance_streams_data):
    result = []
    for app in performance_streams_data:
        for record in app['app_performance']:
            if not(record['d'] in result):
                result.append(record['d'])
    return sorted(result)

def get_app_ids_from_performance_stream_data(performance_stream_data):
    return list(map(lambda x: x['app_id'], performance_stream_data))

def fill_performance_streams_df(blank_df, performance_streams_data):
    for row in performance_streams_data:
        if row['os'] == 'ios':
            for record in row['app_performance']:
                revenue_ar = record['ar']/100 if 'ar' in record else 0
                revenue_ir = record['ir']/100 if 'ir' in record else 0
                blank_df.at[row['app_id'], record['d']] = revenue_ar + revenue_ir
        elif row['os'] == 'android':
            for record in row['app_performance']:        
                blank_df.at[row['app_id'], record['d']] = record['r']/100 if 'r' in record else 0
    return blank_df

def generate_performance_streams_df(performance_streams_data):
#     prepare blank dataframe
    unique_dates = get_unique_dates(performance_streams_data)
    app_ids = get_app_ids_from_performance_stream_data(performance_streams_data)
    df = pd.DataFrame(index=app_ids, columns=unique_dates)
    
# fill the data
    return fill_performance_streams_df(df, performance_streams_data)

def get_daily_app_performance_single_unified_id(api_key, base_url, unified_id, country_code, start_date, end_date):
    """
    Fetch unified app data from the SensorTower API in batches of 100 IDs per request.
    
    Parameters:
        api_key (str): Your SensorTower API key.
        base_url (str): The base URL for the API.
        app_ids (list[str]): List of unified app IDs to fetch.
        
    Returns:
        list[dict]: Combined response data from all requests.
    """
    
    # API endpoint
    endpoint = "/v1/unified/sales_report_estimates"
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    params = {
        'app_ids': unified_id,
        'countries': country_code,
        'date_granularity': 'daily',
        'start_date': start_date,
        'end_date': end_date,
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    return response.json()

def calc_download_revenue_sum_from_sales_report_estimates(sales_report):
    unified_id = sales_report[0]['app_id']
    start_date = sales_report[0]['date']
    end_date = sales_report[-1]['date']
    
    download_sum = 0
    revenue_sum = 0
    for row in sales_report:
        download_sum = download_sum + row['unified_units']
        revenue_sum = revenue_sum + row['unified_revenue']
    return {
        'unified_id': unified_id,
        'start_date': start_date,
        'end_date': end_date,
        'download': download_sum,
        'revenue': revenue_sum/100
    }

def get_first_n_day_sum_performance_single_unified_id(api_key, base_url, unified_id, country_code, release_date, n):

    query_result = get_daily_app_performance_single_unified_id(
        api_key = api_key, 
        base_url = base_url, 
        unified_id = unified_id, 
        country_code = country_code, 
        start_date = release_date, 
        end_date = add_days_to_date(start_date, n)
    )
    
    print(query_result)
    
    return calc_download_revenue_sum_from_sales_report_estimates(query_result)

def transform_df_local_app_info_to_dict_unified_id_local_app_info(df_local_app_info):
    table_app_ids = df_local_app_info.to_dict(orient='records')
    
    # get the list of unique unified_ids
    set_unified_ids = set(map(lambda x: x['unified_app_id'], table_app_ids))
    
    # Initiate the data structure unified_id: list of app of id info
    dict_unified_id_app_ids = dict()    
    
    for unified_id in set_unified_ids:
        dict_unified_id_app_ids[unified_id] = list(filter(lambda x: x['unified_app_id'] == unified_id, table_app_ids))
    
    return dict_unified_id_app_ids

def filter_vn_app_id(app_id_list):
    result = []
    for app in app_id_list:
        if 'VN' in parse_string_to_list(app['valid_countries']) or 'VN' in parse_string_to_list(app['top_countries']):
            result.append(app)
    return result

def get_top_games_by_year(df, percentile):
    """
    Get top games (Unified ID) contributing 90% of total revenue for each year.
    
    Args:
        df (DataFrame): Input DataFrame containing game performance data.
    
    Returns:
        dict: Dictionary with years as keys and lists of Unified IDs as values.
    """
    # Ensure the 'Date' column is datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Extract the year from the 'Date' column
    df['Year'] = df['Date'].dt.year

    # Initialize result dictionary
    top_games_by_year = {}

    # Iterate through each year in the dataset
    for year, group in df.groupby('Year'):
        # Total revenue for the year
        total_revenue = group['Revenue (Absolute, $)'].sum()

        # Sort by revenue in descending order
        group = group.sort_values(by='Revenue (Absolute, $)', ascending=False)

        # Calculate cumulative revenue and cumulative percentage
        group['Cumulative Revenue'] = group['Revenue (Absolute, $)'].cumsum()
        group['Cumulative %'] = group['Cumulative Revenue'] / total_revenue

        # Get the IDs that contribute up to <percentile>% of the revenue
        top_ids = group.loc[group['Cumulative %'] <= percentile, 'Unified ID'].tolist()

        # Store the result for the year
        top_games_by_year[year] = top_ids

    return top_games_by_year

def extract_all_values(dictionary):
    """
    Extract all values from a dictionary into a single list.
    
    Args:
        dictionary (dict): Input dictionary with values being lists.
    
    Returns:
        list: A list containing all values across all keys.
    """
    return [value for values in dictionary.values() for value in values]

# A function that takes in the set of unified_id 
# then output the full-info table of those unified_id (canonical, game tags and stuff)

def retrieve_full_info_game_table(api_key, base_url, set_unified_ids):
    
    print("Get canonical app data...")
    
    canonical_app_data = get_unified_app_data(
        api_key = api_key, 
        base_url = base_url, 
        app_ids = list(set_unified_ids)
    )
    
    df_canonical_app_data = pd.DataFrame(canonical_app_data)
    
    canonical_id_list = list(df_canonical_app_data['canonical_app_id'])
    
    canonical_id_list_fixed = [str(item) for item in canonical_id_list]
    
    print("Get game tags...")
    
    tags_data = get_required_tags(
        api_key, 
        base_url, 
        canonical_id_list_fixed
    )
    
    
    
    tags_table = get_tags_table(tags_data)
    
    df_tags_table = pd.DataFrame(tags_table)
    
    df_game_full_info = pd.merge(
        df_tags_table,
        df_canonical_app_data,
        how = 'left',
        on = 'canonical_app_id'
    )
    
    new_order = [
        'unified_app_id',
        'canonical_app_id',
        'name',
        'cohort_id',
        'itunes_apps',
        'android_apps',
        'unified_publisher_ids',
        'itunes_publisher_ids',
        'android_publisher_ids',
        'game_class',
        'game_genre',
        'game_subgenre',
        'game_art_style',
        'game_camera_pov',
        'game_setting',
        'game_theme',
        'game_product_model',
        'game_ip_corporate_parent',
        'game_ip_operator',
        'game_ip_media_type',
        'game_licensed_ip',
        'game_earliest_release_date',
        'game_release_date_ww',
        'game_release_date_us',
        'game_release_date_jp',
        'game_release_date_cn'
    ]
    
    return df_game_full_info[new_order]

def compare_dataframes(df1, df2):
    """
    Compare two DataFrames cell by cell and record differences.

    Args:
        df1 (DataFrame): First DataFrame to compare.
        df2 (DataFrame): Second DataFrame to compare.

    Returns:
        list: A list of dictionaries containing the differences with details.
    """
    # Ensure both DataFrames have the same shape
    if df1.shape != df2.shape:
        raise ValueError("DataFrames do not have the same shape.")

    differences = []

    # Iterate over each cell in the DataFrame
    for row_idx in range(len(df1)):
        for col in df1.columns:
            if df1.at[row_idx, col] != df2.at[row_idx, col]:
                differences.append({
                    'Row': row_idx,
                    'Column': col,
                    'Value_df1': df1.at[row_idx, col],
                    'Value_df2': df2.at[row_idx, col]
                })
    
    return differences

def flatten_app_list_of_dicts(list_of_dict):
    result = []
    for item in list_of_dict:
        result.append(str(item['app_id']))
    return result

def plot_performance_streams_df(df_original):
    
    df = df_original.copy()
    
#     # Convert columns to datetime for proper handling
#     df.columns = pd.to_datetime(df.columns)

#     # Plot individual app revenue time series
#     plt.figure(figsize=(12, 6))
#     for app in df.index:
#         plt.plot(df.columns, df.loc[app], marker='o', label=f'{app}')
#         # Add data labels to each point
#         for x, y in zip(df.columns, df.loc[app]):
#             plt.annotate(f'{y}', (x, y), textcoords="offset points", xytext=(0, 5), ha='center')

#     # Plot total revenue time series
#     total_revenue = df.sum(axis=0)
#     plt.plot(df.columns, total_revenue, marker='o', linestyle='--', color='black', label='Total Revenue')
#     # Add data labels for total revenue
#     for x, y in zip(df.columns, total_revenue):
#         plt.annotate(f'{y}', (x, y), textcoords="offset points", xytext=(0, 5), ha='center')

#     # Customize the plot
#     plt.title('Revenue Trends of Individual Apps and Total Revenue')
#     plt.xlabel('Time')
#     plt.ylabel('Revenue')
#     plt.xticks(df.columns, rotation=45)
#     plt.legend()
#     plt.tight_layout()
#     plt.grid(True)

#     # Show the plot
#     plt.show()
    
    
    
    # Convert column names (quarters) to datetime
    df.columns = pd.to_datetime(df.columns)

    # Transpose for easier time-based plotting
    df_transposed = df.T

    # Plot each app's time series
    plt.figure(figsize=(12, 6))
    for app_id in df_transposed.columns:
        plt.plot(df_transposed.index, df_transposed[app_id], label=f'App: {app_id}')

    # Calculate and plot total revenue
    total_revenue = df_transposed.sum(axis=1)
    plt.plot(df_transposed.index, total_revenue, label='Total Revenue', color='black', linewidth=2, linestyle='--')

    # Add plot labels and legend
    plt.title('Revenue Time Series of Apps')
    plt.xlabel('Time')
    plt.ylabel('Revenue')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Show the plot
    plt.show()

def plot_interactive_performance_streams_df(df_original):
    
    df = df_original.copy()
    
    # Create a Plotly Figure
    fig = go.Figure()

    # Add lines for individual apps
    for app in df.index:
        fig.add_trace(go.Scatter(
            x=df.columns, 
            y=df.loc[app], 
            mode='lines+markers', 
            name=f'{app}',
            text=df.loc[app],
            hovertemplate='App: %{name}<br>Date: %{x}<br>Revenue: %{y}<extra></extra>'
        ))

    # Add a line for total revenue
    total_revenue = df.sum(axis=0)
    fig.add_trace(go.Scatter(
        x=df.columns, 
        y=total_revenue, 
        mode='lines+markers', 
        name='Total Revenue',
        line=dict(dash='dot', color='black'),
        text=total_revenue,
        hovertemplate='Total Revenue<br>Date: %{x}<br>Revenue: %{y}<extra></extra>'
    ))

    # Customize layout
    fig.update_layout(
        title='Revenue Trends of Individual Apps and Total Revenue',
        xaxis_title='Time',
        yaxis_title='Revenue',
        xaxis=dict(rangeslider=dict(visible=True), type='date'),
        hovermode='x unified',
        template='plotly_white',
        legend=dict(title="Apps"),
    )

    # Show interactive plot
    fig.show()

def get_adjusted_yearly_revenue(revenue_df, app_info_df):
    
    def get_revenue_multiplier_of_app_id(app_id):
        return list(app_info_df[
            app_info_df[
                'app_id'].astype(str) == str(app_id)
        ]['revenue_multiplier'])[0]
    
    revenue_multiplier_vector = list(
        map(
            lambda x: get_revenue_multiplier_of_app_id(x),
            list(revenue_df.index)
        )
    )
    
    revenue_df_adjusted = revenue_df.mul(revenue_multiplier_vector, axis=0)
    
    revenue_df_adjusted_sum = revenue_df_adjusted.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_adjusted_sum.index = pd.to_datetime(revenue_df_adjusted_sum.index)

    # Group by year and sum the revenue for each year
    yearly_revenue = revenue_df_adjusted_sum.resample('Y').sum()

    # Convert the index to year only
    yearly_revenue.index = yearly_revenue.index.year
    
    return yearly_revenue

def get_yearly_revenue_no_multiplier(revenue_df, app_info_df):
    
    revenue_df_sum = revenue_df.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_sum.index = pd.to_datetime(revenue_df_sum.index)

    # Group by year and sum the revenue for each year
    yearly_revenue = revenue_df_sum.resample('Y').sum()

    # Convert the index to year only
    yearly_revenue.index = yearly_revenue.index.year
    
    return yearly_revenue

def get_adjusted_daily_revenue(revenue_df, app_info_df):
    
    def get_revenue_multiplier_of_app_id(app_id):
        return list(app_info_df[
            app_info_df[
                'app_id'].astype(str) == str(app_id)
        ]['revenue_multiplier'])[0]
    
    revenue_multiplier_vector = list(
        map(
            lambda x: get_revenue_multiplier_of_app_id(x),
            list(revenue_df.index)
        )
    )
    
    revenue_df_adjusted = revenue_df.mul(revenue_multiplier_vector, axis=0)
    
    revenue_df_adjusted_sum = revenue_df_adjusted.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_adjusted_sum.index = pd.to_datetime(revenue_df_adjusted_sum.index)

    # Convert the index to date format (YYYY-MM-DD)
    revenue_df_adjusted_sum.index = revenue_df_adjusted_sum.index.date
    
    return revenue_df_adjusted_sum

def get_daily_revenue_no_multiplier(revenue_df):
    
    revenue_df_sum = revenue_df.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_sum.index = pd.to_datetime(revenue_df_sum.index)

    # Convert the index to date format (YYYY-MM-DD)
    revenue_df_sum.index = revenue_df_sum.index.date
    
    return revenue_df_sum

def transform_raw_revenue_dataframe(df_input):
    
    df = df_input.copy()
    
    # Ensure the 'Date' column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Extract the year from the 'Date' column
    df['Year'] = df['Date'].dt.year
    
    # Pivot the DataFrame
    transformed_df = df.pivot(index='Unified ID', columns='Year', values='Revenue (Absolute, $)')
    
    # Sort columns chronologically (ascending order by year)
    transformed_df = transformed_df.sort_index(axis=1, ascending=True)
    
    # Reset the column names (optional, to remove "Year" label)
    transformed_df.columns.name = None
    
    # Sort by the latest year (the last column after sorting columns by year)
    latest_year = transformed_df.columns[-1]  # Get the latest year
    transformed_df = transformed_df.sort_values(by=latest_year, ascending=False)
    
    return transformed_df

def adjust_revenue_df(original_df, replacement_data):
    """
    Replace the revenue series for specific game IDs in a DataFrame.
    
    Parameters:
    - original_df: The original DataFrame where index is 'unified_app_id' and columns are years.
    - replacement_data: A list of dictionaries with 'unified_app_id' and 'yearly_sum_performance_series'.
    
    Returns:
    - A new DataFrame with updated revenue values.
    """
    # Create a copy to avoid modifying the original DataFrame
    updated_df = original_df.copy()
    
    for replacement in replacement_data:
        unified_app_id = replacement['unified_app_id']
        new_series = replacement['yearly_sum_performance_series']
        
        # Replace the row corresponding to the unified_app_id with the new series
        for year, value in new_series.items():
            updated_df.at[unified_app_id, year] = value
            
    return updated_df

def get_top_items_contributing_x_pct_from_series(series_input, x):
    s = series_input.copy()
    total = s.sum()
    s = s.sort_values(ascending = False)
    s_cumsum = s.fillna(0).cumsum()
    s_cumulative_pct = s_cumsum/total
    return list(s_cumulative_pct[s_cumulative_pct <= x].index)

def get_unified_ids_contributing_x_pct_revenue_each_year(df_game_revenue_adjusted, x):
    
    top_items_contributing_x_pct_revenue_per_year = dict()

    for year in df_game_revenue_adjusted.columns:
        top_items_contributing_x_pct_revenue_per_year[year] = get_top_items_contributing_x_pct_from_series(df_game_revenue_adjusted[year], x)
    
    all_year_set = set()

    for id_list in top_items_contributing_x_pct_revenue_per_year.values():
        all_year_set.update(id_list)

    top_items_contributing_x_pct_revenue_per_year['all_year'] = list(all_year_set)
    
    return top_items_contributing_x_pct_revenue_per_year

def process_game_taxonomy(file_path):
    # Read the Excel file with the given path
    taxonomy_tree = pd.read_excel(file_path, sheet_name='taxonomy_tree')
    definition = pd.read_excel(file_path, sheet_name='definition')

    # Create a dictionary with a composite key of (item, level)
    definition_dict = {(row['item'], row['level']): row['definition'] for _, row in definition.iterrows()}

    # Process the data into the nested structure
    st_game_taxonomy = []

    # Group by game_class to start building the hierarchy
    for game_class, class_group in taxonomy_tree.groupby('game_class'):
        game_class_key = (game_class, 'game_class')
        game_class_dict = {
            'game_class': game_class,
            'definition': definition_dict.get(game_class_key, ''),
            'genres': []
        }
        
        # Group by game_genre within the game_class
        for game_genre, genre_group in class_group.groupby('game_genre'):
            game_genre_key = (game_genre, 'game_genre')
            game_genre_dict = {
                'game_genre': game_genre,
                'definition': definition_dict.get(game_genre_key, ''),
                'subgenres': []
            }
            
            # Add subgenres to the genre
            for _, row in genre_group.iterrows():
                game_subgenre_key = (row['game_subgenre'], 'game_subgenre')
                game_subgenre_dict = {
                    'game_subgenre': row['game_subgenre'],
                    'definition': definition_dict.get(game_subgenre_key, '')
                }
                game_genre_dict['subgenres'].append(game_subgenre_dict)
            
            game_class_dict['genres'].append(game_genre_dict)
        
        st_game_taxonomy.append(game_class_dict)
    
    return st_game_taxonomy

def transform_df_local_app_info_to_dict_unified_id_local_app_info(df_local_app_info):
    table_app_ids = df_local_app_info.to_dict(orient='records')
    
    # get the list of unique unified_ids
    set_unified_ids = set(map(lambda x: x['unified_app_id'], table_app_ids))
    
    # Initiate the data structure unified_id: list of app of id info
    dict_unified_id_app_ids = dict()    
    
    for unified_id in set_unified_ids:
        dict_unified_id_app_ids[unified_id] = list(filter(lambda x: x['unified_app_id'] == unified_id, table_app_ids))
    
    return dict_unified_id_app_ids

def filter_vn_app_id(app_id_list):
    result = []
    for app in app_id_list:
        if 'VN' in parse_string_to_list(app['valid_countries']) or 'VN' in parse_string_to_list(app['top_countries']):
            result.append(app)
    return result

def get_release_date_from_unified_id(df_local_app_info, df_unified_full_info, unified_id):
    
#     deprecated due to old algorithm

    dict_unified_id_local_app_info = transform_df_local_app_info_to_dict_unified_id_local_app_info(df_local_app_info)
    
    list_local_app_info_vn = filter_vn_app_id(dict_unified_id_local_app_info[unified_id])
    
    if not list_local_app_info_vn:
        return string_to_timestamp_v2(df_unified_full_info[df_unified_full_info['unified_app_id'] == unified_id]['game_earliest_release_date'])
    else:
        return min(list(map(lambda x: string_to_timestamp(x['country_release_date']), list_local_app_info_vn)))
    

def get_release_year(dataframe, unified_id):
    
#     Input dataframe = df_game_revenue_adjusted (index being the unified ids, columns being the years)

    """
    Finds the starting year of the revenue time series for a specific game.

    Args:
        dataframe (pd.DataFrame): The input table with revenue time series data, 
                                  with Unified ID as the index.
        unified_id (str): The Unified ID of the game.

    Returns:
        int: The starting year (release year) of the game, or None if no revenue is found.
    """
    # Check if the Unified ID exists in the index
    if unified_id not in dataframe.index:
        print(f"Game with Unified ID {unified_id} not found.")
        return None

    # Extract the revenue time series for the given Unified ID
    row_revenue = dataframe.loc[unified_id]

    # Find the first year with non-NaN and non-zero revenue
    for year, revenue in row_revenue.items():
        if not pd.isna(revenue) and revenue > 0:
            return int(year)

    # If no revenue is found, return None
    return None

def get_release_date_from_daily_game_performance_table(table, unified_id):
#     not the full revenue table like input of the function get_release_year
    
    print("Processing unified app id: {}".format(unified_id))
    
    date_series = list(
        filter(
            lambda x: x['unified_app_id'] == unified_id,
            table
        )
    )[0]['daily_adjusted_revenue_series']
    
    if date_series.empty:
        return None
    
    return date_series.index[0]

# Previous version

# def calculate_first_n_day_revenue(daily_sum_performance_df_for_games, unified_app_id, n):
#     """
#     Calculate the first N-day revenue for a specific game.

#     Args:
#         daily_sum_performance_df_for_games (list): List of dictionaries containing game data.
#         unified_app_id (str): The unified app ID of the game.
#         n (int): The number of days (N) to calculate the revenue for.

#     Returns:
#         float or None: The first N-day revenue for the specified game, or None if the game is not found
#                        or doesn't have enough data points.
#     """
#     # Find the game with the specified unified_app_id
#     for game_data in daily_sum_performance_df_for_games:
#         if game_data['unified_app_id'] == unified_app_id:
#             # Get the Pandas Series for the game's daily revenue
#             daily_revenue_series = game_data['daily_adjusted_revenue_series']
            
#             # Sort the Series by the index to ensure chronological order
#             daily_revenue_series = daily_revenue_series.sort_index()
            
#             # Check if the revenue series has at least N data points
#             if len(daily_revenue_series) < n:
#                 print(f"Game {unified_app_id} does not have enough data points for {n}-day revenue.")
#                 return None
            
#             # Calculate the first N-day revenue
#             first_n_day_revenue = daily_revenue_series.head(n).sum()
#             return first_n_day_revenue
    
#     # Return None if the game with the specified ID is not found
#     print(f"Game with unified_app_id {unified_app_id} not found.")
#     return None

def calculate_first_n_day_revenue(daily_sum_performance_df_for_games, unified_app_id, n):
    """
    Calculate the first N-day revenue for a specific game, accounting for gaps in data.

    Args:
        daily_sum_performance_df_for_games (list): List of dictionaries containing game data.
        unified_app_id (str): The unified app ID of the game.
        n (int): The number of days (N) to calculate the revenue for.

    Returns:
        float or None: The first N-day revenue for the specified game, or None if the game is not found
                       or the date range does not span N days.
    """
    
    print("Processing for unified app id {}".format(unified_app_id))
    
    for game_data in daily_sum_performance_df_for_games:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the Pandas Series for the game's daily revenue
            daily_revenue_series = game_data['daily_adjusted_revenue_series']
            
            # Check whether the series is empty
            if daily_revenue_series.empty:
                print(f"Game {unified_app_id} revenue series is empty.")
                return None
            
            # Sort the Series by the index to ensure chronological order
            daily_revenue_series = daily_revenue_series.sort_index()

            # Get the first and last date in the series
            start_date = daily_revenue_series.index.min()
            end_date = daily_revenue_series.index.max()
            
            # Get today date
            today_date = date.today()

            # Check if the date range spans at least N days
            date_range_days = (end_date - start_date).days + 1
            if date_range_days < n:
                if (today_date - start_date).days + 1 >= n:
                    return daily_revenue_series.sum()
                else:
                    print(f"Game {unified_app_id} does not have a date range spanning {n} days.")
                    return None

            # Calculate the first N-day revenue based on the date range
            valid_revenue = daily_revenue_series[start_date: start_date + pd.Timedelta(days=n-1)]
            first_n_day_revenue = valid_revenue.sum()
            return first_n_day_revenue

    # Return None if the game with the specified ID is not found
    print(f"Game with unified_app_id {unified_app_id} not found.")
    return None

def calculate_performance_for_new_games(daily_sum_performance_df_for_games, new_games_ids):
    """
    Calculate the first 7-day, 30-day, and 90-day revenue for new games.

    Args:
        daily_sum_performance_df_for_games (list): List of dictionaries containing game data.
        new_games_ids (list): List of unified_app_id for new games.

    Returns:
        list: A list of dictionaries containing unified_app_id and calculated revenue metrics.
    """
    performance_list = []

    for game_id in new_games_ids:
        # Initialize the dictionary for the game
        game_performance = {'unified_app_id': game_id}
        
        # Calculate revenue for 7-day, 30-day, and 90-day
        game_performance['first 7-day revenue'] = calculate_first_n_day_revenue(daily_sum_performance_df_for_games, game_id, 7)
        game_performance['first 30-day revenue'] = calculate_first_n_day_revenue(daily_sum_performance_df_for_games, game_id, 30)
        game_performance['first 90-day revenue'] = calculate_first_n_day_revenue(daily_sum_performance_df_for_games, game_id, 90)
        
        # Add the result to the list
        performance_list.append(game_performance)

    return performance_list

def update_game_classification(df_input, df_genre_validation_gpt_output_manually_corrected):
    """
    Updates the 'game_class', 'game_genre', and 'game_subgenre' columns in `df_game_full_info` 
    based on the corrected classifications in `df_genre_validation_gpt_output_manually_corrected`.

    Args:
        df_game_full_info (pd.DataFrame): The main dataframe containing game information.
        df_genre_validation_gpt_output_manually_corrected (pd.DataFrame): 
            The dataframe with corrected classification data.

    Returns:
        pd.DataFrame: Updated `df_game_full_info` with corrected classification for certain games.
    """
    
    df_game_full_info = df_input.copy()
    
    # Create a mapping of game_id to the finalized classifications
    corrected_classifications = df_genre_validation_gpt_output_manually_corrected[
        ['game_id', 'finalized_game_class', 'finalized_game_genre', 'finalized_game_subgenre']
    ].set_index('game_id')

    # Ensure game_id exists in both DataFrames
    df_game_full_info.set_index('unified_app_id', inplace=True)
    
    # Update the game_class, game_genre, and game_subgenre based on the corrected data
    df_game_full_info.update(corrected_classifications.rename(columns={
        'finalized_game_class': 'game_class',
        'finalized_game_genre': 'game_genre',
        'finalized_game_subgenre': 'game_subgenre'
    }))
    
    # Reset index to preserve the original structure
    df_game_full_info.reset_index(inplace=True)
    
    return df_game_full_info

def update_game_classification_for_revenue_df(df_input, additional_genre_data):
    """
    Updates the genre classification information in a DataFrame with additional data.

    Args:
        df (pd.DataFrame): The DataFrame containing game revenue and classification info.
        additional_genre_data (list of dict): List of dictionaries with additional genre data.

    Returns:
        pd.DataFrame: Updated DataFrame with genre classification information.
    """
    
    df = df_input.copy()
    
    # Convert the list of dictionaries to a temporary DataFrame
    additional_genre_df = pd.DataFrame(additional_genre_data)
    
    # Rename columns in the temporary DataFrame to match the target DataFrame
    additional_genre_df.rename(columns={
        'game_id': 'Unified ID',
        'finalized_game_class': 'game_class',
        'finalized_game_genre': 'game_genre',
        'finalized_game_subgenre': 'game_subgenre'
    }, inplace=True)
    
    # Set 'Unified ID' as the index for both DataFrames to facilitate alignment
    additional_genre_df.set_index('Unified ID', inplace=True)
    
    # Update the main DataFrame with the additional genre data
    df.update(additional_genre_df)

    return df

def join_game_genres_to_revenue(df_game_revenue_adjusted, df_game_full_info_with_corrected_genres):
    """
    Joins the fields 'game_class', 'game_genre', and 'game_subgenre' from the 
    df_game_full_info_with_corrected_genres dataframe to the df_game_revenue_adjusted dataframe
    and moves the newly added columns to the leftmost position.

    Args:
        df_game_revenue_adjusted (pd.DataFrame): The dataframe with revenue data by year.
        df_game_full_info_with_corrected_genres (pd.DataFrame): 
            The dataframe with game information.
        
    Returns:
        pd.DataFrame: Updated dataframe with 'game_class', 'game_genre', and 'game_subgenre' 
                      fields added at the leftmost position.
    """
    # Ensure 'unified_app_id' is the index in df_game_full_info_with_corrected_genres
    df_genres = df_game_full_info_with_corrected_genres.set_index('unified_app_id')[
        ['game_class', 'game_genre', 'game_subgenre']
    ]

    # Join the genre information to the revenue dataframe
    df_merged = df_game_revenue_adjusted.join(df_genres, how='left')

    # Move the new columns to the leftmost position
    new_columns = ['game_class', 'game_genre', 'game_subgenre']
    reordered_columns = new_columns + [col for col in df_merged.columns if col not in new_columns]
    df_merged = df_merged[reordered_columns]

    return df_merged

def get_monthly_revenue_series(daily_sum_performance, unified_app_id):
    """
    Convert the daily revenue series of a game to a monthly revenue series.

    Args:
        daily_sum_performance (list of dict): The data structure containing game daily revenue.
        unified_app_id (str): The unified_app_id of the game.

    Returns:
        dict: A dictionary with 'unified_app_id' and 'monthly_adjusted_revenue_series'.
    """
    # Find the game with the specified unified_app_id
    for game_data in daily_sum_performance:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the daily revenue series
            daily_series = game_data['daily_adjusted_revenue_series']
            
            # Ensure the index is a datetime index
            daily_series.index = pd.to_datetime(daily_series.index)
            
            # Resample to monthly frequency and sum the revenues
            monthly_series = daily_series.resample('M').sum()
            
            return {
                'unified_app_id': unified_app_id,
                'monthly_adjusted_revenue_series': monthly_series
            }
    
    # If the game is not found, return None or raise an exception
    print(f"Game with unified_app_id '{unified_app_id}' not found.")
    return None

def calculate_average_30_day_revenue(daily_sum_performance, unified_app_id):
    """
    Calculate the 30-day average revenue for a game based on daily data.
    
    Args:
        daily_sum_performance (list of dict): List of dictionaries with daily revenue series.
        unified_app_id (str): The unified_app_id of the game.
    
    Returns:
        dict: A dictionary containing:
            - 'unified_app_id': The game ID.
            - 'average_30_day_revenue': The average 30-day revenue or None if not enough data.
    """
    
    # Find the game with the specified unified_app_id
    for game_data in daily_sum_performance:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the daily revenue series
            daily_series = game_data['daily_adjusted_revenue_series']
            
            # Check if there are at least 180 data points
            if len(daily_series) >= 180:
                # Calculate the average daily revenue
                avg_daily_revenue = daily_series.mean()
                # Calculate the average 30-day revenue
                avg_30_day_revenue = avg_daily_revenue * 30
                return {
                    'unified_app_id': unified_app_id,
                    'average_30_day_revenue': avg_30_day_revenue
                }
            else:
                # Not enough data to calculate the average
                return {
                    'unified_app_id': unified_app_id,
                    'average_30_day_revenue': None
                }
    
    # If the game is not found, return None
    print(f"Game with unified_app_id '{unified_app_id}' not found.")
    return None

def fill_download_streams_df(blank_df, performance_streams_data):
    for row in performance_streams_data:
        if row['os'] == 'ios':
            for record in row['app_performance']:
                download_iu = record['iu'] if 'iu' in record else 0
                download_au = record['au'] if 'au' in record else 0
                blank_df.at[row['app_id'], record['d']] = download_iu + download_au
        elif row['os'] == 'android':
            for record in row['app_performance']:        
                blank_df.at[row['app_id'], record['d']] = record['u'] if 'u' in record else 0
    return blank_df

def generate_download_streams_df(performance_streams_data):
#     prepare blank dataframe
    unique_dates = get_unique_dates(performance_streams_data)
    app_ids = get_app_ids_from_performance_stream_data(performance_streams_data)
    df = pd.DataFrame(index=app_ids, columns=unique_dates)
    
# fill the data
    return fill_download_streams_df(df, performance_streams_data)

def get_daily_download(download_df):
    
    # Sum across the downloads of different apps of the same game
    download_df_sum = download_df.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    download_df_sum.index = pd.to_datetime(download_df_sum.index)

    # Convert the index to date format (YYYY-MM-DD)
    download_df_sum.index = download_df_sum.index.date

    return download_df_sum

def transform_game_download_list_of_dict_to_dataframe(the_list):
    
# the input list can be in the form of [
#     {
#      'unified_app_id',
#      'daily_download_series'
#     }
# ]
    
    # Convert list of dictionaries to a dictionary of Series
    the_dict = {entry['unified_app_id']: entry['daily_download_series'] for entry in the_list}
    
    # Convert to DataFrame
    download_df = pd.DataFrame(the_dict).T  # Transpose to make game IDs rows
    
    return download_df

def calculate_first_n_day_download(daily_sum_download_df_for_games, unified_app_id, n):
    
    print("Processing for unified app id {}".format(unified_app_id))
    
    for game_data in daily_sum_download_df_for_games:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the Pandas Series for the game's daily revenue
            daily_download_series = game_data['daily_download_series']
            
            # Check whether the series is empty
            if daily_download_series.empty:
                print(f"Game {unified_app_id} download series is empty.")
                return None
            
            # Sort the Series by the index to ensure chronological order
            daily_download_series = daily_download_series.sort_index()

            # Get the first and last date in the series
            start_date = daily_download_series.index.min()
            end_date = daily_download_series.index.max()
            
            # Get today date
            today_date = date.today()

            # Check if the date range spans at least N days
            date_range_days = (end_date - start_date).days + 1
            if date_range_days < n:
                if (today_date - start_date).days + 1 >= n:
                    return daily_download_series.sum()
                else:
                    print(f"Game {unified_app_id} does not have a date range spanning {n} days.")
                    return None

            # Calculate the first N-day revenue based on the date range
            valid_download = daily_download_series[start_date: start_date + pd.Timedelta(days=n-1)]
            first_n_day_download = valid_download.sum()
            return first_n_day_download

    # Return None if the game with the specified ID is not found
    print(f"Game with unified_app_id {unified_app_id} not found.")
    return None

def get_release_date_from_daily_game_download_table(table, unified_id):
#     not the full revenue table like input of the function get_release_year
    
    print("Processing unified app id: {}".format(unified_id))
    
    date_series = list(
        filter(
            lambda x: x['unified_app_id'] == unified_id,
            table
        )
    )[0]['daily_download_series']
    
    if date_series.empty:
        return None
    
    return date_series.index[0]
    
def calculate_revenue_and_growth_by_classification(df_revenue_with_genres, classification):
    """
    Calculate total revenue, YoY growth, and CAGR for revenue grouped by 'game_class'.

    Args:
        df_revenue_with_genres (pd.DataFrame): Revenue data with 'game_class', 'game_genre', 'game_subgenre'.
        classification (str): Classification to group by ('class', 'genre', 'subgenre').

    Returns:
        tuple: Two DataFrames:
            - Total revenue by game class.
            - Growth and CAGR by game class.
    """

    classifications = ['class', 'genre', 'subgenre']
    if classification not in classifications:
        raise Exception('Invalid classification. Must be in the list {}'.format(classifications))

    full_classification_list = ['game_' + cls for cls in classifications]

    if classification == 'class':
        to_drop_list = ['game_genre', 'game_subgenre']
        groupby_list = ['game_class']
    elif classification == 'genre':
        to_drop_list = ['game_subgenre']
        groupby_list = ['game_class', 'game_genre']
    else:
        to_drop_list = []
        groupby_list = full_classification_list

    # Group by classification and sum revenue
    total_revenue = df_revenue_with_genres.drop(
        to_drop_list, axis=1
    ).groupby(groupby_list).sum()

    # Calculate YoY growth (proportions, not percentages)
    yoy_growth = total_revenue.pct_change(axis=1) * 100

    # Calculate CAGR with dynamic start year
    cagr_results = {}
    for game_class, revenue_row in total_revenue.iterrows():
        # Drop NaN values
        revenue_row = revenue_row.dropna()

        # Dynamically determine the first year with non-zero revenue
        for year in revenue_row.index:
            if revenue_row[year] > 0:
                actual_start_year = year
                break
        else:
            # If no non-zero revenue found, skip this group
            cagr_results[game_class] = None
            continue

        # Determine the end year
        actual_end_year = revenue_row.index.max()
        revenue_start = revenue_row[actual_start_year]
        revenue_end = revenue_row[actual_end_year]

        if actual_start_year == actual_end_year or revenue_start == 0 or revenue_end == 0:
            # Cannot calculate CAGR for only one year or if revenue is zero
            cagr_results[game_class] = None
        else:
            num_years = int(actual_end_year) - int(actual_start_year)
            cagr_results[game_class] = ((revenue_end / revenue_start) ** (1 / num_years) - 1) * 100

    # Create a DataFrame for CAGR
    cagr_df = pd.DataFrame.from_dict(cagr_results, orient='index', columns=['CAGR'])

    # Combine results into one DataFrame with only YoY growth and CAGR
    growth_and_cagr = pd.concat([yoy_growth.add_suffix('_YoY'), cagr_df], axis=1)

    return total_revenue, growth_and_cagr
