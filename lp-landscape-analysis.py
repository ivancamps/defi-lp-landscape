# Automated Updates and Calculations in 'DeFi Landscape LP Opportunities'

# 1. Setup

# Import all relevant libraries
import pandas as pd
import json
from datetime import datetime as dt
import csv
import requests
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from collections import defaultdict

# Setup Defillama API

# API TVL base URL
tvl_base_url = 'https://api.llama.fi'

# API Yields base URL
yields_base_url = 'https://yields.llama.fi'



# 2. Update Data in 'DeFi Landscape LP Opportunities'

# Helper functions
# Converts a dataframe column from str to date format in-place
def df_str_to_date(df, column_name, date_format):
  df[column_name] = pd.to_datetime(df[column_name], format=date_format, errors='raise')
  df[column_name] = df[column_name].dt.date

# Read in list from CSV file
def read_list_from_csv(filename):
  data_list = []

  with open(filename, newline='') as csvfile:
      # Create a CSV reader
      reader = csv.reader(csvfile)

      # Iterate over each row in the CSV file
      for row in reader:
          # Each row is a list
          data_list.append(row)

  data_list = [sublist[0] for sublist in data_list]

  return data_list




# 2.1. Update data in 'Project Ratings' tab

# Read in list of protocols
protocols_filename = "protocol_slugs.csv"
protocol_slugs = read_list_from_csv(protocols_filename)

# 2.1.1. Update current protocol TVLs
# Get current TVLs for the list of protocols and return them as a list
protocol_tvls = [requests.get(tvl_base_url + '/tvl/' + protocol).json() for protocol in protocol_slugs]

# Format numbers as millions before writing to spreadsheet
protocol_tvls_m = [int(tvl) / 1000000 for tvl in protocol_tvls]


# 2.1.2. Update 1-year average protocol TVLs
# Pull historical TVL data for each protocol in the list broken down by token and chain
protocol_historicals = [requests.get(tvl_base_url + '/protocol/' + protocol).json() for protocol in protocol_slugs]

# List of keys we want to keep
fields = ['name','currentChainTvls']



# Sanity check to compare aggregated TVL to actual current TVL shown by Defillama

# Create a list of dictionaries with protocol name as the key as current TVLs
# by chain as the value for every protocol in the list

# List to store broken down results
protocol_tvls_by_chain = []

# List to store aggreagated TVL results
aggregated_protocol_tvls = []

# Iterate protocol list
for protocol in protocol_historicals:
  # keep only relevant keys
  p = dict((k, protocol[k]) for k in fields if k in protocol)

  # Create key,value pair from the values associated with the two keys left
  p = {protocol['name']: protocol['currentChainTvls']}

  # Iterate nested dict and remove unwanted chains to prevent double counting TVL
  for k,v in p.items():
    for x in list(v.keys()):
      if "borrowed" in x or "staking" in x or 'pool2' in x:
        del v[x]
    aggregated = (k, sum(v.values()))

  # Add protocol to result list
  protocol_tvls_by_chain.append(p)

  # Add protocol overall current TVL to aggregated list
  aggregated_protocol_tvls.append(aggregated)

# COMPARE AGGREGATED VS. ACTUAL TVLs

current_aggregated_protocol_tvls = [x[1] for x in aggregated_protocol_tvls]
protocol_names = [x[0] for x in aggregated_protocol_tvls]

# Create tuples for each protocol comparing aggregated TVL to TVL shown by DL
protocol_tvl_deltas = list(zip(protocol_names, zip(current_aggregated_protocol_tvls, protocol_tvls)))



# Aggregate historical TVL broken down by token and chain to get overall historical protocol TVLs

# Create a list of dictionaries with protocol name as the key and historical TVL
# by token and chain as the value for every protocol in the list

# List to store broken down results
clean_protocol_historicals = []

# Iterate protocol list
for protocol in protocol_historicals:
  # Keep only relevant keys
  p = dict((k, protocol[k]) for k in fields if k in protocol)

  # Create key,value pair from the values associated with the two keys left
  p = {protocol['name']: protocol['chainTvls']}

  # Iterate nested dict and remove unwanted chains to prevent double counting TVL
  for k,v in p.items():
    for x in list(v.keys()):
      if "borrowed" in x or "staking" in x or 'pool2' in x:
        del v[x]

  # Add protocol to result list
  clean_protocol_historicals.append(p)


# List to store the final result
historical_protocol_tvls = []

# Aggregate TVL data per date for each chain
for protocol in clean_protocol_historicals:

  # Dictionary to store result
  aggregated_historicals = defaultdict(float)

  for p, d1 in protocol.items():
    for chain, chain_data in d1.items():
        for entry in chain_data["tvl"]:
            date = entry["date"]
            totalLiquidityUSD = float(entry["totalLiquidityUSD"])
            aggregated_historicals[date] += totalLiquidityUSD

  # Convert the aggregated data to a sorted list of tuples
  sorted_aggregated_historicals = sorted(aggregated_historicals.items(), key=lambda x: x[0])

  # Convert the aggregated data to a dataframe
  aggregated_historicals_df = pd.DataFrame(sorted_aggregated_historicals, columns=['date', 'totalLiquidityUSD'])

  # Convert the 'Date' column from UNIX timestamp to datetime format
  aggregated_historicals_df['date'] = pd.to_datetime(aggregated_historicals_df['date'], unit='s')
  aggregated_historicals_df['date'] = aggregated_historicals_df['date'].dt.date

  # Add dataframe to result list
  historical_protocol_tvls.append(aggregated_historicals_df)


# Order by date in reverse chronological order
historical_protocol_tvls = [df.sort_values(by='date', ascending=False) for df in historical_protocol_tvls]

# Create a common index for all dataframes to conserve the order when concatenating
for df in historical_protocol_tvls:
    df.index = range(len(df))

# Concatenate every dataframe in the list into one dataframe
protocol_historicals_df = pd.concat(historical_protocol_tvls, axis = 1, sort=False)




# 2.2 Update 'Pool Yields' raw data tab
# Get all pool yields
yields = requests.get(yields_base_url + '/pools')

# Convert Yields response to data frame
pool_yields_df = pd.DataFrame(yields.json()['data'])




# 2.3 Update 'Pool Historicals' raw data tab
# Read in list of pool ids
pool_ids_filename = "pool_ids.csv"
pool_ids = read_list_from_csv(pool_ids_filename)

# Get historical TVL and APY data for each pool and return it as a list of data frames
pool_historicals = [requests.get(yields_base_url + '/chart/' + id).json()['data'] for id in pool_ids]
pool_historicals_dfs = [pd.DataFrame(i).drop(['il7d', 'apyBase7d'], axis=1) for i in pool_historicals]

# Convert timestamp format from str to date in every data frame
date_format = '%Y-%m-%dT%H:%M:%S.%fZ'

for df in pool_historicals_dfs:
  df_str_to_date(df, 'timestamp', date_format)

# Order by date in reverse chronological order
pool_historicals_dfs = [df.sort_values(by='timestamp', ascending=False) for df in pool_historicals_dfs]

# Create a common index for all dataframes to conserve the order when concatenating
for df in pool_historicals_dfs:
    df.index = range(len(df))

# Concatenate every data frame in the list into one data frame
pool_historicals_df = pd.concat(pool_historicals_dfs, axis = 1, sort=False)




# 2.4. Update 'Historical Chain TVL' raw data tab
# Get historical TVL data for Ethereum
eth_tvl = requests.get(tvl_base_url + '/v2/historicalChainTvl/Ethereum')

# Convert response to data frame
eth_tvl_df = pd.DataFrame(eth_tvl.json())

# Convert the 'Date' column from UNIX timestamp to datetime format
eth_tvl_df['date'] = pd.to_datetime(eth_tvl_df['date'], unit='s')
eth_tvl_df['date'] = eth_tvl_df['date'].dt.date

# Order by date in reverse chronological order
eth_tvl_df = eth_tvl_df.sort_values(by='date', ascending=False)




# 2.5. Update 'Protocol Info' raw data tab
# Get all protocols
protocols = requests.get(tvl_base_url + '/protocols')

# Convert Protocols response to data frame
protocols_df = pd.DataFrame(protocols.json())




# 2.6. Update data in 'LP Update Historicals' tab for LP Landscape Update distribution charts
# Create dictionary to associate all pool ids to their historical data
all_pools_dict = dict(zip(pool_ids, pool_historicals_dfs))

# Read in list of Curve eUSD peer pool ids
eusd_curve_peer_pools_filename = "eusd_curve_peer_pool_ids.csv"
eusd_curve_peer_pool_ids = read_list_from_csv(eusd_curve_peer_pools_filename)


# Keep only data for pools contained in pool id list
eusd_curve_peer_pools = {k: all_pools_dict[k] for k in eusd_curve_peer_pool_ids}

# Separate values (dataframes) from keys (pool id's) before concatenating
eusd_curve_peer_pools_dfs = list(eusd_curve_peer_pools.values())

# Concatenate every dataframe in the list into one dataframe
eusd_curve_peer_pools_df = pd.concat(eusd_curve_peer_pools_dfs, axis = 1, sort=False)


# 2.7. Aggregate historical data to create indices for RToken peer groups
# RToken pool id's
eusd_pool_id = "381b00d5-b4f8-489c-95cb-40018c72bdd3"
hyusd_pool_id = "3378bced-4bde-4ccf-b742-7d5c8ebb7720"

# HELPER FUNCTIONS

# Aggregate historicals for peer pools, store mean values, and return as dataframe
def aggregate_historicals(all_pools_dict, pool_id_list):
  # Keep only data for pools contained in pool_id_list
  peer_pools = {k: all_pools_dict[k] for k in pool_id_list}

  # Separate values (dataframes) from keys (pool id's) before concatenating
  peer_pools_dfs = list(peer_pools.values())

  # Concatenate all dataframes
  peer_pools_df = pd.concat(peer_pools_dfs)

  # Group by timestamp and compute the mean for each group
  aggregated_df = peer_pools_df.groupby('timestamp').mean().reset_index()

  # Order by date in reverse chronological order
  aggregated_df = aggregated_df.sort_values(by='timestamp', ascending=False)

  return aggregated_df


# hyUSD Comps

# Read in list of Curve eUSD peer pool ids
hyusd_peers_ids_filename = "hyusd_peers_ids.csv"
hyusd_peers_ids = read_list_from_csv(hyusd_peers_ids_filename)

# Aggregate historicals for peer pools and store mean values as dataframe
hyusd_peers_df = aggregate_historicals(all_pools_dict, hyusd_peers_ids)