# Automated Updates and Calculations in 'DeFi Landscape LP Opportunities'



# 1. Setup

# Install and import all relevant libraries

import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.colab import drive
from collections import defaultdict

# Connect to Google Sheets API and setup Defillama API

drive.mount('/content/drive')

# Set up Google Sheets API credentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('/content/drive/My Drive/Colab Notebooks/lp-landscape-analysis-dd6d6479b244.json', scope)
gc = gspread.authorize(credentials)

# API TVL base URL
tvl_base_url = 'https://api.llama.fi'

# API Yields base URL
yields_base_url = 'https://yields.llama.fi'



# 2. Update Data in 'DeFi Landscape LP Opportunities'

# HELPER FUNCTIONS
# Converts a dataframe column from str to date format in-place
def df_str_to_date(df, column_name, date_format):
  df[column_name] = pd.to_datetime(df[column_name], format=date_format, errors='raise')
  df[column_name] = df[column_name].dt.date

# Open spreadsheet and load relevant tabs

# Open the Google Sheet we'll be reading and writing to
lp_landscape = gc.open('DeFi Landscape LP Opportunities_v2')

# Select the 'Project Ratings' tab and read the data into a dataframe
project_ratings_sheet = lp_landscape.worksheet("Project Ratings")
lp_project_ratings = project_ratings_sheet.get_all_records()
lp_project_ratings_df = pd.DataFrame(lp_project_ratings)

# Select the 'Stables' tab and read the data into a dataframe
stables_sheet = lp_landscape.worksheet("Stables")
lp_stables = stables_sheet.get_all_records()
lp_stables_df = pd.DataFrame(lp_stables)

# Select 'Pool Yields' tab
pool_yields_sheet = lp_landscape.worksheet("Pool Yields")

# Select 'Protocol Historicals' tab
protocol_historicals_sheet = lp_landscape.worksheet("Protocol Historicals")

# Select 'Pool Historicals' tab
pool_historicals_sheet = lp_landscape.worksheet("Pool Historicals")

# Select 'Protocol Info' tab
protocol_info_sheet = lp_landscape.worksheet("Protocol Info")

# Select 'Historical Chain TVL' tab
historical_chain_tvl_sheet = lp_landscape.worksheet("Historical Chain TVL")



# 2.1. Update data in 'Project Ratings' tab

# Extract protocol names from the worksheet as a list in the format used by Defillama API (slug)
protocol_slugs = lp_project_ratings_df["API Protocol Name"].tolist()

# 2.1.1. Update current protocol TVLs

# Get current TVLs for the list of protocols and return them as a list
protocol_tvls = [requests.get(tvl_base_url + '/tvl/' + protocol).json() for protocol in protocol_slugs]

# Format numbers as millions before writing to spreadsheet
protocol_tvls_m = [int(tvl) / 1000000 for tvl in protocol_tvls]

# Write results stored in protocol_tvls to 'Current TVL ($m)' in 'Project Ratings' tab to update current protocol TVLs
write_to_column(project_ratings_sheet, "C2", protocol_tvls_m)

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

protocol_tvl_deltas



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




# 2.3 Update 'Pool Historicals' raw data tab"""

# Extract pool ID's from the worksheet as a list
pool_ids = lp_stables_df["API pool id"].tolist()

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





# 2.4. Update 'Protocol Info' raw data tab

# Get all protocols
protocols = requests.get(tvl_base_url + '/protocols')

# Convert Protocols response to data frame
protocols_df = pd.DataFrame(protocols.json())




# 2.5. Update 'Historical Chain TVL' raw data tab

# Get historical TVL data for Ethereum
eth_tvl = requests.get(tvl_base_url + '/v2/historicalChainTvl/Ethereum')

# Convert response to data frame
eth_tvl_df = pd.DataFrame(eth_tvl.json())

# Convert the 'Date' column from UNIX timestamp to datetime format
eth_tvl_df['date'] = pd.to_datetime(eth_tvl_df['date'], unit='s')
eth_tvl_df['date'] = eth_tvl_df['date'].dt.date

# Order by date in reverse chronological order
eth_tvl_df = eth_tvl_df.sort_values(by='date', ascending=False)




# 2.6. Update data in 'LP Update Historicals' tab for LP Landscape Update distribution charts

# Create dictionary to associate all pool ids to their historical data
all_pools_dict = dict(zip(pool_ids, pool_historicals_dfs))

# Select the 'eUSD Curve Comps' tab and read the data into a dataframe
eusd_curve_sheet = lp_landscape.worksheet("eUSD Curve Comps")
lp_eusd_curve = eusd_curve_sheet.get_all_records()
lp_eusd_curve_df = pd.DataFrame(lp_eusd_curve)

# Select 'LP Update Historicals' tab
lp_update_historicals_sheet = lp_landscape.worksheet("LP Update Historicals")

# Extract pool names from the worksheet as a list
eusd_peer_pool_ids = lp_eusd_curve_df["API pool id"].tolist()

# MIM-3CRV pool id
mim_3crv_pool_id = "8a20c472-142c-4442-b724-40f2183c073e"

# Remove MIM-3CRV pool from list if present
if mim_3crv_pool_id in eusd_peer_pool_ids: eusd_peer_pool_ids.remove(mim_3crv_pool_id)

# Keep only data for pools contained in pool id list
eusd_curve_peer_pools = {k: all_pools_dict[k] for k in eusd_peer_pool_ids}

# Separate values (dataframes) from keys (pool id's) before concatenating
eusd_curve_peer_pools_dfs = list(eusd_curve_peer_pools.values())

# Concatenate every dataframe in the list into one dataframe
eusd_curve_peer_pools_df = pd.concat(eusd_curve_peer_pools_dfs, axis = 1, sort=False)



# 2.7. Aggregate historical data to create indices for RToken peer groups

# Select the 'hyUSD Comps' tab and read the data into a dataframe
hyusd_sheet = lp_landscape.worksheet("hyUSD Comps")
lp_hyusd = hyusd_sheet.get_all_records()
lp_hyusd_df = pd.DataFrame(lp_hyusd)

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

# Extract pool names from the worksheet as a list
hyusd_peer_pool_ids = lp_hyusd_df["API pool id"].tolist()

# Remove hyUSD from list
if hyusd_pool_id in hyusd_peer_pool_ids: hyusd_peer_pool_ids.remove(hyusd_pool_id)

# Aggregate historicals for peer pools and store mean values as dataframe
hyusd_peers_df = aggregate_historicals(all_pools_dict, hyusd_peer_pool_ids)
