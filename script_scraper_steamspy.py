import pandas as pd

from module_scraper import *
from settings import *

url_steamspy = STEAMSPY_URL
params_steamspy = STEAMSPY_PARAMS

# It is assumed that there is an applist scraped
# You can find code to get app_list in script_scraper_steam.py in current folder
app_list = pd.read_csv('./data/download/app_list.csv')

# Define download paths and filenames
download_path = './data/download'
steam_data_filename = 'steamspy_app_data.csv'
steam_index = 'steamspy_index.txt'

# Define columns to retrieve
steamspy_columns = [
    'appid', 'name', 'developer', 'publisher', 'score_rank', 'positive',
    'negative', 'userscore', 'owners', 'average_forever', 'average_2weeks',
    'median_forever', 'median_2weeks', 'price', 'initialprice', 'discount',
    'languages', 'genre', 'ccu', 'tags'
]

# Overwrites last index for demonstration (would usually store highest index so can continue across sessions)
# reset_index(download_path, steam_index)
# Retrieve last index downloaded from file
index = get_index(download_path, steam_index)
# Wipe or create data file and write headers if index is 0
prepare_data_file(download_path, steam_data_filename, index, steamspy_columns)
# Set end and chunksize for demonstration - remove to run through entire app list
process_batches(
    parser=parse_steamspy_request,
    app_list=app_list,
    download_path=download_path,
    data_filename=steam_data_filename,
    index_filename=steam_index,
    columns=steamspy_columns,
    begin=index,
    end=64573,
    batchsize=50,
    pause=0.3
)
