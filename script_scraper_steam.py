import pandas as pd

from module_scraper import *
from settings import *

url_steamspy = STEAMSPY_URL
params_steamspy = STEAMSPY_PARAMS


# Create applist and export to csv
# Uncomment if you do not have app_list

# app_list = pd.DataFrame()
# start_page = 0
# end_page = 65
# for page in range(start_page, end_page):
#     url = f"{STEAMSPY_URL}?request=all&page={page}"
#     json_data = get_request(url, parameters=params_steamspy)
#     page_data = pd.DataFrame.from_dict(json_data, orient='index')
#     # generate sorted app_list from steamspy data
#     app_list = pd.concat([app_list, page_data])
#     print(f'Page {page}: {page_data.shape}')
#
# app_list.reset_index()
# app_list = app_list.rename(columns={'index': 'appid'})
# # turn off export if you have the file
# # app_list.to_csv('./data/download/app_list.csv', index=False)
# # instead read from stored csv

app_list = pd.read_csv('./data/download/app_list.csv')
# display first few rows
print(app_list.head())
print(app_list.shape)


download_path = './data/download'
steam_data_filename = 'steam_app_data.csv'
steam_index = 'steam_index.txt'

# Define columns for scraper to retrieve
steam_columns = [
    'type', 'name', 'steam_appid', 'required_age', 'is_free', 'controller_support',
    'dlc', 'detailed_description', 'about_the_game', 'short_description', 'fullgame',
    'supported_languages', 'header_image', 'website', 'pc_requirements', 'mac_requirements',
    'linux_requirements', 'legal_notice', 'drm_notice', 'ext_user_account_notice',
    'developers', 'publishers', 'demos', 'price_overview', 'packages', 'package_groups',
    'platforms', 'metacritic', 'reviews', 'categories', 'genres', 'screenshots',
    'movies', 'recommendations', 'achievements', 'release_date', 'support_info',
    'background', 'content_descriptors'
]

# Uncomment to reset index, and start scraping from first app
# reset_index(download_path, steam_index)
# Get last index to continue scrapping
index = get_index(download_path, steam_index)
# Wipe or create data file and write headers if index is 0
prepare_data_file(download_path, steam_data_filename, index, steam_columns)
# Set end and chunksize for demonstration - remove to run through entire app list
process_batches(
    parser=parse_steam_request,
    app_list=app_list,
    download_path=download_path,
    data_filename=steam_data_filename,
    index_filename=steam_index,
    columns=steam_columns,
    begin=index,
    end=64573,
    batchsize=50,
    pause=1
)


