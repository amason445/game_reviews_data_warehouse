'''
This script accesses the Rawg.io genres endpoint as JSON objects, transforms them and then loads them to a Microsoft SQL Server Database.
Each ETL stage is stored in functions and then accessed by the main function at the bottom of the script.
End point: https://api.rawg.io/docs/#tag/genres
'''

import logging
from datetime import datetime
import toml
import requests
import utilities
import pyodbc

#set log file configuration using Python's logging library
logging.basicConfig(filename='logging\\genre.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

#path to toml config file
config = toml.load("config.toml")

def initial_scrape():

    #variables store results space and logging information
    genre_scrape_results_list = []

    #sets API request limit for pagination feature
    page_limit = 10
    request_count = 0

    #target URL for Python requests library
    URL = f"https://api.rawg.io/api/genres?key={config['APIkeys']['rawgio_key']}&limit=50"

    try:
        genre_request = requests.get(URL).json()
        
        for i in range(0, len(genre_request['results'])):
            genre_scrape_results_list.append(genre_request['results'][i])
            
            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0


        next_page = genre_request['next']
    
    except Exception as e:
        logging.error(f"Initial request for failed:\n{e}")

    for i in range(0, page_limit):

        try:
            genre_request = requests.get(next_page).json()

            for j in range(0, len(genre_request['results'])):
                genre_scrape_results_list.append(genre_request['results'][j])

            logging.info(f"URL {next_page} was successful")

            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0

            next_page = genre_request['next']

        except Exception as e:
            logging.error(f"Request failed:\n{e}")


    logging.info(f"Initial genre request was successful.\nCount of genre: {len(genre_scrape_results_list)}\n")
        
    #write intial extract to temporary JSON file using utilities.py
    utilities.write_to_json(genre_scrape_results_list, config['JSONarchive']['genre_extract'])

def transform_to_load_set(scrape_timestamp: datetime):

    #read initial extract from temporary JSON files using utilities.py
    genre_extract = utilities.read_from_json(config['JSONarchive']['genre_extract'])
    games_extract = utilities.read_from_json(config['JSONarchive']['games_extract'])

    genre_properties_transformation_results_list = []
    genre_bridge_transformation_results_list = []
        
    for i in range(0, len(genre_extract)):
        try:
            target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                'GenreId': genre_extract[i]['id'],
                                'GenreName': genre_extract[i]['name'],
                                'GenreCount': genre_extract[i]['games_count']
                                }
            
            genre_properties_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {genre_extract[i]['id']} has failed. Exception:\n {e}")

    for i in range(0, len(games_extract)):

        genre_mapping_list = games_extract[i]['genres']

        try:

            for j in range(0, len(genre_mapping_list)):
                    target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                        'GenreGameKey': str(games_extract[i]['id']) + '-' + str(genre_mapping_list[j]['id']),
                                        'GameId': str(games_extract[i]['id']),
                                        'GenreId': str(genre_mapping_list[j]['id'])
                                        }
                
            genre_bridge_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {games_extract[i]['id']} has failed. Exception:\n {e}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(genre_properties_transformation_results_list, config['JSONarchive']['genre_properties_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Genre properties transformation is complete.\nNumber of Records: {len(genre_properties_transformation_results_list)}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(genre_bridge_transformation_results_list, config['JSONarchive']['genre_bridge_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Genre bridge transformation is complete.\nNumber of Records: {len(genre_properties_transformation_results_list)}")


def load_to_database():
    
    genre_properties_filtered = utilities.read_from_json(config['JSONarchive']['genre_properties_filtered'])
    genre_bridge_filtered = utilities.read_from_json(config['JSONarchive']['genre_bridge_filtered'])

    #reads connection string from config file using pyodbc and connects to database
    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    load_count = 0

    for i in range(0, len(genre_properties_filtered)):

        try:

            #insert query to load raw data into database
            query = f"""

            INSERT INTO STAGE.dim_GenreTable (GenreId, GenreName, GenreCount, ScrapeDate)
            VALUES (
                {int(genre_properties_filtered[i]['GenreId'])},
                {"'" + str(genre_properties_filtered[i]['GenreName']).replace("'", "''") + "'"},
                {int(genre_properties_filtered[i]['GenreCount'])},
                {"'" + str(genre_properties_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {genre_properties_filtered[i]['GenreId']}. Exception:\n {e}\nQuery:\n {query}")


    for i in range(0, len(genre_bridge_filtered)):

        try:

            #insert query to load raw data into database
            query = f"""

            INSERT INTO STAGE.dim_GenreBridgeTable (GenreGameKey, GenreId, GameId, ScrapeDate)
            VALUES (
                {"'" + str(genre_bridge_filtered[i]['GenreGameKey'])  + "'"},
                {int(genre_bridge_filtered[i]['GenreId'])},
                {int(genre_bridge_filtered[i]['GameId'])},
                {"'" + str(genre_bridge_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {genre_bridge_filtered[i]['GenreId']}. Exception:\n {e}\nQuery:\n {query}")


    logging.info(f"Load to database is complete. {load_count} records were successfully loaded!")

    crsr.commit()
    conn.close

#calls each of the above functions to complete ETL
if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape()
    transform_to_load_set(scrape_timestamp)
    load_to_database()