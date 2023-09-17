import logging
from datetime import datetime
import toml
import requests
import utilities
import pyodbc

logging.basicConfig(filename='logging\\platforms.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

config = toml.load("config.toml")

def initial_scrape():

    #variables store results space and logging information
    platforms_scrape_results_list = []

    #sets API request limit for pagination feature
    page_limit = 10
    request_count = 0

    URL = f"https://api.rawg.io/api/platforms?key={config['APIkeys']['rawgio_key']}&limit=50"

    try:
        platform_request = requests.get(URL).json()
        
        for i in range(0, len(platform_request['results'])):
            platforms_scrape_results_list.append(platform_request['results'][i])
            
            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0


        next_page = platform_request['next']
    
    except Exception as e:
        logging.error(f"Initial request for failed:\n{e}")

    for i in range(0, page_limit):

        try:
            platform_request = requests.get(next_page).json()

            for j in range(0, len(platform_request['results'])):
                platforms_scrape_results_list.append(platform_request['results'][j])

            logging.info(f"URL {next_page} was successful")

            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0

            next_page = platform_request['next']

        except Exception as e:
            logging.error(f"Request failed:\n{e}")


    logging.info(f"Initial platform request was successful.\nCount of platforms: {len(platforms_scrape_results_list)}\n")
        
    
    #write intial extract to temporary JSON file using utilities.py
    utilities.write_to_json(platforms_scrape_results_list, config['JSONarchive']['platform_extract'])

def transform_to_load_set(scrape_timestamp: datetime):

    #read initial extract from temporary JSON file using utilities.py
    platform_extract = utilities.read_from_json(config['JSONarchive']['platform_extract'])
    games_extract = utilities.read_from_json(config['JSONarchive']['games_extract'])

    platform_properties_transformation_results_list = []
    platform_bridge_transformation_results_list = []
        
    for i in range(0, len(platform_extract)):
        try:
            target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                'PlatformId': platform_extract[i]['id'],
                                'PlatformName': platform_extract[i]['name'],
                                'PlatformCount': platform_extract[i]['games_count']
                                }
            
            platform_properties_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {platform_extract[i]['id']} has failed. Exception:\n {e}")

    for i in range(0, len(games_extract)):

        try:

            platform_mapping_list = games_extract[i]['platforms']

            for j in range(0, len(platform_mapping_list)):
                    target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                        'PlatformGameKey': str(games_extract[i]['id']) + '-' + str(platform_mapping_list[j]['platform']['id']),
                                        'GameId': str(games_extract[i]['id']),
                                        'PlatformId': str(platform_mapping_list[j]['platform']['id'])
                                        }
                
            platform_bridge_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {games_extract[i]['id']} has failed. Exception:\n {e}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(platform_properties_transformation_results_list, config['JSONarchive']['platform_properties_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Platform properties transformation is complete.\nNumber of Records: {len(platform_bridge_transformation_results_list)}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(platform_bridge_transformation_results_list, config['JSONarchive']['platform_bridge_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Platform bridge transformation is complete.\nNumber of Records: {len(platform_bridge_transformation_results_list)}")


def load_to_database():
    
    platform_properties_filtered = utilities.read_from_json(config['JSONarchive']['platform_properties_filtered'])
    platform_bridge_filtered = utilities.read_from_json(config['JSONarchive']['platform_bridge_filtered'])

    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    load_count = 0

    for i in range(0, len(platform_properties_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_PlatformTable (PlatformId, PlatformName, PlatformCount, ScrapeDate)
            VALUES (
                {int(platform_properties_filtered[i]['PlatformId'])},
                {"'" + str(platform_properties_filtered[i]['PlatformName']).replace("'", "''") + "'"},
                {int(platform_properties_filtered[i]['PlatformCount'])},
                {"'" + str(platform_properties_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {platform_properties_filtered[i]['PlatformId']}. Exception:\n {e}\nQuery:\n {query}")


    for i in range(0, len(platform_bridge_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_PlatformBridgeTable (PlatformGameKey, PlatformId, GameId, ScrapeDate)
            VALUES (
                {"'" + str(platform_bridge_filtered[i]['PlatformGameKey']) + "'"},
                {int(platform_bridge_filtered[i]['PlatformId'])},
                {int(platform_bridge_filtered[i]['GameId'])},
                {"'" + str(platform_bridge_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {platform_bridge_filtered[i]['PlatformId']}. Exception:\n {e}\nQuery:\n {query}")


    logging.info(f"Load to database is complete. {load_count} records were successfully loaded!")

    crsr.commit()
    conn.close

if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape()
    transform_to_load_set(scrape_timestamp)
    load_to_database()