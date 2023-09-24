import logging
from datetime import datetime
import toml
import requests
import utilities
import pyodbc

logging.basicConfig(filename='logging\\parent_platforms.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

config = toml.load("config.toml")

def initial_scrape():

    #variables store results space and logging information
    parent_scrape_results_list = []

    #sets API request limit for pagination feature
    page_limit = 10
    request_count = 0

    URL = f"https://api.rawg.io/api/platforms/lists/parents?key={config['APIkeys']['rawgio_key']}&limit=50"

    try:
        parent_request = requests.get(URL).json()
        
        for i in range(0, len(parent_request['results'])):
            parent_scrape_results_list.append(parent_request['results'][i])
            
            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0


        next_page = parent_request['next']
    
    except Exception as e:
        logging.error(f"Initial request for failed:\n{e}")

    for i in range(0, page_limit):

        try:
            parent_request = requests.get(next_page).json()

            for j in range(0, len(parent_request['results'])):
                parent_scrape_results_list.append(parent_request['results'][j])

            logging.info(f"URL {next_page} was successful")

            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0

            next_page = parent_request['next']

        except Exception as e:
            logging.error(f"Request failed:\n{e}")


    logging.info(f"Initial parent platform request was successful.\nCount of genre: {len(parent_scrape_results_list)}\n")
        
    
    #write intial extract to temporary JSON file using utilities.py
    utilities.write_to_json(parent_scrape_results_list, config['JSONarchive']['parent_platform_extract'])

def transform_to_load_set(scrape_timestamp: datetime):

    #read initial extract from temporary JSON file using utilities.py
    parent_platform_extract = utilities.read_from_json(config['JSONarchive']['parent_platform_extract'])
    games_extract = utilities.read_from_json(config['JSONarchive']['games_extract'])

    parent_platform_properties_transformation_results_list = []
    parent_platform_bridge_transformation_results_list = []
        
    for i in range(0, len(parent_platform_extract)):
        try:
            target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                'ParentPlatformId': parent_platform_extract[i]['id'],
                                'ParentPlatformName': parent_platform_extract[i]['name'],
                                }
            
            parent_platform_properties_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {parent_platform_extract[i]['id']} has failed. Exception:\n {e}")

    for i in range(0, len(games_extract)):

        try:

            parent_mapping_list = games_extract[i]['parent_platforms']

            for j in range(0, len(parent_mapping_list)):
                    target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                        'ParentPlatformGameKey': str(games_extract[i]['id']) + '-' + str(parent_mapping_list[j]['platform']['id']),
                                        'GameId': str(games_extract[i]['id']),
                                        'ParentPlatformId': str(parent_mapping_list[j]['platform']['id'])
                                        }
                
            parent_platform_bridge_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {games_extract[i]['id']} has failed. Exception:\n {e}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(parent_platform_properties_transformation_results_list, config['JSONarchive']['parent_platform_properties_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Parent Platform properties transformation is complete.\nNumber of Records: {len(parent_platform_properties_transformation_results_list)}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(parent_platform_bridge_transformation_results_list, config['JSONarchive']['parent_platform_bridge_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Parent Platform bridge transformation is complete.\nNumber of Records: {len(parent_platform_bridge_transformation_results_list)}")


def load_to_database():
    
    parent_properties_filtered = utilities.read_from_json(config['JSONarchive']['parent_platform_properties_filtered'])
    parent_bridge_filtered = utilities.read_from_json(config['JSONarchive']['parent_platform_bridge_filtered'])

    
    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    load_count = 0

    for i in range(0, len(parent_properties_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_ParentPlatformTable (ParentPlatformId, ParentPlatformName, ScrapeDate)
            VALUES (
                {int(parent_properties_filtered[i]['ParentPlatformId'])},
                {"'" + str(parent_properties_filtered[i]['ParentPlatformName']).replace("'", "''") + "'"},
                {"'" + str(parent_properties_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {parent_properties_filtered[i]['ParentPlatformId']}. Exception:\n {e}\nQuery:\n {query}")


    for i in range(0, len(parent_bridge_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_ParentPlatformBridgeTable (ParentPlatformGameKey, ParentPlatformId, GameId, ScrapeDate)
            VALUES (
                {"'" + str(parent_bridge_filtered[i]['ParentPlatformGameKey'])  + "'"},
                {int(parent_bridge_filtered[i]['ParentPlatformId'])},
                {int(parent_bridge_filtered[i]['GameId'])},
                {"'" + str(parent_bridge_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {parent_bridge_filtered[i]['ParentPlatformId']}. Exception:\n {e}\nQuery:\n {query}")


    logging.info(f"Load to database is complete. {load_count} records were successfully loaded!")

    crsr.commit()
    conn.close

if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape()
    transform_to_load_set(scrape_timestamp)
    load_to_database()