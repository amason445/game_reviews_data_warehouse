import logging
from datetime import datetime
import toml
import requests
import utilities
import pyodbc

logging.basicConfig(filename='logging\\stores.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

config = toml.load("config.toml")

def initial_scrape():

    #variables store results space and logging information
    stores_scrape_results_list = []

    #sets API request limit for pagination feature
    page_limit = 10
    request_count = 0

    URL = f"https://api.rawg.io/api/stores?key={config['APIkeys']['rawgio_key']}&limit=50"

    try:
        stores_request = requests.get(URL).json()
        
        for i in range(0, len(stores_request['results'])):
            stores_scrape_results_list.append(stores_request['results'][i])
            
            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0


        next_page = stores_request['next']
    
    except Exception as e:
        logging.error(f"Initial request for failed:\n{e}")

    for i in range(0, page_limit):

        try:
            stores_request = requests.get(next_page).json()

            for j in range(0, len(stores_request['results'])):
                stores_scrape_results_list.append(stores_request['results'][j])

            logging.info(f"URL {next_page} was successful")

            request_count += 1
            
            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0

            next_page = stores_request['next']

        except Exception as e:
            logging.error(f"Request failed:\n{e}")


    logging.info(f"Initial stores request was successful.\nCount of stores: {len(stores_scrape_results_list)}\n")
        
    #write intial extract to temporary JSON file using utilities.py
    utilities.write_to_json(stores_scrape_results_list, config['JSONarchive']['stores_extract'])

def transform_to_load_set(scrape_timestamp: datetime):

    #read initial extract from temporary JSON file using utilities.py
    stores_extract = utilities.read_from_json(config['JSONarchive']['stores_extract'])
    games_extract = utilities.read_from_json(config['JSONarchive']['games_extract'])

    stores_properties_transformation_results_list = []
    stores_bridge_transformation_results_list = []
        
    for i in range(0, len(stores_extract)):
        try:
            target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                'StoreId': stores_extract[i]['id'],
                                'StoreName': stores_extract[i]['name'],
                                'StoreCount': stores_extract[i]['games_count']
                                }
            
            stores_properties_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {stores_extract[i]['id']} has failed. Exception:\n {e}")

    for i in range(0, len(games_extract)):

        try:

            stores_mapping_list = games_extract[i]['stores']

            for j in range(0, len(stores_mapping_list)):
                    target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                        'StoreGameKey': str(games_extract[i]['id']) + '-' + str(stores_mapping_list[j]['store']['id']),
                                        'GameId': str(games_extract[i]['id']),
                                        'StoreId': str(stores_mapping_list[j]['store']['id'])
                                        }
                
            stores_bridge_transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {games_extract[i]['id']} has failed. Exception:\n {e}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(stores_properties_transformation_results_list, config['JSONarchive']['stores_properties_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Platform properties transformation is complete.\nNumber of Records: {len(stores_properties_transformation_results_list)}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(stores_bridge_transformation_results_list, config['JSONarchive']['stores_bridge_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Platform bridge transformation is complete.\nNumber of Records: {len(stores_bridge_transformation_results_list)}")


def load_to_database():
    
    stores_properties_filtered = utilities.read_from_json(config['JSONarchive']['stores_properties_filtered'])
    stores_bridge_filtered = utilities.read_from_json(config['JSONarchive']['stores_bridge_filtered'])

    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    load_count = 0

    for i in range(0, len(stores_properties_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_StoresTable (StoreId, StoreName, StoreCount, ScrapeDate)
            VALUES (
                {int(stores_properties_filtered[i]['StoreId'])},
                {"'" + str(stores_properties_filtered[i]['StoreName']).replace("'", "''") + "'"},
                {int(stores_properties_filtered[i]['StoreCount'])},
                {"'" + str(stores_properties_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {stores_properties_filtered[i]['StoreId']}. Exception:\n {e}\nQuery:\n {query}")


    for i in range(0, len(stores_bridge_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_StoresBridgeTable (StoreGameKey, StoreId, GameId, ScrapeDate)
            VALUES (
                {"'" + str(stores_bridge_filtered[i]['StoreGameKey']) + "'"},
                {int(stores_bridge_filtered[i]['StoreId'])},
                {int(stores_bridge_filtered[i]['GameId'])},
                {"'" + str(stores_bridge_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {stores_bridge_filtered[i]['StoreId']}. Exception:\n {e}\nQuery:\n {query}")


    logging.info(f"Load to database is complete. {load_count} records were successfully loaded!")

    crsr.commit()
    conn.close

if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape()
    transform_to_load_set(scrape_timestamp)
    load_to_database()