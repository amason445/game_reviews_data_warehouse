import logging
from datetime import datetime
import toml
import requests
import utilities
import pyodbc

logging.basicConfig(filename='logging\\developers.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

config = toml.load("config.toml")

def initial_scrape():

    #variables store results space and logging information
    results_list = []
    game_count = 0

    #sets API request limit for pagination feature
    page_limit = 10

    URL = f"https://api.rawg.io/api/developers?key={config['APIkeys']['rawgio_key']}&page_size=50"

    try:
        developers_request = requests.get(URL).json()
        
        for i in range(0, len(developers_request['results'])):
            results_list.append(developers_request['results'][i])
            game_count += developers_request['results'][i]['games_count']

        next_page = developers_request['next']
    
    except Exception as e:
        logging.error(f"Initial request failed:\n{e}")

    for i in range(0, page_limit):

        try:
            developers_request = requests.get(next_page).json()

            for i in range(0, len(developers_request['results'])):
                results_list.append(developers_request['results'][i])
                game_count += developers_request['results'][i]['games_count']

            logging.info(f"URL {next_page} was successful")

            next_page = developers_request['next']

        except Exception as e:
            logging.error(f"Request failed:\n{e}")


    logging.info(f"Initial developer request was successful.\nCount of developers: {len(results_list)}\nCount of scrapeable games: {game_count}")
        
    
    #write intial extract to temporary JSON file using utilities.py
    utilities.write_to_json(results_list, config['JSONarchive']['developer_extract'])

def transform_to_load_set(scrape_timestamp: datetime):

    #read initial extract from temporary JSON file using utilities.py
    developer_extract = utilities.read_from_json(config['JSONarchive']['developer_extract'])

    transformation_results_list = []
        
    for i in range(0, len(developer_extract)):
        try:
            target_dictionary = {'ScrapeTimestamp': str(scrape_timestamp),
                                'DeveloperId': developer_extract[i]['id'],
                                'DeveloperName': developer_extract[i]['name'],
                                'DeveloperCount': developer_extract[i]['games_count']}
            
            transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {developer_extract[i]['id']} has failed. Exception:\n {e}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(transformation_results_list, config['JSONarchive']['developer_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Developer Transformation is complete.\nNumber of Records: {len(transformation_results_list)}")

def load_to_database():
    
    developer_filtered = utilities.read_from_json(config['JSONarchive']['developer_filtered'])
    
    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    load_count = 0

    for i in range(0, len(developer_filtered)):

        try:

            query = f"""

            INSERT INTO STAGE.dim_DeveloperTable (DeveloperId, DeveloperName, DeveloperCount, ScrapeDate)
            VALUES (
                {int(developer_filtered[i]['DeveloperId'])},
                {"'" + str(developer_filtered[i]['DeveloperName']).replace("'", "''") + "'"},
                {int(developer_filtered[i]['DeveloperCount'])},
                {"'" + str(developer_filtered[i]['ScrapeTimestamp']) + "'"})

            """

            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {developer_filtered[i]['DeveloperId']}. Exception:\n {e}\nQuery:\n {query}")

    logging.info(f"Load to database is complete. {load_count} records were successfully loaded!")

    crsr.commit()
    conn.close

if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape()
    transform_to_load_set(scrape_timestamp)
    load_to_database()