import logging
from datetime import datetime
import toml
import requests
import utilities

logging.basicConfig(filename='logging\\developers.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

def initial_scrape(scrape_timestamp):

    #variables store results space and logging information
    results_list = []
    game_count = 0

    #sets API request limit for pagination feature
    page_limit = 10

    config = toml.load("config.toml")
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
        
    
    #write intial extract to temporary JSON file
    utilities.write_to_json(results_list, config['JSONarchive']['developer_extract'])

    



if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape(scrape_timestamp)