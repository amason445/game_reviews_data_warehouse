import logging
from datetime import datetime
import toml
import requests
import utilities
import pyodbc

logging.basicConfig(filename='logging\\games.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

config = toml.load("config.toml")

def initial_scrape():

    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    try:
        query = f"""
                WITH max_date AS (

                    SELECT
                    DeveloperId,
                    RANK() OVER(ORDER BY ScrapeDate DESC) as max_date

                    FROM STAGE.dim_DeveloperTable
                )

                SELECT DISTINCT DeveloperId

                    FROM max_date

                    WHERE max_date = 1
                """

        crsr.execute(query)
        sieve_results = crsr.fetchall()

        idUniverse = [row[0] for row in sieve_results]

    except Exception as e:
        logging.error(f"Sieve query failed. Exception:\n {e}")

    conn.close()

    #variables store results space and logging information
    game_scrape_results_list = []

    #sets API request limit for pagination feature
    page_limit = 40
    request_count = 0
    cumulative_request_count = 0

    for i in range(0, len(idUniverse)):

        URL = f"https://api.rawg.io/api/games?key={config['APIkeys']['rawgio_key']}&developers={idUniverse[i]}&limit=50&ordering=released"

        try:
            developers_games_request = requests.get(URL).json()

            if 'results' not in developers_games_request:
                logging.info(f"Developer Id {idUniverse[i]} does not have game results available")
                continue
            
            for game_dict in range(0, len(developers_games_request['results'])):
                developers_games_request['results'][game_dict].update({'DeveloperId': idUniverse[i]})
                game_scrape_results_list.append(developers_games_request['results'][game_dict])

            request_count += 1
            cumulative_request_count += 1

            if request_count > 100:
                utilities.request_break(request_count)
                request_count = 0
            
            logging.info(f"Total Requests: {cumulative_request_count}")

            next_page = developers_games_request['next']
        
        except Exception as e:
            logging.error(f"Initial request for {idUniverse[i]} failed:\n{e}")
        
        for j in range(0, page_limit):

            try:
                developers_games_request = requests.get(next_page).json()

                for game_dict in range(0, len(developers_games_request['results'])):
                    developers_games_request['results'][game_dict].update({'DeveloperId': idUniverse[i]})
                    game_scrape_results_list .append(developers_games_request['results'][game_dict])
              
                request_count += 1
                cumulative_request_count += 1

                if request_count > 100:
                    utilities.request_break(request_count)
                    request_count = 0

                logging.info(f"URL {next_page} was successful. Total Requests: {cumulative_request_count}")

                if developers_games_request['next'] is None:
                    logging.info(f"Id has no further pagination {idUniverse[i]}")
                    break

                next_page = developers_games_request['next']

            except Exception as e:
                logging.error(f"Request for {idUniverse[i]} failed:\n{e}")
                break

    logging.info(f"Initial developer request was successful.\nCount of games: {len(game_scrape_results_list)}")

    #write intial extract to temporary JSON file using utilities.py
    utilities.write_to_json(game_scrape_results_list, config['JSONarchive']['games_extract'])

def transform_to_load_set(scrape_timestamp: datetime):

    #read initial extract from temporary JSON file using utilities.py
    developer_games_extract = utilities.read_from_json(config['JSONarchive']['games_extract'])

    transformation_results_list = []
        
    for i in range(0, len(developer_games_extract)):
        try:
            target_dictionary = {
                'GameId': developer_games_extract[i]['id'],
                'GameTitle': developer_games_extract[i]['name'],
                'ReleaseDate':developer_games_extract[i]['released'],
                'DeveloperId': developer_games_extract[i]['DeveloperId'],
                'RawgIO_Rating': developer_games_extract[i]['rating'],
                'RawgRatingsCount': developer_games_extract[i]['ratings_count'],
                'MetacriticScore': developer_games_extract[i]['metacritic'],
                'ScrapeDate': str(scrape_timestamp)
            }
            
            transformation_results_list.append(target_dictionary)

        except Exception as e:
            logging.error(f"Transformation of ID {developer_games_extract[i]['id']} has failed. Exception:\n {e}")

    #write intial extract to temporary JSON file using utitilities.py
    utilities.write_to_json(transformation_results_list, config['JSONarchive']['games_filtered'])

    #write intial extract to temporary JSON file using utilities.py
    logging.info(f"Developer Transformation is complete.\nNumber of Records: {len(transformation_results_list)}")

def load_to_database():
    
    games_filtered = utilities.read_from_json(config['JSONarchive']['games_filtered'])
    
    conn = pyodbc.connect(config['Database']['connection_string'])
    crsr = conn.cursor()

    load_count = 0

    for i in range(0, len(games_filtered)):

        try:
            query = f"""
            
                INSERT INTO STAGE.fact_GameReviews (
                    GameId, 
                    GameTitle, 
                    ReleaseDate,
                    DeveloperId,
                    RawgIO_Rating,
                    RawgRatingsCount,
                    MetacriticScore, 
                    ScrapeDate
                    )
                VALUES (
                    {int(games_filtered[i]['GameId'])},
                    {"'" + str(games_filtered[i]['GameTitle']).replace("'", "''") + "'"},
                    {"'" + str(games_filtered[i]['ReleaseDate']) + "'"},
                    {int(games_filtered[i]['DeveloperId'])},
                    {float(games_filtered[i]['RawgIO_Rating'])},
                    {int(games_filtered[i]['RawgRatingsCount'])},
                    {"'" + str(games_filtered[i]['MetacriticScore']) + "'"},
                    {"'" + str(games_filtered[i]['ScrapeDate']) + "'"}  
                    )
            
            """
            crsr.execute(query)
            load_count += 1

        except Exception as e:
            logging.error(f"Issue loading ID: {games_filtered[i]['GameId']}. Exception:\n {e}\nQuery:\n {query}")

    logging.info(f"Load to database is complete. {load_count} records were successfully loaded!")

    crsr.commit()
    conn.close

if __name__ == "__main__":
    scrape_timestamp = datetime.now()
    initial_scrape()
    transform_to_load_set(scrape_timestamp)
    load_to_database()