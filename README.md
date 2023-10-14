# Video Game Data Warehouse Project

## Project Summary
This repository contains an academic project that I build for a capstone course during my Master of Data Science program at Regis University. This project contains ETL and SQL for a data warehouse I built using data from Rawg.io's public API. The final product is a star schema I built using their video game reviews and meta data. I also wrote SQL Views with this data warehouse and connected to Microsoft's PowerBI for visualizations. This repository contains artifacts from the project including the source code, raw data and sample visualizations. It also contains documentation explaining the database architecture and ETL process.

## Technology Used
- Postman
- Python
- Microsoft SQL Server
- Microsoft PowerBI
- Microsoft Excel

## Data Source: Rawg.io
Rawg.io is a large, public database that collects and maintains information about video games and video game ratings. Rawg.io also provides a publicly available API which I used to scrape this data (Rawg.io, 2023). The API follows the REST architecture, uses HTTP requests and returns JSON Objects (Gupta, 2022). For this project, I accessed five end points Rawg.io wrote for their API:

- [Developer End Point](https://api.rawg.io/docs/#tag/developers)
- [Games End Point](https://api.rawg.io/docs/#tag/games)
- [Genre End Point](https://api.rawg.io/docs/#tag/genres)
- [Platform End Point](https://api.rawg.io/docs/#tag/platforms)
- [Stores End Point](https://api.rawg.io/docs/#tag/stores)

Each endpoint was accessed with it's own Python script and analysis had to be done on each endpoint with Postman and Python to extract the relevant fields. Postman is a free service that allows users to test individual API requests (Postman, 2023). Once the structure was analyzed, Python scripts were written to do a patch extraction on each end point.

## ETL Process
The ETL processes leverages Python to extract the relevant data from each end point and load it into staging tables in Microsoft SQL Server. First, the JSON is scraped and transformed in intermediary steps. These steps are stored on local flat files. Once the data is loaded, a Stored Procedure written in SQL can be used to normalize and load the data to the final landing area in SQL Server. Once it is normalized, this data warehouse can be accessed with SQL for further analysis. 

Right now, the Python is not bundled into a scheduler so each end point must be run manually. Additionally, the game end point is dependent on the developers being scraped and loaded first. The ETL was designed this was to insure every develop has their complete history of games loaded into the data warehouse. Below is a screenshot of the ETL process.

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/ETL%20Process.png)

Finally, all of the SQL is stored in the folder SQL - DDL including the table defintions, the stored procedure, test queries and views for analysis. Additionally, the Python relies on a configuration file that is called "config.toml". This file contains important information such as where the intermediary JSON will be pathed to and the Rawg.io API key.

## Date Warehouse Architecture
The data warehouse architecture follows a typical star schema pattern (Databricks, 2023). However, a lot of bridge tables were needed because each end point is joined with many-to-many relationships. Below is a screenshot of the architecture:

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/Video%20Game%20Data%20Warehouse.png)

## References 
Databricks. What is star schema?. (n.d.). https://www.databricks.com/glossary/star-schema 
Gupta, L. (2022, April 7). *What is rest.* REST API Tutorial. https://restfulapi.net/ 
IBM. (2023, June 6). Bridge tables. https://www.ibm.com/docs/el/cognos-analytics/12.0.0?topic=relationships-bridge-tables 
Postman. (n.d.). *Postman API Platform.* https://www.postman.com/ 
Rawg.io. *The biggest video game database on RAWG - video game Discovery Service. The Biggest Video Game Database on RAWG - Video Game Discovery Service.* (n.d.). https://rawg.io/ 
Rawg.io. *Explore RAWG Video Games Database API - RAWG.* RAWG. (n.d.). https://rawg.io/apidocs



 
