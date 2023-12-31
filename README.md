# Regis University Practicum Project I - Video Game Review Aggregation and Data Warehouse

## Project Summary
This repository contains artifacts from an academic project that I built during my graduate program at Regis University. For this project, I built a data warehouse using data from Rawg.io's public API. Rawg.io is a large, public database that contains information about video games and their ratings. I also wrote views with this data warehouse and connected them Microsoft's PowerBI for visualization. The repository artifacts include the source code, raw data and sample visualizations and documentation explaining the database architecture and ETL process.

There were some project difficulties and roadblocks. I had a few bugs I had to fix throughout the project and I had to manage API resources properly. As of October 13, 2023, Rawg.io's free tier only allows 20,000 API requests per month. So, I had to carefully estimate API useage and cache intermediary data to avoid duplicating requests.

## Technology Used
- [Postman](https://www.postman.com/)
- Python
- Microsoft Excel
- [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server)
- [Microsoft PowerBI](https://powerbi.microsoft.com/en-us/)
- [Transact-SQL](https://learn.microsoft.com/en-us/sql/t-sql/language-reference?view=sql-server-ver16)


## Data Source: Rawg.io
Rawg.io is a large, public database that collects and maintains information about video games and video game ratings. Rawg.io also provides a publicly available API which I used to scrape this data (Rawg.io, 2023). The API follows the REST architecture, uses HTTP requests and returns JSON Objects (Gupta, 2022). For this project, I accessed five endpoints Rawg.io wrote for their API:

- [Developer End Point](https://api.rawg.io/docs/#tag/developers)
- [Games End Point](https://api.rawg.io/docs/#tag/games)
- [Genre End Point](https://api.rawg.io/docs/#tag/genres)
- [Platform End Point](https://api.rawg.io/docs/#tag/platforms)
- [Stores End Point](https://api.rawg.io/docs/#tag/stores)

Each endpoint was accessed with it's own Python script. Before they could be accessed, analysis had to be done on each endpoint with Postman to understand the JSON Structure extract target fields. Postman is a free service that allows users to test individual API requests (Postman, 2023). Once the API structure was analyzed, Python scripts were written to do a batch extraction on each endpoint. In order to use this API, it is mandatory to register for an API Key on the website.

## ETL Process and Source Code
The ETL processes leverages Python to extract the target data from each endpoint and load it into staging tables stored in Microsoft SQL Server. First, the JSON is scraped and transformed in intermediary steps stored on flat files. Then, it is loaded into SQL Server with pyodbc. Then, once the data is loaded, a Stored Procedure written in SQL can be used to normalize and load the data intto the final landing area in SQL Server. After normalization, this data warehouse can be accessed with SQL for further analysis. 

Right now, the Python is not bundled into a scheduler so each end point must be run manually. Additionally, the games end point is dependent on the developers being scraped and loaded first. The ETL was designed this was to insure every develop has their complete history of games loaded into the data warehouse. Below is a screenshot of the ETL process.

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/ETL%20Process.png)

Finally, the SQL is stored in the folder: SQL - DDL. This folder includes the table defintions, a query to get developer ids after they're loaded, the stored procedure, test queries and views I wrote for analysis. Additionally, the Python relies on a configuration file that is called: config.toml. This file contains important information such as where the intermediary JSON documents will be stored and the Rawg.io API key.

## Date Warehouse Architecture
The data warehouse architecture follows a typical star schema pattern (Databricks, 2023). However, a lot of bridge tables were needed because each end point is joined with many-to-many relationships (IBM, 2023). Below is a screenshot of the architecture:

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/Video%20Game%20Data%20Warehouse.png)

Additionally, there are four schemas that will need to be created in SQL Server:

- **STAGE**: This is the landing schema for the Python process. It only stores raw data
- **MAIN**: This schema contains the star schema. It is the final data warehouse
- **etl**: This schema contains the stored procedure that brings creates the main tables from the staging tables
- **POWER_BI** This schema contains analytical views written from the main tables

## Sample Data and Visualizations
I've included some sample output data and rough visualizations in a folder called: Sample Data and Visualizations.

Finally, I've also included some screenshots below of dashboards I've built in Microsoft's PowerBI:

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/PowerBI%20Dashboard%20Example%201.png)

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/PowerBI%20Dashboard%20Example%202.png)

![alt text](https://github.com/amason445/game_reviews_data_warehouse/blob/main/Reference%20Screenshots/PowerBI%20Dashboard%20Example%203.png)

## Future Ideas
- Bundle each endpoint Python script into a scheduler to have routine batch jobs
- Enrich the data warehouse with more diverse dimensional data
- Scrape additional endpoints

## References 
- Databricks. What is star schema?. (n.d.). https://www.databricks.com/glossary/star-schema 
- Gupta, L. (2022, April 7). *What is rest.* REST API Tutorial. https://restfulapi.net/ 
- IBM. (2023, June 6). Bridge tables. https://www.ibm.com/docs/el/cognos-analytics/12.0.0?topic=relationships-bridge-tables 
- Postman. (n.d.). *Postman API Platform.* https://www.postman.com/ 
- Rawg.io. *The biggest video game database on RAWG - video game Discovery Service. The Biggest Video Game Database on RAWG - Video Game Discovery Service.* (n.d.). https://rawg.io/ 
- Rawg.io. *Explore RAWG Video Games Database API - RAWG.* RAWG. (n.d.). https://rawg.io/apidocs



 
