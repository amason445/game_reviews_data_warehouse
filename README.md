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

## References 
Gupta, L. (2022, April 7). *What is rest.* REST API Tutorial. https://restfulapi.net/ 
Postman. (n.d.). *Postman API Platform.* https://www.postman.com/ 
Rawg.io. *The biggest video game database on RAWG - video game Discovery Service. The Biggest Video Game Database on RAWG - Video Game Discovery Service.* (n.d.). https://rawg.io/ 
Rawg.io. *Explore RAWG Video Games Database API - RAWG.* RAWG. (n.d.). https://rawg.io/apidocs

 
