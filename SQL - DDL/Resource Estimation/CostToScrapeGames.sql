/***	
This query runs off the developer dim table and was written to estimate API Resource Cost wen pulling game data.
AvgCost is any developer's games divided by the expected returned games for each page. It will cost one API request to scrape 15 games.
MinCost is a theoretical floor determined by the API page limit I set at 50 in the URL. API pages don't seem to reach 50 but any that do would introduce cost savings.
API Documentation: https://api.rawg.io/docs/
***/

USE GDW
GO

WITH resource_columns AS (

	SELECT DeveloperId
		  ,DeveloperName
		  ,DeveloperCount
		  ,DeveloperCount / 15 AS AvgCost
		  ,DeveloperCount / 50 AS MinCost
		  ,ScrapeDate
		  ,RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) AS ScrapeDateOrder
	FROM STAGE.dim_DeveloperTable

)

SELECT
COUNT(DISTINCT DeveloperId) AS DeveloperCount,
SUM(DeveloperCount) AS TotalGames,
MAX(DeveloperCount) AS MaxGamesForAnyDeveloper,
MIN(DeveloperCount) AS MinGamesForAnyDeveloper,
SUM(AvgCost) AS TotalCost,
MAX(AvgCost) AS MostExpensiveDeveloperCount,
MIN(AvgCost) AS LeastExpensiveDeveloperCount,
SUM(MinCost) AS TheoreticalCost,
MAX(MinCost) AS TheoreticalMostExpensiveDeveloperCount,
MIN(MinCost) AS TheoreticalLeastExpensiveDeveloperCount

FROM resource_columns

WHERE ScrapeDateOrder = 1