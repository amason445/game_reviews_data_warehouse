/***
This query runs off the developer dim table and was written to estimate API Resource Cost wen pulling game data.
AvgCost is any developer's games divided by the expected returned games for each page. It will cost one API request to scrape 15 games.
MinCost is a theoretical floor determined by the API page limit I set at 50 in the URL. API pages don't seem to reach 50 but any that do would introduce cost savings.
***/

WITH resource_columns AS (

	SELECT [DeveloperId]
		  ,[DeveloperName]
		  ,[DeveloperCount]
		  ,[DeveloperCount] / 15 as AvgCost
		  ,[DeveloperCount] / 50 as MinCost
		  ,[ScrapeDate]
		  ,RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	FROM [GDW].[STAGE].[dim_DeveloperTable]

)

SELECT
SUM([DeveloperCount]) as TotalGames,
MAX([DeveloperCount]) as MaxGamesForAnyDeveloper,
MIN([DeveloperCount]) as MinGamesForAnyDeveloper,
SUM(AvgCost) as TotalCost,
MAX(AvgCost) as MostExpensiveDeveloperCount,
MIN(AvgCost) as LeastExpensiveDeveloperCount,
SUM(MinCost) as TheoreticalCost,
MAX(MinCost) as TheoreticalMostExpensiveDeveloperCount,
MIN(MinCost) as TheoreticalLeastExpensiveDeveloperCount

FROM resource_columns

WHERE ScrapeDateOrder = 1