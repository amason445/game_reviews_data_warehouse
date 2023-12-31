/**
This view uses joins across the star schema in GDW.MAIN to build a data set of developers, the games they've made, where they're listed for sale and what platforms they're on.
I used view to build a dashboard that ranks developers by review scores. It also allows filtering by Store Name and Platform Family (Play Station, Xbox, Nintendo, PC/Linux/Apple).
**/

USE GDW
GO

CREATE OR ALTER VIEW POWER_BI.top_developers_v
AS

SELECT DISTINCT
dim.DeveloperName,
fact.ReleaseDate,
fact.GameTitle,
fact.RawgIO_Rating,
fact.RawgRatingsCount,
fact.MetacriticScore,
stores_dim.StoreName,
parent_dim.ParentPlatformName as PlatformFamily

FROM MAIN.dim_DeveloperTable dim

INNER JOIN MAIN.dim_DeveloperBridgeTable bridge ON
	dim.DeveloperId = bridge.DeveloperId

INNER JOIN MAIN.fact_GameReviews fact ON
	bridge.GameId = fact.GameId
	AND fact.MetacriticScore IS NOT NULL
	AND RawgRatingsCount > 0

INNER JOIN MAIN.dim_StoresBridgeTable stores_bridge ON
	fact.GameId = stores_bridge.GameId

INNER JOIN MAIN.dim_StoresTable stores_dim ON
	stores_bridge.StoreId = stores_dim.StoreId

INNER JOIN MAIN.dim_ParentPlatformBridgeTable parent_bridge ON
	fact.GameId = parent_bridge.GameId

INNER JOIN MAIN.dim_ParentPlatformTable parent_dim ON
	parent_bridge.ParentPlatformId = parent_dim.ParentPlatformId