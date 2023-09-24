/***
This query runs the count of games I have databased for each genre. A game can have multiple genres so it uses two left joins. 
The first join is on the genre table to the bridge table and the second join is between the bridge table and the game fact table.
***/

SELECT 
platform.ParentPlatformName, 
count(fact.GameId) as GameCount,
min(fact.ReleaseDate) as OldestGame,
max(fact.ReleaseDate) as NewestGame


FROM [GDW].[MAIN].[dim_ParentPlatformTable] platform

LEFT JOIN [GDW].[MAIN].[dim_ParentPlatformBridgeTable] bridge ON
	platform.ParentPlatformId = bridge.ParentPlatformId

LEFT JOIN [GDW].[MAIN].[fact_GameReviews] fact ON
	bridge.GameId = fact.GameId

GROUP BY ParentPlatformName

ORDER BY GameCount DESC
