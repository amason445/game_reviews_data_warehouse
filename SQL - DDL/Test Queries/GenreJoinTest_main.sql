/***
This query runs the count of games I have databased for each genre. A game can have multiple genres so it uses two left joins. 
The first join is on the genre table to the bridge table and the second join is between the bridge table and the game fact table.
This query runs on the staging schema.
***/

SELECT 
genre.GenreName, 
count(fact.GameId) as GameCount,
avg(fact.RawgIO_Rating) as AvgRawgRating

FROM [GDW].[MAIN].[dim_GenreTable] genre

LEFT JOIN [GDW].[MAIN].[dim_GenreBridgeTable] bridge ON
	genre.GenreId = bridge.GenreId

LEFT JOIN [GDW].[MAIN].[fact_GameReviews] fact ON
	bridge.GameId = fact.GameId

GROUP BY GenreName

ORDER BY GameCount DESC
