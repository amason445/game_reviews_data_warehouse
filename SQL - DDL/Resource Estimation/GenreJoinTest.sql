SELECT genre.GenreName, count(DISTINCT fact.GameId) as GameCount

FROM [GDW].[STAGE].[dim_GenreTable] genre

LEFT JOIN [GDW].[STAGE].[dim_GenreBridgeTable] bridge ON
	genre.GenreId = bridge.GenreId

LEFT JOIN [GDW].[STAGE].[fact_GameReviews] fact ON
	bridge.GameId = fact.GameId

GROUP BY GenreName

ORDER BY GameCount DESC
