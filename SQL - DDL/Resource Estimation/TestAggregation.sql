SELECT devs.DeveloperName, 
avg(reviews.RawgIO_Rating) as AverageRating, 
count(DISTINCT reviews.GameId) as TotalGames

FROM STAGE.dim_DeveloperTable devs

LEFT JOIN STAGE.fact_GameReviews reviews on
	devs.DeveloperId = reviews.DeveloperId

GROUP BY devs.DeveloperName