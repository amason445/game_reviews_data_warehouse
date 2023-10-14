/**
This is a test query that joins the developer dim table to the ratings fact table.
It then aggregates them and counts the total number of games and average Rawg Review for each developer.
**/

SELECT devs.DeveloperName, 
avg(reviews.RawgIO_Rating) as AverageRating, 
count(DISTINCT reviews.GameId) as TotalGames

FROM STAGE.dim_DeveloperTable devs

LEFT JOIN STAGE.fact_GameReviews reviews on
	devs.DeveloperId = reviews.DeveloperId

GROUP BY devs.DeveloperName;