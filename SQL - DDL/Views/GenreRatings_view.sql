/**
This view uses joins across the star schema in GDW.MAIN to build a data set of video game genres, the games for each genre, and the developer for each game.
I used view to build a dashboard that allows users to analyze review data for each genre over time.
It also included a table allowing users to see which developers are releasing for each genre.
**/

USE GDW
GO

CREATE OR ALTER VIEW POWER_BI.genre_ratings_v
AS

SELECT
genre_dim.GenreName,
games_fact.GameTitle,
games_fact.ReleaseDate,
games_fact.RawgIO_Rating,
games_fact.RawgRatingsCount,
games_fact.MetacriticScore,
developer_dim.DeveloperName


FROM MAIN.dim_GenreTable genre_dim

INNER JOIN MAIN.dim_GenreBridgeTable genre_bridge ON
	genre_dim.GenreId = genre_bridge.GenreId

INNER JOIN MAIN.fact_GameReviews games_fact ON
	genre_bridge.GameId = games_fact.GameId
	AND MetacriticScore IS NOT NULL
	AND RawgRatingsCount > 0

INNER JOIN MAIN.dim_DeveloperBridgeTable developer_bridge ON
	games_fact.GameId = developer_bridge.GameId

INNEr JOIN MAIN.dim_DeveloperTable developer_dim ON
	developer_bridge.DeveloperId = developer_dim.DeveloperId

