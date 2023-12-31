/****** 
This query checks the amount of metacritic reviews that are available in the population of games.
It builds a table that counts the current number of games and then counts games which have a metacritic review available.
It allowed me measure how many Metacritic games we have and games which are currently lacking reviews.
******/

SELECT 
'Full Universe' as PopulationSegment,
COUNT(GameDeveloperId) as FrequencyOfMetacriticScores
FROM MAIN.fact_GameReviews

UNION

SELECT
'With MetacriticScore' as PopulationSegment,
COUNT(GameDeveloperId) as FrequencyOfMetacriticScores
FROM MAIN.fact_GameReviews

WHERE MetacriticScore is not null