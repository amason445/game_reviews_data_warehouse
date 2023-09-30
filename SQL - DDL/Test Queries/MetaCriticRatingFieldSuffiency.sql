/****** 
This query checks the amount of metacritic reviews that are available in the population of games.
It builds a table that counts the current number of games and then counts games which have a metacritic review available.
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

UNION

SELECT
'Universe to MetaCriticScore' as PopulationSegment,
(COUNT(GameDeveloperId)/COUNT(MetacriticScore)) * 100 as PercentWithMetacriticScore
FROM MAIN.fact_GameReviews