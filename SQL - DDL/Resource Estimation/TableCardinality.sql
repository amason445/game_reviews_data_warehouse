/***
This query was written to provide table sizes for the database
***/

SELECT 
'Developers' as TableName,
count(DeveloperId) as RowSize

FROM STAGE.dim_DeveloperTable

UNION

SELECT 
'GameReviews' as TableName,
count(GameId) as RowSize

FROM STAGE.fact_GameReviews