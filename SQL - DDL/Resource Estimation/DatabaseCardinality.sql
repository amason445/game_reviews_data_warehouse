/***
This query was written to provide table sizes for the database.
It will count the rows in each table and join them by unions.
***/

USE GDW
GO

SELECT 
'Game Developers' as TableName,
COUNT(DeveloperId) as RowSize

FROM MAIN.dim_DeveloperTable

UNION

SELECT 
'GameReviews' as TableName,
COUNT(GameId) as RowSize

FROM MAIN.fact_GameReviews

UNION

SELECT
'Game Genres' as TableName,
count(GenreId) as RowSize

FROM MAIN.dim_GenreTable

UNION

SELECT 
'Game Parent Platforms' as TableName,
count(ParentPlatformId) as RowSize

FROM MAIN.dim_ParentPlatformTable

UNION

SELECT 
'Game Platforms' as TableName,
count(PlatformId) as RowSize

FROM MAIN.dim_PlatformTable

UNION

SELECT 
'Game Stores' as TableName,
count(StoreId) as RowSize

FROM MAIN.dim_StoresTable