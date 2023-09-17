/***
This procedure scrapes the staging tables, adjusts them and uses merge insert to load them into the main tables
***/


USE GDW
GO

CREATE PROCEDURE etl.STAGEtoMAIN_proc
AS

--create temporary tables for ETL process
BEGIN

	CREATE TABLE #fact_GameReviews (
		GameId integer,
		GameTitle varchar(500),
		ReleaseDate varchar(50),
		DeveloperId integer,
		RawgIO_Rating float,
		RawgRatingsCount integer,
		MetacriticScore varchar(50),
		ScrapeDate varchar(50));

	CREATE TABLE #dim_date (
		Date date,
		year integer,
		quarter integer,
		month integer,
		day integer);

	CREATE TABLE #dim_DeveloperTable (
		DeveloperId integer,
		DeveloperName varchar(200),
		DeveloperCount integer,
		ScrapeDate varchar(50));


	CREATE TABLE #dim_GenreTable (
		GenreId integer,
		GenreName varchar(200),
		GenreCount integer,
		ScrapeDate varchar(50));

	CREATE TABLE #dim_GenreBridgeTable (
		GenreGameKey varchar(50),
		GenreId integer,
		GameId integer,
		ScrapeDate varchar(50));

	CREATE TABLE #dim_PlatformTable (
		PlatformId integer,
		PlatformName varchar(200),
		PlatformCount integer,
		ScrapeDate varchar(50));

	CREATE TABLE #dim_PlatformBridgeTable (
		PlatformGameKey varchar(50),
		PlatformId integer,
		GameId integer,
		ScrapeDate varchar(50));

	CREATE TABLE #dim_StoresTable (
		StoreId integer,
		StoreName varchar(200),
		StoreCount integer,
		ScrapeDate varchar(50));

	CREATE TABLE #dim_StoresBridgeTable (
		StoreGameKey varchar(50),
		StoreId integer,
		GameId integer,
		ScrapeDate varchar(50));

END

--process to move staging fact table to main fact table
BEGIN

	SELECT
	GameId,
	GameTitle,
	

	FROM STAGE.fact_GameReviews