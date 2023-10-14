/***
This procedure scrapes the staging tables, adjusts them and uses merge insert to load them into the main tables.
This procedure is required to extract the raw data from staging, normalize it and then load it to the final tables.
It first changes the data types, creates logic for primary keys and it recasts certain fields to the right data types using temporary tables.
Then it uses merge logic to validate primary keys in the temporary tables against whats already loaded in the main tables.
Finally it uses update/insert queries to merge the data.
***/


USE GDW
GO

ALTER PROCEDURE etl.STAGEtoMAIN_proc
AS

--create temporary tables for ETL process
BEGIN

	CREATE TABLE #fact_GameReviews (
		GameDeveloperId varchar(100),
		GameId integer,
		GameTitle varchar(500),
		ReleaseDate date,
		DeveloperId integer,
		RawgIO_Rating float,
		RawgRatingsCount integer,
		MetacriticScore integer,
		ScrapeDate datetime);

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

	CREATE TABLE #dim_DeveloperBridgeTable (
		DeveloperGameKey varchar(50),
		DeveloperId integer,
		GameId integer,
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

	CREATE TABLE #dim_ParentPlatformTable (
		ParentPlatformId integer,
		ParentPlatformName varchar(200),
		ScrapeDate varchar(50));

	CREATE TABLE #dim_ParentPlatformBridgeTable (
		ParentPlatformGameKey varchar(50),
		ParentPlatformId integer,
		GameId integer,
		ScrapeDate varchar(50));

END

--process to move staging fact table to temporary fact table, adjust data and filter for the maximum scrape date
BEGIN
WITH max_date AS (

	SELECT
	CONCAT(GameId, '-', DeveloperId) as GameDeveloperId,
	GameId,
	GameTitle,
	CASE
		WHEN ReleaseDate = 'None' THEN null
		ELSE CAST(ReleaseDate AS DATE)
	END ReleaseDate,
	DeveloperId,
	RawgIO_Rating,
	RawgRatingsCount,
	CASE 
		WHEN MetacriticScore = 'None' THEN null
		ELSE MetacriticScore
	END MetacriticScore,
	CAST(ScrapeDate as datetime2) as ScrapeDate,
	RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

	FROM STAGE.fact_GameReviews),

	max_date_filter AS (
	SELECT 
	GameDeveloperId,
	GameId,
	GameTitle,
	ReleaseDate,
	DeveloperId,
	RawgIO_Rating,
	RawgRatingsCount,
	MetacriticScore,
	ScrapeDate
		
	FROM max_date WHERE ScrapeDateOrder = 1)

	INSERT INTO #fact_GameReviews
	(
		GameDeveloperId,
		GameId,
		GameTitle,
		ReleaseDate,
		DeveloperId,
		RawgIO_Rating,
		RawgRatingsCount,
		MetacriticScore,
		ScrapeDate
	)
	
	SELECT DISTINCT
		GameDeveloperId,
		GameId,
		GameTitle,
		ReleaseDate,
		DeveloperId,
		RawgIO_Rating,
		RawgRatingsCount,
		MetacriticScore,
		ScrapeDate
	FROM max_date_filter

END;

--process to load date dim date table from the fact table
BEGIN
	WITH max_date AS (
	SELECT
	CASE
		WHEN ReleaseDate = 'None' THEN null
		ELSE CAST(ReleaseDate AS DATE)
	END date,
	CASE
		WHEN ReleaseDate = 'None' THEN null
		ELSE DATEPART(YEAR,CAST(ReleaseDate AS DATE))
	END year,
	CASE
		WHEN ReleaseDate = 'None' THEN null
		ELSE DATEPART(QUARTER,CAST(ReleaseDate AS DATE))
	END quarter,
	CASE
		WHEN ReleaseDate = 'None' THEN null
		ELSE DATEPART(MONTH,CAST(ReleaseDate AS DATE))
	END month,
	CASE
		WHEN ReleaseDate = 'None' THEN null
		ELSE DATEPART(day,CAST(ReleaseDate AS DATE))
	END day,
	CAST(ScrapeDate as datetime2) as ScrapeDate,
	RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

	FROM STAGE.fact_GameReviews),

	max_date_filter AS (
	SELECT DISTINCT
	date,
	year,
	quarter,
	month,
	day
		
	FROM max_date WHERE ScrapeDateOrder = 1 AND date IS NOT NULL)

	INSERT INTO #dim_date
	(
		date,
		year,
		quarter,
		month,
		day
	)
	
	SELECT
		date,
		year,
		quarter,
		month,
		day
	FROM max_date_filter
END;

--process to load developer dim table to temporary dim table
BEGIN
	WITH max_date AS (
		SELECT
		DeveloperId,
		DeveloperName,
		DeveloperCount,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_DeveloperTable),

		max_date_filter AS (
		SELECT 
		DeveloperId,
		DeveloperName,
		DeveloperCount,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)

	INSERT INTO #dim_DeveloperTable
	(
		DeveloperId,
		DeveloperName,
		DeveloperCount,
		ScrapeDate
	)
	
	SELECT
		DeveloperId,
		DeveloperName,
		DeveloperCount,
		ScrapeDate
	FROM max_date_filter
END;

--process to load developer bridge dim table to temporary dim table
BEGIN
	WITH max_date AS (
		SELECT
		CONCAT(CAST(GameId as varchar(100)), '-', CAST(DeveloperId as varchar(100))) AS DeveloperGameKey,
		DeveloperId,
		GameId,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder

from STAGE.fact_GameReviews),

		max_date_filter AS (
		SELECT
		DeveloperGameKey,
		DeveloperId,
		GameId,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)

	INSERT INTO #dim_DeveloperBridgeTable
	(
		DeveloperGameKey,
		DeveloperId,
		GameId,
		ScrapeDate
	)
	
	SELECT DISTINCT
		DeveloperGameKey,
		DeveloperId,
		GameId,
		ScrapeDate
	FROM max_date_filter
END;


--process to load genre dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		GenreId,
		GenreName,
		GenreCount,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_GenreTable),

		max_date_filter AS (
		SELECT 
		GenreId,
		GenreName,
		GenreCount,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)

		INSERT INTO #dim_GenreTable
		(
			GenreId,
			GenreName,
			GenreCount,
			ScrapeDate
		)
	
		SELECT
			GenreId,
			GenreName,
			GenreCount,
			ScrapeDate
		FROM max_date_filter
END;

--process to load genre bridge dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		GenreGameKey,
		GenreId,
		GameId,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_GenreBridgeTable),

		max_date_filter AS (
		SELECT 
		GenreGameKey,
		GenreId,
		GameId,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)


		INSERT INTO #dim_GenreBridgeTable
		(
			GenreGameKey,
			GenreId,
			GameId,
			ScrapeDate
		)
	
		SELECT DISTINCT
			GenreGameKey,
			GenreId,
			GameId,
			ScrapeDate
		FROM max_date_filter
END;


--process to load platform dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		PlatformId,
		PlatformName,
		PlatformCount,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_PlatformTable),

		max_date_filter AS (
		SELECT 
		PlatformId,
		PlatformName,
		PlatformCount,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)

		INSERT INTO #dim_PlatformTable
		(
			PlatformId,
			PlatformName,
			PlatformCount,
			ScrapeDate
		)
	
		SELECT
			PlatformId,
			PlatformName,
			PlatformCount,
			ScrapeDate
		FROM max_date_filter
END;

--process to load platform bridge dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		PlatformGameKey,
		PlatformId,
		GameId,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_PlatformBridgeTable),

		max_date_filter AS (
		SELECT 
		PlatformGameKey,
		PlatformId,
		GameId,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)


		INSERT INTO #dim_PlatformBridgeTable
		(
			PlatformGameKey,
			PlatformId,
			GameId,
			ScrapeDate
		)
	
		SELECT DISTINCT
			PlatformGameKey,
			PlatformId,
			GameId,
			ScrapeDate
		FROM max_date_filter
END;


--process to load stores dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		StoreId,
		StoreName,
		StoreCount,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_StoresTable),

		max_date_filter AS (
		SELECT 
		StoreId,
		StoreName,
		StoreCount,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)

		INSERT INTO #dim_StoresTable
		(
			StoreId,
			StoreName,
			StoreCount,
			ScrapeDate
		)
	
		SELECT
			StoreId,
			StoreName,
			StoreCount,
			ScrapeDate
		FROM max_date_filter
END;

--process to load stores bridge dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		StoreGameKey,
		StoreId,
		GameId,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_StoresBridgeTable),

		max_date_filter AS (
		SELECT 
		StoreGameKey,
		StoreId,
		GameId,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)


		INSERT INTO #dim_StoresBridgeTable
		(
			StoreGameKey,
			StoreId,
			GameId,
			ScrapeDate
		)
	
		SELECT DISTINCT
			StoreGameKey,
			StoreId,
			GameId,
			ScrapeDate
		FROM max_date_filter
END;

--process to load parent platforms dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		ParentPlatformId,
		ParentPlatformName,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	

		FROM STAGE.dim_ParentPlatformTable),

		max_date_filter AS (
		SELECT 
		ParentPlatformId,
		ParentPlatformName,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)

		INSERT INTO #dim_ParentPlatformTable
		(
			ParentPlatformId,
			ParentPlatformName,
			ScrapeDate
		)
	
		SELECT
			ParentPlatformId,
			ParentPlatformName,
			ScrapeDate
		FROM max_date_filter
END;


--process to load parent platform dim table to temporary dim table
BEGIN
	with max_date as (
		SELECT
		ParentPlatformGameKey,
		ParentPlatformId,
		GameId,
		ScrapeDate,
		RANK() OVER (ORDER BY CAST(ScrapeDate as datetime2) DESC) as ScrapeDateOrder
	
		FROM STAGE.dim_ParentPlatformBridgeTable),

		max_date_filter AS (
		SELECT 
		ParentPlatformGameKey,
		ParentPlatformId,
		GameId,
		ScrapeDate
		
		FROM max_date WHERE ScrapeDateOrder = 1)


		INSERT INTO #dim_ParentPlatformBridgeTable
		(
			ParentPlatformGameKey,
			ParentPlatformId,
			GameId,
			ScrapeDate
		)
	
		SELECT DISTINCT
			ParentPlatformGameKey,
			ParentPlatformId,
			GameId,
			ScrapeDate
		FROM max_date_filter
END;

--process to load temporary tables to main fact table
--uses merge logic to check staging data against primary keys which have already been stored in main against temp tables
--then inserts of updates the record
BEGIN
	MERGE INTO MAIN.fact_GameReviews as target
	USING #fact_GameReviews as source
	ON target.GameDeveloperId = source.GameDeveloperId
	WHEN MATCHED THEN 
	UPDATE SET
		target.GameId = source.GameId,
		target.GameTitle = source.GameTitle,
		target.ReleaseDate = source.ReleaseDate,
		target.DeveloperId = source.DeveloperId,
		target.RawgIO_Rating = source.RawgIO_Rating,
		target.RawgRatingsCount = source.RawgIO_Rating,
		target.MetacriticScore = source.MetacriticScore,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
		(
		GameDeveloperId,
		GameId,
		GameTitle,
		ReleaseDate,
		DeveloperId,
		RawgIO_Rating,
		RawgRatingsCount,
		MetacriticScore,
		ScrapeDate)
		VALUES (
		source.GameDeveloperId,
		source.GameId,
		source.GameTitle,
		source.ReleaseDate,
		source.DeveloperId,
		source.RawgIO_Rating,
		source.RawgRatingsCount,
		source.MetacriticScore,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_date as target
	USING #dim_date as source
	ON target.Date = source.Date
	WHEN MATCHED THEN
	UPDATE SET
		target.year = source.year,
		target.quarter = source.quarter,
		target.month = source.month,
		target.day = source.day
	WHEN NOT MATCHED THEN INSERT
	(
		Date,
		year,
		quarter,
		month,
		day
	)
	VALUES (
		source.Date,
		source.year,
		source.quarter,
		source.month,
		source.day);

	MERGE INTO MAIN.dim_DeveloperTable as target
	USING #dim_DeveloperTable as source
	ON target.DeveloperId = source.DeveloperId
	WHEN MATCHED THEN
	UPDATE SET
		target.DeveloperName = source.DeveloperName,
		target.DeveloperCount = source.DeveloperCount,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		DeveloperId,
		DeveloperName,
		DeveloperCount,
		ScrapeDate
	)
	VALUES (
		source.DeveloperId,
		source.DeveloperName,
		source.DeveloperCount,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_DeveloperBridgeTable as target
	USING #dim_DeveloperBridgeTable as source
	ON target.DeveloperGameKey = source.DeveloperGameKey
	WHEN MATCHED THEN
	UPDATE SET
		target.DeveloperId = source.DeveloperId,
		target.GameId = source.GameId,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		DeveloperGameKey,
		DeveloperId,
		GameId,
		ScrapeDate
	)
	VALUES (
		source.DeveloperGameKey,
		source.DeveloperId,
		source.GameId,
		source.ScrapeDate);


	MERGE INTO MAIN.dim_GenreTable as target
	USING #dim_GenreTable as source
	ON target.GenreId = source.GenreId
	WHEN MATCHED THEN
	UPDATE SET
		target.GenreName = source.GenreName,
		target.GenreCount = source.GenreCount,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		GenreId,
		GenreName,
		GenreCount,
		ScrapeDate
	)
	VALUES (
		source.GenreId,
		source.GenreName,
		source.GenreCount,
		source.ScrapeDate);


	MERGE INTO MAIN.dim_GenreBridgeTable as target
	USING #dim_GenreBridgeTable as source
	ON target.GenreGameKey = source.GenreGameKey
	WHEN MATCHED THEN
	UPDATE SET
		target.GenreId = source.GenreId,
		target.GameId = source.GameId,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		GenreGameKey,
		GenreId,
		GameId,
		ScrapeDate
	)
	VALUES (
		source.GenreGameKey,
		source.GenreId,
		source.GameId,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_PlatformTable as target
	USING #dim_PlatformTable as source
	ON target.PlatformId = source.PlatformId
	WHEN MATCHED THEN
	UPDATE SET
		target.PlatformName = source.PlatformName,
		target.PlatformCount = source.PlatformCount,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		PlatformId,
		PlatformName,
		PlatformCount,
		ScrapeDate
	)
	VALUES (
		source.PlatformId,
		source.PlatformName,
		source.PlatformCount,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_PlatformBridgeTable as target
	USING #dim_PlatformBridgeTable as source
	ON target.PlatformGameKey = source.PlatformGameKey
	WHEN MATCHED THEN
	UPDATE SET
		target.PlatformId = source.PlatformId,
		target.GameId = source.GameId,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		PlatformGameKey,
		PlatformId,
		GameId,
		ScrapeDate
	)
	VALUES (
		source.PlatformGameKey,
		source.PlatformId,
		source.GameId,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_StoresTable as target
	USING #dim_StoresTable as source
	ON target.StoreId = source.StoreId
	WHEN MATCHED THEN
	UPDATE SET
		target.StoreName = source.StoreName,
		target.StoreCount = source.StoreCount,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		StoreId,
		StoreName,
		StoreCount,
		ScrapeDate
	)
	VALUES (
		source.StoreId,
		source.StoreName,
		source.StoreCount,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_StoresBridgeTable as target
	USING #dim_StoresBridgeTable as source
	ON target.StoreGameKey = source.StoreGameKey
	WHEN MATCHED THEN
	UPDATE SET
		target.StoreId = source.StoreId,
		target.GameId = source.GameId,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		StoreGameKey,
		StoreId,
		GameId,
		ScrapeDate
	)
	VALUES (
		source.StoreGameKey,
		source.StoreId,
		source.GameId,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_ParentPlatformTable as target
	USING #dim_ParentPlatformTable as source
	ON target.ParentPlatformId = source.ParentPlatformId
	WHEN MATCHED THEN
	UPDATE SET
		target.ParentPlatformName = source.ParentPlatformName,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		ParentPlatformId,
		ParentPlatformName,
		ScrapeDate
	)
	VALUES (
		source.ParentPlatformId,
		source.ParentPlatformName,
		source.ScrapeDate);

	MERGE INTO MAIN.dim_ParentPlatformBridgeTable as target
	USING #dim_ParentPlatformBridgeTable as source
	ON target.ParentPlatformGameKey = source.ParentPlatformGameKey
	WHEN MATCHED THEN
	UPDATE SET
		target.ParentPlatformId = source.ParentPlatformId,
		target.GameId = source.GameId,
		target.ScrapeDate = source.ScrapeDate
	WHEN NOT MATCHED THEN INSERT
	(
		ParentPlatformGameKey,
		ParentPlatformId,
		GameId,
		ScrapeDate
	)
	VALUES (
		source.ParentPlatformGameKey,
		source.ParentPlatformId,
		source.GameId,
		source.ScrapeDate);
END;


	