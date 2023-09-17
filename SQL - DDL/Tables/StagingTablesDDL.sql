--create staging tables for ETL process
CREATE TABLE STAGE.fact_GameReviews (
	GameId integer,
	GameTitle varchar(500),
	ReleaseDate varchar(50),
	DeveloperId integer,
	RawgIO_Rating float,
	RawgRatingsCount integer,
	MetacriticScore varchar(50),
	ScrapeDate varchar(50));

CREATE TABLE STAGE.dim_date (
	Date date NOT NULL PRIMARY KEY,
	year integer,
	quarter integer,
	month integer,
	day integer);

CREATE TABLE STAGE.dim_DeveloperTable (
	DeveloperId integer,
	DeveloperName varchar(200),
	DeveloperCount integer,
	ScrapeDate varchar(50));


CREATE TABLE STAGE.dim_GenreTable (
	GenreId integer,
	GenreName varchar(200),
	GenreCount integer,
	ScrapeDate varchar(50));

CREATE TABLE STAGE.dim_GenreBridgeTable (
	GenreGameKey varchar(50),
	GenreId integer,
	GameId integer,
	ScrapeDate varchar(50));

CREATE TABLE STAGE.dim_PlatformTable (
	PlatformId integer,
	PlatformName varchar(200),
	PlatformCount integer,
	ScrapeDate varchar(50));

CREATE TABLE STAGE.dim_PlatformBridgeTable (
	PlatformGameKey varchar(50),
	PlatformId integer,
	GameId integer,
	ScrapeDate varchar(50));

CREATE TABLE STAGE.dim_StoresTable (
	StoreId integer,
	StoreName varchar(200),
	StoreCount integer,
	ScrapeDate varchar(50));

CREATE TABLE STAGE.dim_StoresBridgeTable (
	StoreGameKey varchar(50),
	StoreId integer,
	GameId integer,
	ScrapeDate varchar(50));