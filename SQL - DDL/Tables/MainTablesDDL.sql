--create main tables for ETL process
CREATE TABLE MAIN.fact_GameReviews (
	GameId integer not null primary key,
	GameTitle varchar(500),
	ReleaseDate varchar(50),
	DeveloperId integer,
	RawgIO_Rating float,
	RawgRatingsCount integer,
	MetacriticScore varchar(50),
	ScrapeDate varchar(50));

CREATE TABLE MAIN.dim_date (
	Date date not null primary key,
	year integer,
	quarter integer,
	month integer,
	day integer);

CREATE TABLE MAIN.dim_DeveloperTable (
	DeveloperId integer not null primary key,
	DeveloperName varchar(200),
	DeveloperCount integer,
	ScrapeDate varchar(50));


CREATE TABLE MAIN.dim_GenreTable (
	GenreId integer not null primary key,
	GenreName varchar(200),
	GenreCount integer,
	ScrapeDate varchar(50));

CREATE TABLE MAIN.dim_GenreBridgeTable (
	GenreGameKey varchar(50) not null primary key,
	GenreId integer,
	GameId integer,
	ScrapeDate varchar(50));

CREATE TABLE MAIN.dim_PlatformTable (
	PlatformId integer not null primary key,
	PlatformName varchar(200),
	PlatformCount integer,
	ScrapeDate varchar(50));

CREATE TABLE MAIN.dim_PlatformBridgeTable (
	PlatformGameKey varchar(50) not null primary key,
	PlatformId integer,
	GameId integer,
	ScrapeDate varchar(50));

CREATE TABLE MAIN.dim_StoresTable (
	StoreId integer not null primary key,
	StoreName varchar(200),
	StoreCount integer,
	ScrapeDate varchar(50));

CREATE TABLE MAIN.dim_StoresBridgeTable (
	StoreGameKey varchar(50) not null primary key,
	StoreId integer,
	GameId integer,
	ScrapeDate varchar(50));