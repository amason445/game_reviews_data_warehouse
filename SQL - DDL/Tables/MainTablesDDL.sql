USE GDW
GO

--create main tables for ETL process
CREATE TABLE MAIN.fact_GameReviews (
	GameDeveloperId varchar(100) not null primary key,
	GameId integer,
	GameTitle varchar(500),
	ReleaseDate date,
	DeveloperId integer,
	RawgIO_Rating float,
	RawgRatingsCount integer,
	MetacriticScore integer,
	ScrapeDate datetime);

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

CREATE TABLE MAIN.dim_DeveloperBridgeTable (
	DeveloperGameKey varchar(50) not null primary key,
	DeveloperId integer,
	GameId integer,
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