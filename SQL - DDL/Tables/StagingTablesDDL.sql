USE GDW
GO

--create staging tables for ETL process
CREATE TABLE STAGE.fact_GameReviews (
	GameId integer NOT NULL PRIMARY KEY,
	GameTitle varchar,
	ReleaseDate date,
	DeveloperId integer,
	RawgIO_Rating float,
	RawgRatingsCount integer,
	MetacriticScore integer);

CREATE TABLE STAGE.dim_date (
	Date date NOT NULL PRIMARY KEY,
	year integer,
	quarter integer,
	month integer,
	day integer);

CREATE TABLE STAGE.dim_DeveloperTable (
	DeveloperId integer NOT NULL PRIMARY KEY,
	DeveloperName varchar,
	DeveloperCount integer);

CREATE TABLE STAGE.dim_GenreTable (
	GenreId integer NOT NULL PRIMARY KEY,
	GenreName varchar,
	GenreCount integer);

CREATE TABLE STAGE.dim_GenreBridgeTable (
	GenreGameKey varchar NOT NULL PRIMARY KEY,
	GenreId integer,
	GameId integer);

CREATE TABLE STAGE.dim_PlatformTable (
	PlatformId integer NOT NULL PRIMARY KEY,
	PlatformName varchar,
	PlatformCount integer);