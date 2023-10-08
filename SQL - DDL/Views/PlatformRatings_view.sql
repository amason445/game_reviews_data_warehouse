USE GDW
GO

CREATE OR ALTER VIEW POWER_BI.platform_ratings_v
AS

SELECT
dim_platform.PlatformName,
dim_parent.ParentPlatformName,
fact_reviews.GameTitle,
fact_reviews.ReleaseDate,
fact_reviews.RawgIO_Rating,
fact_reviews.RawgRatingsCount,
fact_reviews.MetacriticScore


FROM MAIN.dim_PlatformTable dim_platform

INNER JOIN MAIN.dim_PlatformBridgeTable platform_bridge ON
	dim_platform.PlatformId = platform_bridge.PlatformId

INNER JOIN MAIN.fact_GameReviews fact_reviews ON
	platform_bridge.GameId = fact_reviews.GameId
	AND MetacriticScore IS NOT NULL
	AND RawgRatingsCount > 0

INNER JOIN MAIN.dim_ParentPlatformBridgeTable as parent_bridge ON
	fact_reviews.GameId = parent_bridge.GameId

INNER JOIN MAIN.dim_ParentPlatformTable dim_parent ON
	parent_bridge.ParentPlatformId = dim_parent.ParentPlatformId