USE GDW
GO



WITH platform_ratings as (

SELECT DISTINCT
dim_parent.ParentPlatformName,
dim_platform.PlatformName,
fact_reviews.GameTitle,
fact_reviews.ReleaseDate,
fact_reviews.RawgIO_Rating,
fact_reviews.RawgRatingsCount,
fact_reviews.MetacriticScore


FROM MAIN.dim_ParentPlatformTable dim_parent

INNER JOIN MAIN.dim_ParentPlatformBridgeTable parent_bridge ON
	dim_parent.ParentPlatformId = parent_bridge.ParentPlatformId

INNER JOIN MAIN.fact_GameReviews fact_reviews ON
	parent_bridge.GameId = fact_reviews.GameId
	AND MetacriticScore IS NOT NULL
	AND RawgIO_Rating IS NOT NULL

INNER JOIN MAIN.dim_PlatformBridgeTable as platform_bridge ON
	fact_reviews.GameId = platform_bridge.GameId

INNER JOIN MAIN.dim_PlatformTable dim_platform ON
	platform_bridge.PlatformId = dim_platform.PlatformId)

SELECT 
YEAR(ReleaseDate) as ReleaseYear, 
ParentPlatformName, 
PlatformName, 
AVG(RawgIO_Rating) as AverageRawgRating,
AVG(RawgRatingsCount) as AverageRawgEngagement,
AVG(MetaCriticScore) as AverageMetaCriticScore,
COUNT(GameTitle) as NumberOfGames

FROM platform_ratings

GROUP BY YEAR(ReleaseDate), ParentPlatformName, PlatformName

HAVING YEAR(ReleaseDate) IS NOT NULL