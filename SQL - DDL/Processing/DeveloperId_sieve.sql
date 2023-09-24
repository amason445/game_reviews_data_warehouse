/***
Query is intended to be embedded in Python code to select all distinct developer Ids from the developer table
***/

WITH max_date AS (

	SELECT
	DeveloperId,
	ScrapeDate,
	RANK() OVER(ORDER BY ScrapeDate DESC) as max_date

	FROM STAGE.dim_DeveloperTable
)

SELECT DISTINCT DeveloperId 

FROM max_date

WHERE max_date = 1