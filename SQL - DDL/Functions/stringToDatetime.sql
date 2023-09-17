/***
This function checks if a field has a date string. 
If it does, it converts it into a date type.
If it does not, it returns null

I referenced this stackoverflow answer wehn building this function:
https://stackoverflow.com/questions/5070153/how-to-use-a-case-statement-in-scalar-valued-function-in-sql

***/

USE GDW
GO

CREATE FUNCTION etl.dateStringToDatetime
( @input_string varchar(50) )
RETURNS Date
AS
BEGIN

	DECLARE @output_date AS Date
	SELECT @output_date = 
							CASE @input_string
								WHEN @input_string = 'None' THEN None
								ELSE CAST(@input_string AS Date)
							END

	RETURN @output_date

END;
