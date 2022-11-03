CREATE PROCEDURE #spShrink (@BatchSize int = 10000, @FreePct int =15, @MaxGBFree int = 50000, @filename varchar(128) = 'ALL')
as 
set nocount on;
DECLARE @CurrentSize DECIMAL(20, 2);
DECLARE @LoopCurrentSize DECIMAL(20, 2);
DECLARE @ShrinkTo DECIMAL(20, 2);
DECLARE @sql VARCHAR(2000);
IF @filename = 'all' 
begin
set @filename = '%'
end;
WITH cte
     AS (SELECT name AS FileName, 
                size / 128.0 AS CurrentSizeMB, 
                CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT) / 128.0 SpaceUsed
         FROM sys.database_files
		 WHERE NAME LIKE @filename),
     CTE2
     AS (SELECT CASE
                    WHEN((SpaceUsed * (@FreePct / 100)) > @MaxGBFree)
                    THEN '50000'
                    ELSE(SpaceUsed * (@FreePct / 100))
                END FreeSpace, 
                *
         FROM cte)
     SELECT *, 
            (SpaceUsed + FreeSpace) ShrinkTo
     INTO #temp
     FROM CTE2;
DECLARE ShrinkDatabase CURSOR
FOR SELECT FileName, 
           CurrentSizeMB, 
           ShrinkTo
    FROM #temp
    ORDER BY CurrentSizeMB desc;
--SELECT *
--FROM #temp;
OPEN ShrinkDatabase;
FETCH NEXT FROM ShrinkDatabase INTO @FileName, @CurrentSize, @ShrinkTo;
WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @LoopCurrentSize = @CurrentSize;
        WHILE(@LoopCurrentSize + @BatchSize > @ShrinkTo)
            BEGIN
                IF @LoopCurrentSize < @BatchSize
                    BREAK;
                IF @CurrentSize < @ShrinkTo
                    BREAK;
                IF @LoopCurrentSize - @BatchSize < @ShrinkTo
                    BREAK;
                SET @LoopCurrentSize = @LoopCurrentSize - @BatchSize;
                SET @sql = 'DBCC SHRINKFILE(' + QUOTENAME(@filename) + ', ' + CAST(FLOOR(@LoopCurrentSize) AS VARCHAR) + ')' + CHAR(10) + CHAR(13) + 'GO' + +CHAR(10) + CHAR(13);
                PRINT @SQL;
            END;
        FETCH NEXT FROM ShrinkDatabase INTO @FileName, @CurrentSize, @ShrinkTo;
    END;
CLOSE ShrinkDatabase;
DEALLOCATE ShrinkDatabase;
DROP TABLE #temp;
go
exec #spShrink
go
drop procedure #spShrink
