--Below script can be used to Shrink data file in chunks of 500MB, it will either shrink data file to 30% or keep 30% free space, which ever is higher. Its not recommended to Shrink data files but we all need to do it as we fight for space :)
--Its not recommended to Shrink data files but I still do it :-) https://www.sqlskills.com/blogs/paul/why-you-should-not-shrink-your-data-files/ 

USE DBNAME --Change it to appropriate database.

SET NOCOUNT ON;
GO
DECLARE @filename VARCHAR(128);
DECLARE @CurrentSize DECIMAL(20, 2);
DECLARE @SpaceUsed DECIMAL(20, 2);
DECLARE ShrinkDatabase CURSOR
FOR SELECT name AS FileName
     , size / 128.0 AS CurrentSizeMB
     , CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT)/128.0 SpaceUsed
    FROM sys.database_files
    WHERE [type_desc] = 'ROWS';
OPEN ShrinkDatabase;
FETCH NEXT FROM ShrinkDatabase INTO @FileName, @CurrentSize, @SpaceUsed;
WHILE @@FETCH_STATUS = 0
    BEGIN
    --SELECT @filename
    -- , @CurrentSize
    -- , @SpaceUsed;
    DECLARE @target INT, @target2 INT;

--Please modify below to from 0.30 to different value keep more/less free space.

    SET @target = @CurrentSize - (@CurrentSize * 0.30);
    SET @target2 = @SpaceUsed + (@SpaceUsed * 0.30);
    --SELECT @target LESS
    -- , @target2 MORE;
    IF(@target2 > @target)
    BEGIN
    SET @target = @target2;
    END;
    PRINT '--'+CONVERT(VARCHAR(128), @TARGET);
    DECLARE @cycle INT;
    DECLARE @sql VARCHAR(2000);
    SET NOCOUNT ON;
    SET @cycle = @CurrentSize;
    WHILE 1 = 1
    BEGIN
    SET @cycle = @cycle - 500;
    SET @sql = 'DBCC SHRINKFILE('+quotename(@filename)+', '+CAST(@cycle AS VARCHAR)+')'+CHAR(10)+CHAR(13)+'GO'++CHAR(10)+CHAR(13);
    PRINT(@sql);
    IF @cycle < @target BREAK;
    END;
    FETCH NEXT FROM ShrinkDatabase INTO @FileName, @CurrentSize, @SpaceUsed;
    END;
CLOSE ShrinkDatabase;
DEALLOCATE ShrinkDatabase;
