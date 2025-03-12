CREATE OR ALTER PROCEDURE dbo.#sp_BatchShrinkFiles
    @BatchShrinkSize int = 100,  -- Size in MB for each shrink operation
    @FreePct int = 10,            -- Percentage of free space to maintain
    @MaxMBFree int = 10240,       -- Maximum free space in MB (Default 10GB)
    @FileName varchar(128) = '', -- Specific filename, 'ALL' Or Empty '' Or NULL for all files
    @Execute bit = 1              -- 1 = execute commands, 0 = print only
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentSize DECIMAL(20, 2);
    DECLARE @LoopCurrentSize DECIMAL(20, 2);
    DECLARE @ShrinkTo DECIMAL(20, 2);
    DECLARE @Sql VARCHAR(2000);
    
	-- Handle the ALL parameter, including NULL and empty string
	DECLARE @FilePattern varchar(128) = CASE 
		WHEN UPPER(@FileName) = 'ALL' THEN '%' 
		WHEN @FileName IS NULL THEN '%'
		WHEN @FileName = '' THEN '%'
		ELSE @FileName 
	END;
    -- Create temp table with file information
    WITH FileInfo AS (
        SELECT 
            name AS FileName,
            size / 128.0 AS CurrentSizeMB,
            CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT) / 128.0 AS SpaceUsedMB
        FROM sys.database_files
        WHERE name LIKE @FilePattern
    ),
    SpaceCalc AS (
        SELECT 
            FileName,
            CurrentSizeMB,
            SpaceUsedMB,
            CASE 
                WHEN (SpaceUsedMB * (@FreePct / 100.0)) > @MaxMBFree 
                THEN @MaxMBFree
                ELSE (SpaceUsedMB * (@FreePct / 100.0))
            END AS FreeSpaceMB
        FROM FileInfo
    )
    SELECT 
        FileName,
        CurrentSizeMB,
        SpaceUsedMB,
        FreeSpaceMB,
        (SpaceUsedMB + FreeSpaceMB) AS ShrinkToMB
    INTO #ShrinkTargets
    FROM SpaceCalc;

    -- Declare cursor for processing each file
    DECLARE ShrinkCursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT FileName, CurrentSizeMB, ShrinkToMB
        FROM #ShrinkTargets
        ORDER BY CurrentSizeMB DESC;

    BEGIN TRY
        OPEN ShrinkCursor;
        FETCH NEXT FROM ShrinkCursor INTO @FileName, @CurrentSize, @ShrinkTo;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @LoopCurrentSize = @CurrentSize;
            
            -- Skip if file is already smaller than target size
            IF @CurrentSize <= @ShrinkTo 
            BEGIN
                PRINT '--File ' + QUOTENAME(@FileName) + ' is already at or below target size.';
                FETCH NEXT FROM ShrinkCursor INTO @FileName, @CurrentSize, @ShrinkTo;
                CONTINUE;
            END

            -- Perform incremental shrinking
            WHILE @LoopCurrentSize > @ShrinkTo
            BEGIN
                -- Break if remaining size is less than batch size
                IF @LoopCurrentSize < @BatchShrinkSize 
                    BREAK;

                -- Calculate next shrink target
                SET @LoopCurrentSize = 
                    CASE 
                        WHEN @LoopCurrentSize - @BatchShrinkSize < @ShrinkTo 
                        THEN @ShrinkTo
                        ELSE @LoopCurrentSize - @BatchShrinkSize
                    END;

                -- Generate shrink command
                SET @Sql = 'DBCC SHRINKFILE(' + QUOTENAME(@FileName) + ', ' + 
                          CAST(FLOOR(@LoopCurrentSize) AS VARCHAR(20)) + ')';
                
                -- Execute or print based on @Execute parameter
                IF @Execute = 1
                BEGIN
                    PRINT 'Executing: ' + @Sql;
                    EXEC (@Sql);
                    WAITFOR DELAY '00:00:01';
                END
                ELSE
                BEGIN
                    PRINT '' + @Sql;
                END
            END

            FETCH NEXT FROM ShrinkCursor INTO @FileName, @CurrentSize, @ShrinkTo;
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
        IF CURSOR_STATUS('global', 'ShrinkCursor') >= 0
        BEGIN
            CLOSE ShrinkCursor;
            DEALLOCATE ShrinkCursor;
        END
        DROP TABLE #ShrinkTargets;
        RETURN;
    END CATCH

    -- Cleanup
    CLOSE ShrinkCursor;
    DEALLOCATE ShrinkCursor;
    DROP TABLE #ShrinkTargets;

    IF @Execute = 1
        PRINT 'Database file shrink operation completed.';
    ELSE
        PRINT '--Database file shrink commands generated (preview mode).';
END
GO


-- Just print the commands without executing
EXEC dbo.#sp_BatchShrinkFiles 
    @BatchShrinkSize = 100
