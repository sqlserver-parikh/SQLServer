CREATE OR ALTER PROCEDURE dbo.#sp_BatchShrinkFiles
    @BatchShrinkSize int = 100,  -- Size in MB for each shrink operation
    @FreePct int = 10,           -- Percentage of free space to maintain
    @MaxMBFree int = 10240,      -- Maximum free space in MB (Default 10GB)
    @FileName varchar(128) = '', -- Specific filename, 'ALL' Or Empty '' Or NULL for all files
    @Drive varchar(128) = '',    -- Drive or mount point to target, 'ALL' Or Empty '' Or NULL for all drives
    @DatabaseName varchar(128) = '',  -- Database name to target, 'ALL' Or Empty '' Or NULL for all databases
    @Execute bit = 0            -- 1 = execute commands, 0 = print only
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentSize DECIMAL(20, 2);
    DECLARE @LoopCurrentSize DECIMAL(20, 2);
    DECLARE @ShrinkTo DECIMAL(20, 2);
    DECLARE @Sql NVARCHAR(MAX);
    DECLARE @FilePattern VARCHAR(128);

    -- Handle the ALL parameter, including NULL and empty string
    SET @FilePattern = CASE 
        WHEN UPPER(@FileName) = 'ALL' THEN '%' 
        WHEN @FileName IS NULL THEN '%'
        WHEN @FileName = '' THEN '%' 
        ELSE @FileName 
    END;

    -- Handle the ALL parameter for drive, including NULL and empty string
    SET @Drive = CASE 
        WHEN UPPER(@Drive) = 'ALL' THEN '%'
        WHEN @Drive IS NULL THEN '%'
        WHEN @Drive = '' THEN '%'
        ELSE @Drive + '%'
    END;

    -- Handle the ALL parameter for database name, including NULL and empty string
    DECLARE @DatabaseCursor CURSOR;
    IF UPPER(@DatabaseName) = 'ALL' OR @DatabaseName IS NULL OR @DatabaseName = ''
    BEGIN
        SET @DatabaseCursor = CURSOR FOR
        SELECT name
        FROM sys.databases
        WHERE state_desc = 'ONLINE'
          AND name NOT IN ('tempdb', 'master', 'model', 'msdb'); -- Exclude system databases
    END
    ELSE
    BEGIN
        SET @DatabaseCursor = CURSOR FOR
        SELECT @DatabaseName;
    END

    OPEN @DatabaseCursor;

    FETCH NEXT FROM @DatabaseCursor INTO @DatabaseName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Create dynamic SQL to switch context to the specified database
        SET @Sql = 'USE ' + QUOTENAME(@DatabaseName) + '; ';
        SET @Sql += '
        IF OBJECT_ID(''tempdb..##ShrinkTargets'') IS NOT NULL
            DROP TABLE ##ShrinkTargets;

        WITH FileInfo AS (
            SELECT 
                name AS FileName,
                physical_name,
                size / 128.0 AS CurrentSizeMB,
                CAST(FILEPROPERTY(name, ''SpaceUsed'') AS BIGINT) / 128.0 AS SpaceUsedMB
            FROM sys.database_files
            WHERE name LIKE ''' + @FilePattern + '''
              AND physical_name LIKE ''' + @Drive + '''
        ),
        SpaceCalc AS (
            SELECT 
                FileName,
                CurrentSizeMB,
                SpaceUsedMB,
                CASE 
                    WHEN (SpaceUsedMB * (' + CAST(@FreePct AS VARCHAR(10)) + ' / 100.0)) > ' + CAST(@MaxMBFree AS VARCHAR(10)) + ' 
                    THEN ' + CAST(@MaxMBFree AS VARCHAR(10)) + '
                    ELSE (SpaceUsedMB * (' + CAST(@FreePct AS VARCHAR(10)) + ' / 100.0))
                END AS FreeSpaceMB
            FROM FileInfo
        )
        SELECT 
            FileName,
            CurrentSizeMB,
            SpaceUsedMB,
            FreeSpaceMB,
            (SpaceUsedMB + FreeSpaceMB) AS ShrinkToMB
        INTO ##ShrinkTargets
        FROM SpaceCalc;';

        EXEC (@Sql);

        -- Declare cursor for processing each file
        DECLARE ShrinkCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT FileName, CurrentSizeMB, ShrinkToMB
            FROM ##ShrinkTargets
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
                    SET @Sql = 'USE ' + QUOTENAME(@DatabaseName) + '; DBCC SHRINKFILE(' + QUOTENAME(@FileName) + ', ' + 
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
            RETURN;
        END CATCH

        -- Cleanup
        CLOSE ShrinkCursor;
        DEALLOCATE ShrinkCursor;

        FETCH NEXT FROM @DatabaseCursor INTO @DatabaseName;
    END

    -- Ensure the global temp table is dropped if it still exists
    IF OBJECT_ID('tempdb..##ShrinkTargets') IS NOT NULL
        DROP TABLE ##ShrinkTargets;

    CLOSE @DatabaseCursor;
    DEALLOCATE @DatabaseCursor;

    IF @Execute = 1
        PRINT 'Database file shrink operation completed.';
    ELSE
        PRINT '--Database file shrink commands generated (preview mode).';
END
GO

-- Just print the commands without executing
EXEC dbo.#sp_BatchShrinkFiles 
    @BatchShrinkSize = 100
