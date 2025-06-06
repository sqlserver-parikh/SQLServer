USE tempdb
GO
CREATE OR ALTER PROCEDURE dbo.usp_BatchShrinkFiles  
    @DatabaseName varchar(128) = '', -- Database name to target, 'ALL' Or Empty '' Or NULL for all databases  
    @BatchShrinkSize int = 10000,   -- Size in MB for each shrink operation  
    @FreePct int = 10,              -- Percentage of free space to maintain  
    @MaxMBFree int = 10240,         -- Maximum free space in MB (Default 10GB)  
    @FileName varchar(128) = '',    -- Specific filename, 'ALL' Or Empty '' Or NULL for all files  
    @Drive varchar(128) = '',       -- Drive or mount point to target, 'ALL' Or Empty '' Or NULL for all drives  
    @Execute bit = 1,               -- 1 = execute commands, 0 = print only  
    @LockTimeout int = 180000,      -- Lock timeout in milliseconds (default 3 minutes)
    @TimeLimit int = 600,           -- Each file will be shrink for max 10mins
    @LogTable bit = 1             -- 0 = don't create/log to table, 1 = create and log to table
AS  
BEGIN  
    SET NOCOUNT ON;  
  
    DECLARE @CurrentSize DECIMAL(20, 2);  
    DECLARE @LoopCurrentSize DECIMAL(20, 2);  
    DECLARE @ShrinkTo DECIMAL(20, 2);  
    DECLARE @Sql NVARCHAR(MAX);  
    DECLARE @FilePattern VARCHAR(128);  
    DECLARE @PhysicalName VARCHAR(512);  
    DECLARE @InitialFreeSpace DECIMAL(20, 2); -- Free space before shrinking  
    DECLARE @CurrentFreeSpace DECIMAL(20, 2); -- Free space after current shrink  
    DECLARE @FinalFreeSpace DECIMAL(20, 2);   -- Free space after complete batch  
    DECLARE @SpaceUsedMB DECIMAL(20, 2);      -- Space used in MB  
    DECLARE @IStartTime datetime;

    SET @IStartTime = GETDATE();
      
    -- Logging variables  
    DECLARE @LogID BIGINT;  
    DECLARE @StartTime DATETIME2(7);  
    DECLARE @EndTime DATETIME2(7);  
    DECLARE @FinalSize DECIMAL(20, 2);  
    DECLARE @CurrentDB VARCHAR(128);  
    DECLARE @ElapsedSeconds DECIMAL(10, 2);  
  
    -- Handle the ALL parameter, including NULL and empty string  
    SET @FilePattern = CASE   
        WHEN UPPER(@FileName) = 'ALL' THEN '%'   
        WHEN @FileName IS NULL THEN '%'  
        WHEN @FileName = '' THEN '%'   
        ELSE @FileName   
    END;  
  
    -- Create logging table if it doesn't exist and @LogTable = 1  
    IF @LogTable = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblShrinkFileLog]') AND type in (N'U'))  
        BEGIN  
            CREATE TABLE [dbo].[tblShrinkFileLog] (  
                [LogID] [bigint] IDENTITY(1,1) NOT NULL,  
                [DatabaseName] [varchar](128) NOT NULL,  
                [FileName] [varchar](128) NOT NULL,  
                [PhysicalName] [varchar](512) NOT NULL,  
                [ShrinkStartTime] [datetime2](7) NOT NULL,  
                [ShrinkEndTime] [datetime2](7) NULL,  
                [DurationSeconds] [decimal](10,2) NULL,  
                [InitialSize_MB] [decimal](20,2) NOT NULL,  
                [FinalSize_MB] [decimal](20,2) NULL,  
                [TargetSize_MB] [decimal](20,2) NOT NULL,  
                [ShrinkIncrement_MB] [int] NOT NULL,  
                [TargetFreeSpace_Pct] [int] NOT NULL,  
                [MaxFreeSpace_MB] [int] NOT NULL,  
                [InitialFreeSpace_MB] [decimal](20,2) NULL,  
                [CurrentFreeSpace_MB] [decimal](20,2) NULL,  
                [FinalFreeSpace_MB] [decimal](20,2) NULL,  
                [ShrinkCommand] [nvarchar](max) NOT NULL,  
                [ExecuteMode] [bit] NOT NULL,  
                [Status] [varchar](20) NOT NULL DEFAULT 'InProgress',  
                [ErrorMessage] [nvarchar](max) NULL,  
                [CreatedDate] [datetime2](7) NOT NULL DEFAULT GETDATE(),  
                CONSTRAINT [PK_tblShrinkFileLog] PRIMARY KEY CLUSTERED ([LogID] ASC)  
            );  
              
            -- Create indexes for better query performance  
            CREATE NONCLUSTERED INDEX [IX_tblShrinkFileLog_DatabaseName] ON [dbo].[tblShrinkFileLog] ([DatabaseName]);  
            CREATE NONCLUSTERED INDEX [IX_tblShrinkFileLog_ShrinkStartTime] ON [dbo].[tblShrinkFileLog] ([ShrinkStartTime]);  
            CREATE NONCLUSTERED INDEX [IX_tblShrinkFileLog_Status] ON [dbo].[tblShrinkFileLog] ([Status]);  
              
            PRINT '-- tblShrinkFileLog table created successfully with indexes.';  
        END  
        ELSE  
        BEGIN  
            PRINT '-- tblShrinkFileLog table already exists.';  
        END  
    END
  
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
        SET @CurrentDB = @DatabaseName;  
          
  
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
                CAST(FILEPROPERTY(name, ''SpaceUsed'') AS BIGINT) / 128.0 AS SpaceUsedMB,  
                (size / 128.0) - (CAST(FILEPROPERTY(name, ''SpaceUsed'') AS BIGINT) / 128.0) AS FreeSpaceMB  
            FROM sys.database_files  
            WHERE name LIKE ''' + @FilePattern + '''  
              AND physical_name LIKE ''' + @Drive + '''  
        ),  
        SpaceCalc AS (  
            SELECT   
                FileName,  
                physical_name,  
                CurrentSizeMB,  
                SpaceUsedMB,  
                FreeSpaceMB,  
                CASE   
                    WHEN (SpaceUsedMB * (' + CAST(@FreePct AS VARCHAR(10)) + ' / 100.0)) > ' + CAST(@MaxMBFree AS VARCHAR(10)) + '   
                    THEN ' + CAST(@MaxMBFree AS VARCHAR(10)) + '  
                    ELSE (SpaceUsedMB * (' + CAST(@FreePct AS VARCHAR(10)) + ' / 100.0))  
                END AS TargetFreeSpaceMB  
            FROM FileInfo  
        )  
        SELECT   
            FileName,  
            physical_name,  
            CurrentSizeMB,  
            SpaceUsedMB,  
            FreeSpaceMB,  
            TargetFreeSpaceMB,  
            (SpaceUsedMB + TargetFreeSpaceMB) AS ShrinkToMB  
        INTO ##ShrinkTargets  
        FROM SpaceCalc;';  
  
        EXEC (@Sql);  
  
        -- Declare cursor for processing each file  
        DECLARE ShrinkCursor CURSOR LOCAL FAST_FORWARD FOR  
            SELECT FileName, physical_name, CurrentSizeMB, ShrinkToMB, FreeSpaceMB  
            FROM ##ShrinkTargets  
            ORDER BY CurrentSizeMB DESC;  
  
        BEGIN TRY  
            OPEN ShrinkCursor;  
            FETCH NEXT FROM ShrinkCursor INTO @FileName, @PhysicalName, @CurrentSize, @ShrinkTo, @InitialFreeSpace;  
  
            WHILE @@FETCH_STATUS = 0  
            BEGIN  
                SET @LoopCurrentSize = @CurrentSize;  
                SET @FinalFreeSpace = @InitialFreeSpace; -- Initialize for batch tracking  
                  
                -- Print initial stats  
                PRINT '-- Processing file: ' + QUOTENAME(@FileName) + ' in database: ' + QUOTENAME(@CurrentDB);  
                PRINT '-- Initial Size: ' + CAST(@CurrentSize AS VARCHAR(20)) + ' MB';  
                PRINT '-- Initial Free Space: ' + CAST(@InitialFreeSpace AS VARCHAR(20)) + ' MB';  
                PRINT '-- Target Size: ' + CAST(@ShrinkTo AS VARCHAR(20)) + ' MB';  
                  
                -- Skip if file is already smaller than target size  
                IF @CurrentSize <= @ShrinkTo   
                BEGIN  
                    PRINT '-- File ' + QUOTENAME(@FileName) + ' is already at or below target size.';  
                    FETCH NEXT FROM ShrinkCursor INTO @FileName, @PhysicalName, @CurrentSize, @ShrinkTo, @InitialFreeSpace;  
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
  
                    -- Calculate current free space (approximation before actual shrink)  
                    SET @CurrentFreeSpace = @InitialFreeSpace - (@CurrentSize - @LoopCurrentSize);  
  
                    -- Generate shrink command  
                    SET @Sql = 'USE ' + QUOTENAME(@CurrentDB) + '; SET LOCK_TIMEOUT ' + CAST(@LockTimeout AS VARCHAR(20)) + '; DBCC SHRINKFILE(' + QUOTENAME(@FileName) + ', ' +   
                              CAST(FLOOR(@LoopCurrentSize) AS VARCHAR(20)) + ')';  
                      
                    -- Record start time and insert initial log entry if @LogTable = 1  
                    SET @StartTime = GETDATE();  
                    IF @LogTable = 1
                    BEGIN  
                        INSERT INTO [dbo].[tblShrinkFileLog] (  
                            [DatabaseName], [FileName], [PhysicalName], [ShrinkStartTime],   
                            [InitialSize_MB], [TargetSize_MB], [ShrinkIncrement_MB],   
                            [TargetFreeSpace_Pct], [MaxFreeSpace_MB], [InitialFreeSpace_MB],   
                            [CurrentFreeSpace_MB], [ShrinkCommand], [ExecuteMode], [Status]  
                        )  
                        VALUES (  
                            @CurrentDB, @FileName, @PhysicalName, @StartTime,  
                            @CurrentSize, @LoopCurrentSize, @BatchShrinkSize,  
                            @FreePct, @MaxMBFree, @InitialFreeSpace,  
                            @CurrentFreeSpace, @Sql, @Execute, 'InProgress'  
                        );  
                          
                        SET @LogID = SCOPE_IDENTITY();  
                    END
                      
                    -- Execute or print based on @Execute parameter  
                    IF @Execute = 1  
                    BEGIN  
                        PRINT '-- Executing: ' + @Sql;  
                          
                        BEGIN TRY  
                            EXEC (@Sql);  
                            SET @EndTime = GETDATE();  
                            SET @ElapsedSeconds = DATEDIFF(MILLISECOND, @StartTime, @EndTime) / 1000.0;  
                              
                            -- Get the actual size and free space after shrink operation  
                            DECLARE @GetSizeSQL NVARCHAR(MAX);  
                            SET @GetSizeSQL = 'USE ' + QUOTENAME(@CurrentDB) + ';   
                                SELECT @FinalSize = size / 128.0,  
                                       @SpaceUsedMB = CAST(FILEPROPERTY(name, ''SpaceUsed'') AS BIGINT) / 128.0  
                                FROM sys.database_files   
                                WHERE name = ''' + @FileName + '''';  
                              
                            EXEC sp_executesql @GetSizeSQL,   
                                N'@FinalSize DECIMAL(20,2) OUTPUT, @SpaceUsedMB DECIMAL(20,2) OUTPUT',   
                                @FinalSize OUTPUT, @SpaceUsedMB OUTPUT;  
                              
                            SET @CurrentFreeSpace = @FinalSize - @SpaceUsedMB;  
                            SET @FinalFreeSpace = @CurrentFreeSpace; -- Update for final batch  
                              
                            -- Update log entry with completion details if @LogTable = 1  
                            IF @LogTable = 1
                            BEGIN
                                UPDATE [dbo].[tblShrinkFileLog]   
                                SET [ShrinkEndTime] = @EndTime,  
                                    [DurationSeconds] = @ElapsedSeconds,  
                                    [FinalSize_MB] = @FinalSize,  
                                    [CurrentFreeSpace_MB] = @CurrentFreeSpace,  
                                    [FinalFreeSpace_MB] = @FinalFreeSpace,  
                                    [Status] = 'Completed'  
                                WHERE [LogID] = @LogID;  
                            END
                              
                            PRINT '-- Shrink completed for ' + QUOTENAME(@FileName) + ':';  
                            PRINT '-- Current Size After Shrink: ' + CAST(@FinalSize AS VARCHAR(20)) + ' MB';  
                            PRINT '-- Current Free Space After Shrink: ' + CAST(@CurrentFreeSpace AS VARCHAR(20)) + ' MB';  
                            PRINT '-- Duration: ' + CAST(@ElapsedSeconds AS VARCHAR(10)) + ' seconds';  
                            IF (GETDATE() > DATEADD(ss, @TimeLimit, @IStartTime) OR @TimeLimit IS NULL)
                            BEGIN
                                SET @IStartTime = GETDATE();
                                BREAK;
                            END;
                            WAITFOR DELAY '00:00:01';  
                        END TRY  
                        BEGIN CATCH  
                            SET @EndTime = GETDATE();  
                            SET @ElapsedSeconds = DATEDIFF(MILLISECOND, @StartTime, @EndTime) / 1000.0;  
                              
                            -- Check for lock timeout error (1222)  
                            IF ERROR_NUMBER() = 1222  
                            BEGIN  
                                -- Update log entry with lock timeout details if @LogTable = 1  
                                IF @LogTable = 1
                                BEGIN
                                    UPDATE [dbo].[tblShrinkFileLog]   
                                    SET [ShrinkEndTime] = @EndTime,  
                                        [DurationSeconds] = @ElapsedSeconds,  
                                        [Status] = 'Skipped',  
                                        [ErrorMessage] = 'Lock timeout occurred (' + CAST(@LockTimeout AS VARCHAR(20)) + 'ms). Skipping file.'  
                                    WHERE [LogID] = @LogID;  
                                END
                                  
                                PRINT '-- Lock timeout occurred for ' + QUOTENAME(@FileName) + ' after ' + CAST(@LockTimeout AS VARCHAR(20)) + 'ms. Skipping file.';  
                                FETCH NEXT FROM ShrinkCursor INTO @FileName, @PhysicalName, @CurrentSize, @ShrinkTo, @InitialFreeSpace;  
                                CONTINUE;  
                            END  
                            ELSE  
                            BEGIN  
                                -- Update log entry with other error details if @LogTable = 1  
                                IF @LogTable = 1
                                BEGIN
                                    UPDATE [dbo].[tblShrinkFileLog]   
                                    SET [ShrinkEndTime] = @EndTime,  
                                        [DurationSeconds] = @ElapsedSeconds,  
                                        [Status] = 'Failed',  
                                        [ErrorMessage] = ERROR_MESSAGE()  
                                    WHERE [LogID] = @LogID;  
                                END
                                  
                                PRINT '-- Error during shrink operation for ' + QUOTENAME(@FileName) + ': ' + ERROR_MESSAGE();  
                                -- Continue with next iteration  
                            END  
                        END CATCH  
                    END  
                    ELSE  
                    BEGIN  
                        PRINT '-- ' + @Sql;  
                        SET @EndTime = GETDATE();  
                        SET @ElapsedSeconds = DATEDIFF(MILLISECOND, @StartTime, @EndTime) / 1000.0;  
                          
                        -- Update log entry for preview mode if @LogTable = 1  
                        IF @LogTable = 1
                        BEGIN
                            UPDATE [dbo].[tblShrinkFileLog]   
                            SET [ShrinkEndTime] = @EndTime,  
                                [DurationSeconds] = @ElapsedSeconds,  
                                [FinalSize_MB] = @LoopCurrentSize, -- Projected size  
                                [CurrentFreeSpace_MB] = @CurrentFreeSpace,  
                                [FinalFreeSpace_MB] = @CurrentFreeSpace, -- Projected final free space  
                                [Status] = 'Preview'  
                            WHERE [LogID] = @LogID;  
                        END
                          
                        PRINT '-- Preview mode for ' + QUOTENAME(@FileName) + ':';  
                        PRINT '-- Projected Size After Shrink: ' + CAST(@LoopCurrentSize AS VARCHAR(20)) + ' MB';  
                        PRINT '-- Projected Free Space After Shrink: ' + CAST(@CurrentFreeSpace AS VARCHAR(20)) + ' MB';  
                    END  
                END  
  
                -- Print final stats for the file after the batch is complete  
                PRINT '-- Final Size for ' + QUOTENAME(@FileName) + ': ' + CAST(@FinalSize AS VARCHAR(20)) + ' MB';  
                PRINT '-- Final Free Space for ' + QUOTENAME(@FileName) + ': ' + CAST(@FinalFreeSpace AS VARCHAR(20)) + ' MB';  
                PRINT '-- --------------------------------------------------';  
  
                FETCH NEXT FROM ShrinkCursor INTO @FileName, @PhysicalName, @CurrentSize, @ShrinkTo, @InitialFreeSpace;  
            END  
        END TRY  
        BEGIN CATCH  
            PRINT '-- Error occurred: ' + ERROR_MESSAGE();  
              
            -- Update any in-progress log entries to failed status if @LogTable = 1  
            IF @LogTable = 1
            BEGIN
                UPDATE [dbo].[tblShrinkFileLog]   
                SET [Status] = 'Failed',  
                    [ErrorMessage] = ERROR_MESSAGE(),  
                    [ShrinkEndTime] = GETDATE()  
                WHERE [Status] = 'InProgress'   
                  AND [DatabaseName] = @CurrentDB  
                  AND [ShrinkEndTime] IS NULL;  
            END
              
            IF CURSOR_STATUS('global', 'ShrinkCursor') >= 0  
            BEGIN  
                CLOSE ShrinkCursor;  
                DEALLOCATE ShrinkCursor;  
            END  
            -- Continue to next database instead of returning  
            FETCH NEXT FROM @DatabaseCursor INTO @DatabaseName;  
            CONTINUE;  
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
        PRINT '-- Database file shrink operation completed.' + CASE WHEN @LogTable = 1 THEN ' Check tblShrinkFileLog table for detailed results.' ELSE '' END;  
    ELSE  
        PRINT '-- Database file shrink commands generated (preview mode).' + CASE WHEN @LogTable = 1 THEN ' Check tblShrinkFileLog table for logged operations.' ELSE '' END;  
END
GO
EXEC usp_BatchShrinkFiles
GO
----Cleanup
--DROP PROCEDURE IF EXISTS usp_BatchShrinkFiles
--DROP TABLE IF EXISTS [tblShrinkFileLog]
