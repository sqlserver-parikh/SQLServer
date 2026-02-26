USE tempdb 
GO
SET QUOTED_IDENTIFIER ON;
GO
SET ANSI_NULLS ON;
GO

--------------------------------------------------------------------------------------------------------------------------------
-- Database Growth & Trend Analysis Procedure
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
-- DESCRIPTION:
--  This procedure collects file-level size and space usage data for all databases on an instance.
--  It can output results to the grid, log them to a historical table, and perform trend 
--  analysis (Linear Regression/Pivoted Growth) using either MSDB backup history or 
--  previously logged data.
--------------------------------------------------------------------------------------------------------------------------------
-- PARAMETERS:
--  @LogToTable:          1 = Save snapshot to dbo.tblDBGrowth. 0 = Output current stats to results grid.
--  @Retention:           Duration to keep data in history table. Examples: '2y' (years), '6m' (months), '30d' (days).
--                        Supports combinations like '1y6m'. Default is '2y'.
--  @GrowthTrendAnalysis: 0 = No Trend Analysis (Default).
--                        1 = Use msdb.dbo.backupset (Immediate history based on backup sizes).
--                        2 = Use dbo.tblDBGrowth (Accurate history based on logged snapshots).
--  @Debug:               1 = Print the dynamic SQL without executing it.
--------------------------------------------------------------------------------------------------------------------------------
-- EXAMPLES:
--  1. Perform a manual check of current database sizes:
--     EXEC [dbo].[usp_DBGrowthData] @LogToTable = 0, @GrowthTrendAnalysis = 0;
--
--  2. Run as a daily SQL Agent Job to track growth and purge data older than 1 year:
--     EXEC [dbo].[usp_DBGrowthData] @LogToTable = 1, @Retention = '1y';
--
--  3. Get a 12-month growth projection based on Backup History:
--     EXEC [dbo].[usp_DBGrowthData] @GrowthTrendAnalysis = 1;
--------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('[dbo].[usp_DBGrowthData]', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE [dbo].[usp_DBGrowthData] AS RETURN 0;');
END
GO

ALTER PROCEDURE [dbo].[usp_DBGrowthData]
    @LogToTable BIT = 1,
    @Retention NVARCHAR(20) = '2y',
    @GrowthTrendAnalysis TINYINT = 0,
    @Debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CurrentDate DATETIME2 = SYSUTCDATETIME();
    DECLARE @RetentionDate DATETIME2;
    DECLARE @ErrorMsg NVARCHAR(2048);

    --------------------------------------------------------------------------------
    -- 1. Parse Retention String (Handles '2y', '6m', '30d' and combinations)
    --------------------------------------------------------------------------------
    BEGIN
        DECLARE @Years INT = 0, @Months INT = 0, @Days INT = 0;
        
        -- Extract Year
        IF PATINDEX('%y%', @Retention) > 0
            SET @Years = TRY_CAST(LEFT(@Retention, PATINDEX('%y%', @Retention) - 1) AS INT);
        
        -- Extract Month
        IF PATINDEX('%m%', @Retention) > 0
        BEGIN
            DECLARE @mStart INT = ISNULL(NULLIF(PATINDEX('%y%', @Retention), 0), 0) + 1;
            SET @Months = TRY_CAST(SUBSTRING(@Retention, @mStart, PATINDEX('%m%', @Retention) - @mStart) AS INT);
        END

        -- Extract Day
        IF PATINDEX('%d%', @Retention) > 0
        BEGIN
            DECLARE @dStart INT = ISNULL(NULLIF(PATINDEX('%m%', @Retention), 0), ISNULL(NULLIF(PATINDEX('%y%', @Retention), 0), 0)) + 1;
            SET @Days = TRY_CAST(SUBSTRING(@Retention, @dStart, PATINDEX('%d%', @Retention) - @dStart) AS INT);
        END

        SET @RetentionDate = DATEADD(DAY, -ISNULL(@Days,0), DATEADD(MONTH, -ISNULL(@Months,0), DATEADD(YEAR, -ISNULL(@Years,2), @CurrentDate)));
    END

    --------------------------------------------------------------------------------
    -- 2. Schema Maintenance
    --------------------------------------------------------------------------------
    IF @LogToTable = 1 AND OBJECT_ID(N'dbo.tblDBGrowth', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.tblDBGrowth (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            DatabaseName SYSNAME,
            FileID INT,
            FileType NVARCHAR(60),
            FileSizeMB DECIMAL(18,2),
            UsedSpaceMB DECIMAL(18,2),
            FreeSpaceMB AS (FileSizeMB - UsedSpaceMB),
            FileGroupName SYSNAME NULL,
            PhysicalPath NVARCHAR(260),
            LogDate DATETIME2 DEFAULT SYSUTCDATETIME()
        );
        CREATE INDEX IX_tblDBGrowth_LogDate ON dbo.tblDBGrowth(LogDate);
    END

    --------------------------------------------------------------------------------
    -- 3. Trend Analysis Logic (Dynamic Pivot)
    --------------------------------------------------------------------------------
    IF @GrowthTrendAnalysis IN (1, 2)
    BEGIN
        DECLARE @PivotColumns NVARCHAR(MAX), @SelectColumns NVARCHAR(MAX), @TrendSQL NVARCHAR(MAX);
        
        -- Dynamically build column headers for the last 12 months
        ;WITH Months AS (
            SELECT 0 as M UNION ALL SELECT M - 1 FROM Months WHERE M > -12
        )
        SELECT 
            @PivotColumns = STRING_AGG(QUOTENAME(M), ',') WITHIN GROUP (ORDER BY M DESC),
            @SelectColumns = STRING_AGG('CAST(PVT.' + QUOTENAME(M) + ' AS DECIMAL(18,2)) AS ' + 
                              QUOTENAME(FORMAT(DATEADD(MONTH, M, @CurrentDate), 'MMM_yy') + '_MB'), ', ') 
                              WITHIN GROUP (ORDER BY M DESC)
        FROM Months;

        SET @TrendSQL = '
        WITH TrendData AS (
            SELECT
                ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.database_name' ELSE 'g.DatabaseName' END + ' AS DatabaseName,
                DATEDIFF(MONTH, GETUTCDATE(), ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.backup_start_date' ELSE 'g.LogDate' END + ') AS MonthsAgo,
                AVG(' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bf.file_size / 1048576.0' ELSE 'g.UsedSpaceMB' END + ') AS AvgSizeMB
            FROM ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'msdb.dbo.backupset bs JOIN msdb.dbo.backupfile bf ON bs.backup_set_id = bf.backup_set_id' ELSE 'dbo.tblDBGrowth g' END + '
            WHERE ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.backup_start_date' ELSE 'g.LogDate' END + ' > DATEADD(YEAR, -1, GETUTCDATE())
              AND ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bf.file_type = ''D''' ELSE '1=1' END + '
            GROUP BY ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.database_name, DATEDIFF(MONTH, GETUTCDATE(), bs.backup_start_date)' ELSE 'g.DatabaseName, DATEDIFF(MONTH, GETUTCDATE(), g.LogDate)' END + '
        )
        SELECT 
            PVT.DatabaseName,
            ' + @SelectColumns + ',
            CAST(PVT.[0] - COALESCE(PVT.[-12], PVT.[-6], PVT.[-1]) AS DECIMAL(18,2)) AS NetAnnualGrowthMB,
            CAST(((PVT.[0] - COALESCE(PVT.[-12], PVT.[-6], PVT.[-1])) / 12.0) AS DECIMAL(18,2)) AS EstMonthlyGrowthMB,
            CAST(PVT.[0] + ((PVT.[0] - COALESCE(PVT.[-12], PVT.[-6], PVT.[-1])) / 12.0 * 6) AS DECIMAL(18,2)) AS Projected_6Mo_SizeMB
        FROM (SELECT DatabaseName, MonthsAgo, AvgSizeMB FROM TrendData) AS src
        PIVOT (SUM(AvgSizeMB) FOR MonthsAgo IN (' + @PivotColumns + ')) AS PVT
        ORDER BY NetAnnualGrowthMB DESC;';

        IF @Debug = 1 PRINT @TrendSQL ELSE EXEC sp_executesql @TrendSQL;
        RETURN;
    END

    --------------------------------------------------------------------------------
    -- 4. Current Snapshot Collection
    --------------------------------------------------------------------------------
    BEGIN TRY
        IF OBJECT_ID('tempdb..#Snapshot') IS NOT NULL DROP TABLE #Snapshot;
        CREATE TABLE #Snapshot (
            DatabaseName SYSNAME,
            FileID INT,
            FileType NVARCHAR(60),
            FileSizeMB DECIMAL(18,2),
            UsedSpaceMB DECIMAL(18,2),
            FileGroupName SYSNAME NULL,
            PhysicalPath NVARCHAR(260)
        );

        DECLARE @DBName SYSNAME, @SQL NVARCHAR(MAX);
        DECLARE db_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT name FROM sys.databases 
            WHERE state_desc = 'ONLINE' 
              AND user_access_desc = 'MULTI_USER'
              AND name <> 'tempdb';

        OPEN db_cur;
        FETCH NEXT FROM db_cur INTO @DBName;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SQL = 'USE ' + QUOTENAME(@DBName) + ';
            SELECT 
                DB_NAME(), file_id, type_desc,
                CAST(size / 128.0 AS DECIMAL(18,2)),
                CAST(FILEPROPERTY(name, ''SpaceUsed'') / 128.0 AS DECIMAL(18,2)),
                ISNULL(FILEGROUP_NAME(data_space_id), ''LOG''),
                physical_name
            FROM sys.database_files;';
            
            INSERT INTO #Snapshot EXEC(@SQL);
            FETCH NEXT FROM db_cur INTO @DBName;
        END
        CLOSE db_cur;
        DEALLOCATE db_cur;

        --------------------------------------------------------------------------------
        -- 5. Finalize (Logging and Cleanup)
        --------------------------------------------------------------------------------
        IF @LogToTable = 1
        BEGIN
            -- Clean up old data
            DELETE FROM dbo.tblDBGrowth WHERE LogDate < @RetentionDate;
            
            -- Insert new snapshot
            INSERT INTO dbo.tblDBGrowth (DatabaseName, FileID, FileType, FileSizeMB, UsedSpaceMB, FileGroupName, PhysicalPath)
            SELECT * FROM #Snapshot;
            
            PRINT 'Snapshot logged. History older than ' + CAST(@RetentionDate AS NVARCHAR(30)) + ' purged.';
        END
        ELSE
        BEGIN
            SELECT *, @CurrentDate AS SnapshotDate FROM #Snapshot ORDER BY DatabaseName, FileID;
        END
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = 'usp_DBGrowthData Error: ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 16, 1);
    END CATCH

    IF OBJECT_ID('tempdb..#Snapshot') IS NOT NULL DROP TABLE #Snapshot;
END
GO
