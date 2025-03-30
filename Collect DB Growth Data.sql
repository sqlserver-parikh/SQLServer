use tempdb
go
create or ALTER PROCEDURE [dbo].[usp_DBGrowthData]
    @LogToTable BIT = 1,
    @Retention NVARCHAR(20) = '2y',
    @GrowthTrendAnalysis TINYINT = 2
		--   @GrowthTrendAnalysis TINYINT = 0
--     - 0: Skip growth trend
--     - 1: Use msdb backupset for trend
--     - 2: Use tblDBGrowth for trend
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @StartDate DATE = GETUTCDATE();

        -- Parse @Retention string (supports '2y3m10d')
        DECLARE @Years INT = 0, @Months INT = 0, @Days INT = 0;
        IF @Retention LIKE '%y%'
            SET @Years = TRY_CAST(LEFT(@Retention, CHARINDEX('y', @Retention) - 1) AS INT);
        IF @Retention LIKE '%m%'
            SET @Months = TRY_CAST(SUBSTRING(@Retention, CHARINDEX('y', @Retention) + 1, CHARINDEX('m', @Retention) - CHARINDEX('y', @Retention) - 1) AS INT);
        IF @Retention LIKE '%d%'
            SET @Days = TRY_CAST(SUBSTRING(@Retention, CHARINDEX('m', @Retention) + 1, CHARINDEX('d', @Retention) - CHARINDEX('m', @Retention) - 1) AS INT);

        DECLARE @RetentionDate DATE = DATEADD(DAY, -@Days, DATEADD(MONTH, -@Months, DATEADD(YEAR, -@Years, GETUTCDATE())));

        IF @GrowthTrendAnalysis IN (1, 2)
        BEGIN
            DECLARE @TrendSQL NVARCHAR(MAX);
            SET @TrendSQL = '
            WITH TrendData AS (
                SELECT
                    ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.database_name' ELSE 'g.DatabaseName' END + ' AS DatabaseName,
                    DATEDIFF(MONTH, GETUTCDATE(), ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.backup_start_date' ELSE 'g.LogDate' END + ') AS MonthsAgo,
                    AVG(' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bf.file_size / 1048576.0' ELSE 'g.UsedSpaceMB' END + ') AS AvgSizeMB
                FROM ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'msdb.dbo.backupset bs JOIN msdb.dbo.backupfile bf ON bs.backup_set_id = bf.backup_set_id' ELSE 'dbo.tblDBGrowth g' END + '
                WHERE ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.backup_start_date' ELSE 'g.LogDate' END + ' BETWEEN DATEADD(YEAR, -1, GETUTCDATE()) AND GETUTCDATE()
                      AND ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bf.file_type = ''D'' AND bs.database_name' ELSE 'g.DatabaseName' END + ' NOT IN (''master'', ''tempdb'', ''model'', ''msdb'')
                GROUP BY ' + CASE WHEN @GrowthTrendAnalysis = 1 THEN 'bs.database_name, DATEDIFF(MONTH, GETUTCDATE(), bs.backup_start_date)' ELSE 'g.DatabaseName, DATEDIFF(MONTH, GETUTCDATE(), g.LogDate)' END + '
            ),
            DatabaseMonthCounts AS (
                SELECT 
                    DatabaseName,
                    COUNT(DISTINCT MonthsAgo) AS MonthsWithData
                FROM TrendData
                GROUP BY DatabaseName
            )
            SELECT 
                PVT.DatabaseName,
                PVT.[0] AS CurrentMonthMB,
                PVT.[-1], PVT.[-2], PVT.[-3], PVT.[-4], PVT.[-5], PVT.[-6],
                PVT.[-7], PVT.[-8], PVT.[-9], PVT.[-10], PVT.[-11], PVT.[-12],
                CASE 
                    WHEN dmc.MonthsWithData > 1 THEN 
                        (PVT.[0] - COALESCE(PVT.[-12], PVT.[-11], PVT.[-10], PVT.[-9], PVT.[-8],
                                 PVT.[-7], PVT.[-6], PVT.[-5], PVT.[-4], PVT.[-3], PVT.[-2], PVT.[-1]))
                    ELSE NULL -- No growth calculation possible with only one month
                END AS LastXMonthGrowth,
                dmc.MonthsWithData AS XMonthsData,
                CASE 
                    WHEN dmc.MonthsWithData > 1 THEN
                        PVT.[0] + ((PVT.[0] - COALESCE(PVT.[-12], PVT.[-11], PVT.[-10], PVT.[-9], PVT.[-8],
                                 PVT.[-7], PVT.[-6], PVT.[-5], PVT.[-4], PVT.[-3], PVT.[-2], PVT.[-1])) /
                                 (dmc.MonthsWithData - 1)) * 1
                    ELSE NULL -- Cannot project with only one month of data
                END AS Projected_1Month,
                CASE 
                    WHEN dmc.MonthsWithData > 1 THEN
                        PVT.[0] + ((PVT.[0] - COALESCE(PVT.[-12], PVT.[-11], PVT.[-10], PVT.[-9], PVT.[-8],
                                 PVT.[-7], PVT.[-6], PVT.[-5], PVT.[-4], PVT.[-3], PVT.[-2], PVT.[-1])) /
                                 (dmc.MonthsWithData - 1)) * 3
                    ELSE NULL -- Cannot project with only one month of data
                END AS Projected_3Month,
                CASE 
                    WHEN dmc.MonthsWithData > 1 THEN
                        PVT.[0] + ((PVT.[0] - COALESCE(PVT.[-12], PVT.[-11], PVT.[-10], PVT.[-9], PVT.[-8],
                                 PVT.[-7], PVT.[-6], PVT.[-5], PVT.[-4], PVT.[-3], PVT.[-2], PVT.[-1])) /
                                 (dmc.MonthsWithData - 1)) * 6
                    ELSE NULL -- Cannot project with only one month of data
                END AS Projected_6Month,
                CASE 
                    WHEN dmc.MonthsWithData > 1 THEN
                        PVT.[0] + ((PVT.[0] - COALESCE(PVT.[-12], PVT.[-11], PVT.[-10], PVT.[-9], PVT.[-8],
                                 PVT.[-7], PVT.[-6], PVT.[-5], PVT.[-4], PVT.[-3], PVT.[-2], PVT.[-1])) /
                                 (dmc.MonthsWithData - 1)) * 12
                    ELSE NULL -- Cannot project with only one month of data
                END AS Projected_12Month
            FROM (
                SELECT DatabaseName, MonthsAgo, AvgSizeMB FROM TrendData
            ) AS raw
            PIVOT (
                SUM(AvgSizeMB) FOR MonthsAgo IN ([0], [-1], [-2], [-3], [-4], [-5], [-6], [-7], [-8], [-9], [-10], [-11], [-12])
            ) AS PVT
            JOIN DatabaseMonthCounts dmc ON PVT.DatabaseName = dmc.DatabaseName;
            ';

            EXEC sp_executesql @TrendSQL;
            RETURN;
        END

        -- [Rest of the stored procedure remains unchanged]
        
        IF @LogToTable = 1 AND OBJECT_ID(N'dbo.tblDBGrowth', 'U') IS NULL
        BEGIN
            create TABLE dbo.tblDBGrowth
            (
                DBID INT NOT NULL,
                FileID INT NOT NULL,
                FileType INT NOT NULL,
                FileSizeMB INT NOT NULL,
                UsedSpaceMB INT,
                DatabaseName NVARCHAR(200),
                LogicalFileName NVARCHAR(128),
                FileGroupName SYSNAME NULL,
                DataSpaceID SMALLINT,
                FilePath NVARCHAR(260),
                LogDate DATETIME2(7) NOT NULL,
                StateDesc NVARCHAR(60),
                RecoveryModel NVARCHAR(60),
                CompatibilityLevel INT
            );
        END

        CREATE TABLE #SnapshotTable
        (
            DBID INT,
            FileID INT,
            FileType INT,
            FileSizeMB INT,
            UsedSpaceMB INT NULL,
            DatabaseName NVARCHAR(200),
            LogicalFileName NVARCHAR(128),
            FileGroupName SYSNAME NULL,
            DataSpaceID SMALLINT,
            FilePath NVARCHAR(260),
            LogDate DATETIME2(7),
            StateDesc NVARCHAR(60),
            RecoveryModel NVARCHAR(60),
            CompatibilityLevel INT
        );

        INSERT INTO #SnapshotTable
        SELECT
            mf.database_id,
            mf.file_id,
            mf.type,
            CEILING(mf.size * 1.0 / 128),
            NULL,
            DB_NAME(mf.database_id),
            mf.name,
            NULL,
            NULL,
            mf.physical_name,
            SYSUTCDATETIME(),
            d.state_desc,
            d.recovery_model_desc,
            d.compatibility_level
        FROM sys.master_files mf
        JOIN sys.databases d ON d.database_id = mf.database_id
        WHERE DB_NAME(mf.database_id) NOT IN ('master', 'tempdb', 'model', 'msdb');

        DECLARE @DBName NVARCHAR(255), @SQL NVARCHAR(MAX);
        DECLARE db_cursor CURSOR FOR
        SELECT name FROM sys.databases
        WHERE HAS_DBACCESS(name) = 1 AND state_desc = 'ONLINE' AND is_read_only = 0
          AND user_access_desc = 'MULTI_USER' AND is_in_standby = 0
          AND name NOT IN ('master', 'tempdb', 'model', 'msdb');

        OPEN db_cursor;
        FETCH NEXT FROM db_cursor INTO @DBName;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SQL = '
            USE ' + QUOTENAME(@DBName) + ';
            UPDATE s
            SET s.UsedSpaceMB = CEILING(df.size / 128.0) - CEILING(df.size / 128.0 - CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS INT) / 128.0),
                s.FileGroupName = ISNULL(fg.name, ''LogFile''),
                s.DataSpaceID = ISNULL(fg.data_space_id, 0)
            FROM #SnapshotTable s
            JOIN sys.database_files df ON DB_ID(s.DatabaseName) = DB_ID() AND s.FileID = df.file_id
            LEFT JOIN sys.filegroups fg ON df.data_space_id = fg.data_space_id
            WHERE s.DatabaseName = ''' + @DBName + '''';

            EXEC(@SQL);
            FETCH NEXT FROM db_cursor INTO @DBName;
        END
        CLOSE db_cursor;
        DEALLOCATE db_cursor;

        IF @LogToTable = 1
        BEGIN
            DELETE FROM dbo.tblDBGrowth WHERE LogDate < @RetentionDate;
            INSERT INTO dbo.tblDBGrowth SELECT * FROM #SnapshotTable;
        END
        ELSE
        BEGIN
            SELECT * FROM #SnapshotTable;
        END

        DROP TABLE #SnapshotTable;
    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(MAX), @ErrSeverity INT;
        SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY();
        RAISERROR('usp_DBGrowthData failed: %s', @ErrSeverity, 1, @ErrMsg);
    END CATCH
END
