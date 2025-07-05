USE tempdb
GO
CREATE OR ALTER PROCEDURE usp_DiskDBSpaceReport
(    
    @Disk NVARCHAR(256) = '',   -- Specific disk or mount point to filter
    @DBName NVARCHAR(128) = '',   -- Specific database name to filter
    @LowDiskPct DECIMAL(5,2) = 100, -- Show drives below this free space percentage
    @BackupLookBackDays int = 30 --How far back backup should be checked.
)
AS
SET NOCOUNT ON;

    -- Validate parameters
    IF @LowDiskPct IS NOT NULL AND (@LowDiskPct < 0 OR @LowDiskPct > 100)
    BEGIN
        RAISERROR('Parameter @LowDiskPct must be between 0 and 100', 16, 1);
        RETURN;
    END
	IF @Disk = ''
	SET @Disk = NULL 
    IF (@Disk IS NOT NULL) AND @Disk NOT IN (
        SELECT DISTINCT volume_mount_point
        FROM sys.master_files AS f
        CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id)
    )
    BEGIN
        RAISERROR('Invalid disk drive specified', 16, 1);
        RETURN;
    END
	IF @DBName = ''
	SET @DBName = NULL
    IF (@DBName IS NOT NULL) AND NOT EXISTS (
        SELECT 1 FROM sys.databases WHERE name = @DBName
    )
    BEGIN
        RAISERROR('Invalid database name specified', 16, 1);
        RETURN;
    END

-- Create a temp table to hold transaction log file information
SELECT 
    ISNULL(d.[name], bs.[database_name]) COLLATE SQL_Latin1_General_CP1_CI_AS AS DbName,
    CASE
        WHEN d.recovery_model_desc = 'SIMPLE' THEN
            'Simple Recovery'
        ELSE
            CONVERT(VARCHAR(19),
                MAX(CASE
                    WHEN [type] = 'L' THEN
                        bs.backup_finish_date
                    ELSE
                        NULL
                    END
                ),
                121
            )
    END COLLATE SQL_Latin1_General_CP1_CI_AS AS LogBackupDate
INTO #BackupInfo
FROM sys.databases AS d WITH (NOLOCK)
    LEFT OUTER JOIN msdb.dbo.backupset AS bs WITH (NOLOCK)
        ON bs.[database_name] = d.[name]
        AND bs.backup_finish_date > GETDATE() - @BackupLookBackDays
GROUP BY 
    ISNULL(d.[name], bs.[database_name]),
    d.recovery_model_desc,
    d.log_reuse_wait_desc
ORDER BY d.recovery_model_desc
OPTION (RECOMPILE);

CREATE TABLE #VlfInfo
(
    ServerName VARCHAR(50) DEFAULT @@SERVERNAME,
    DatabaseName VARCHAR(100) NOT NULL,
    FileId INT NOT NULL,
    FileName SYSNAME NOT NULL,
    SizeMb INT NOT NULL,
    FreeMb INT NULL,
    AutogrowMb VARCHAR(20) NOT NULL,
    AutogrowType CHAR(1),
    VlfCount INT
);

CREATE TABLE #FixedDriveInfo
(
    Drive CHAR(1),
    MbFree VARCHAR(10)
);

INSERT INTO #FixedDriveInfo
EXEC xp_fixeddrives;

-- In SQL Server 2012 the DBCC LOGINFO output gained a new column
CREATE TABLE #LogInfo2008
(
    FileId INT,
    FileSize BIGINT,
    StartOffset BIGINT,
    FSeqNo BIGINT,
    Status BIGINT,
    Parity BIGINT,
    CreateLsn NUMERIC(38)
);

CREATE TABLE #LogInfo2012
(
    ResourceUnitId BIGINT,
    FileId INT,
    FileSize BIGINT,
    StartOffset BIGINT,
    FSeqNo BIGINT,
    Status BIGINT,
    Parity BIGINT,
    CreateLsn NUMERIC(38)
);

CREATE TABLE #FileGroupInfo
(
    DbName SYSNAME,
    FgName SYSNAME NULL,
    FileId INT,
    PhysicalName VARCHAR(256)
);

INSERT INTO #FileGroupInfo
EXEC sp_MSForeachdb 'USE [?];
SELECT "?", filegroup_name(data_space_id), file_id, physical_name FROM sys.database_files';

SELECT 
    db_name(database_id) DbName,
	CONVERT(DECIMAL(28, 2), CAST(SUM(CAST(size AS BIGINT)) AS FLOAT) / 128.0) AS DbSizeMb
INTO #DbSizeInfo
FROM sys.master_files
GROUP BY db_name(database_id);

SELECT
    at.transaction_id,
    at.name AS transaction_name,
    at.transaction_begin_time,
    at.transaction_type,
    at.transaction_state,
    s.session_id,
    s.login_name,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    t.text AS sql_text, s.database_id
INTO #OpenTran
FROM sys.dm_tran_active_transactions at
JOIN sys.dm_tran_session_transactions st ON at.transaction_id = st.transaction_id
JOIN sys.dm_exec_sessions s ON st.session_id = s.session_id
JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t;

-- Gather transaction log file size and auto-growth specs for each database
EXEC master.sys.sp_MSforeachdb '
    USE [?];
    INSERT INTO #VlfInfo (DatabaseName, FileId, FileName, SizeMb, FreeMb, AutogrowMb, AutogrowType)
    SELECT  
        DB_NAME(), 
        file_id, 
        name, 
        (size / 128), 
        (size - FILEPROPERTY(NAME, ''SpaceUsed'')) / 128,
        CASE WHEN is_percent_growth = 1 THEN growth ELSE (growth / 128) END, 
        CASE WHEN is_percent_growth = 1 THEN ''P'' ELSE ''M'' END
    FROM sys.database_files
    WHERE type = 1;';

EXEC master.dbo.sp_msforeachdb N'USE [?]; 
    IF (SELECT MAX(compatibility_level) FROM sys.databases) >= 110
    BEGIN
        INSERT INTO #LogInfo2012 
        EXEC sp_executesql N''DBCC LOGINFO([?]) WITH NO_INFOMSGS'';

        UPDATE #VlfInfo 
        SET VlfCount = (SELECT COUNT(*) FROM #LogInfo2012 WHERE FileId = #VlfInfo.FileId)
        WHERE DatabaseName = DB_NAME();

        TRUNCATE TABLE #LogInfo2012;    
    END
    ELSE
    BEGIN
        INSERT INTO #LogInfo2008 
        EXEC sp_executesql N''DBCC LOGINFO([?]) WITH NO_INFOMSGS'';

        UPDATE #VlfInfo 
        SET VlfCount = (SELECT COUNT(*) FROM #LogInfo2008 WHERE FileId = #VlfInfo.FileId)
        WHERE DatabaseName = DB_NAME();

        TRUNCATE TABLE #LogInfo2008;    
    END';

DECLARE @Version NUMERIC(18, 10);
SET @Version = CAST(
    LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), 
    CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX))) - 1) 
    + '.' + REPLACE(
        RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), 
        LEN(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX))) - 
        CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)))),
        '.',
        ''
    ) AS NUMERIC(18, 10)
);

IF (@Version >= 10.502200)
BEGIN
    CREATE TABLE #DbSpaceUseInfo
    (
        DbName VARCHAR(500),
        FileName VARCHAR(500),
        FileId INT,
        SpaceUsed FLOAT,
        IsPrimaryFile BIT,
        IsLogFile BIT
    );

    INSERT INTO #DbSpaceUseInfo
    EXEC ('sp_MSforeachdb''USE [?]; 
        SELECT 
            ''''?'''' DbName, 
            name FileName, 
            fileid FileId, 
            fileproperty(name,''''spaceused'''') SpaceUsed,
            fileproperty(name,''''IsPrimaryFile'''') IsPrimaryFile, 
            fileproperty(name,''''IsLogFile'''') 
        FROM sysfiles''');

    SELECT DISTINCT
        @@SERVERNAME AS ServerName,
        s.volume_mount_point AS Drive,
        CASE
            WHEN total_bytes / 1048576 > 1000 THEN
                CAST(CAST((total_bytes / 1048576) / 1024.0 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' GB'
            ELSE
                CAST(CAST(total_bytes / 1048576 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' MB'
        END AS TotalDiskSpace,
        CASE
            WHEN available_bytes / 1048576 > 1000 THEN
                CAST(CAST((available_bytes / 1048576) / 1024.0 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' GB'
            ELSE
                CAST(CAST(available_bytes / 1048576 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' MB'
        END AS AvailableDiskSpace,
        DB_NAME(f.database_id) + ' (DBID:' + CONVERT(VARCHAR(5), f.database_id) + ' ,  RecoveryModel:'
        + sd.recovery_model_desc + ')' AS DatabaseName,
        f.name + ' (FileID:' + CONVERT(VARCHAR(5), f.file_id) + ')' AS FileName,
        CASE
            WHEN f.type_desc = 'ROWS' THEN 'Data'
            WHEN f.type_desc = 'Log' THEN 'Log'
            ELSE f.type_desc
        END AS FileType,
        ISNULL(FG.FgName, 'LogFile') AS FgName,
        CASE
            WHEN f.type_desc = 'ROWS' THEN 'Data File'
            ELSE
                (
                    SELECT CONVERT(VARCHAR(5), fi.VlfCount) + ' - ' + log_reuse_wait_desc + '('
                        + CONVERT(VARCHAR(128), LogBackupDate) + ')' + 
                        
                    isnull( ( SELECT TOP 1 ISNULL(', (Tran Time:-' + convert(varchar(128),transaction_begin_time ,121),'') FROM #OpenTran OT WHERE  OT.database_id = x.database_id ORDER BY transaction_begin_time ASC) + ')','')
                    FROM sys.databases x
                        LEFT JOIN #BackupInfo bkup ON x.name = bkup.DbName
                    WHERE x.database_id = f.database_id
                )
        END COLLATE SQL_Latin1_General_CP1_CI_AS AS VlfInfo,
        HD.DbSizeMb,
        CONVERT(DECIMAL(20, 0), f.size / 128.0) AS FileSizeMb,
        CONVERT(DECIMAL(20, 2), ((f.size / 128.0) / HD.DbSizeMb) * 100) AS Pct2DbSize,
        CAST(f.size / 128.0 - (d.SpaceUsed / 128.0) AS DECIMAL(15, 2)) AS FileSpaceFreeMb,
		   CASE
            WHEN f.type_desc = 'Log' THEN ISNULL('LogSizeSinceLogBackup:'+ CONVERT(VARCHAR(18),convert(decimal(12,2),DLS.log_since_last_log_backup_mb)),'') + ISNULL('ActiveLogSize:' + convert(varchar(20), convert(decimal(12,2), DLS.active_log_size_mb)),'')
			WHEN f.type_desc = 'Rows' THEN 'Data File'
            ELSE f.type_desc
        END AS LogSizeSinceLastLogBackupMB,
        CONVERT(DECIMAL(15, 2), (100 * CAST(f.size / 128.0 - (d.SpaceUsed / 128.0) AS DECIMAL(15, 2))) 
            / (f.size / 128.0)) AS FilePercentFree,
        CAST(CAST(s.available_bytes / 1048576.0 AS DECIMAL(20, 2)) 
            / CAST(s.total_bytes / 1048576.0 AS DECIMAL(20, 2)) * 100 AS DECIMAL(20, 2)) AS DrivePercentFree,
        CASE
            WHEN b.growth > 100 THEN CONVERT(VARCHAR(6), b.growth / 128) + ' MB'
            ELSE CONVERT(VARCHAR(4), b.growth) + ' %'
        END AS Growth,
        CASE
            WHEN (b.growth > 100 AND b.growth / 128 > 128) THEN
                '--ALTER DATABASE ' + QUOTENAME(DB_NAME(f.database_id)) + 
                ' MODIFY FILE ( NAME = N''' + f.name + ''', FILEGROWTH = ' + 
                CASE
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) < 512 THEN '128MB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 512 AND 2048 THEN '256MB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 2048 AND 5120 THEN '512MB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 5120 AND 20480 THEN '2GB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 20480 AND 51200 THEN '4GB'
                    ELSE '8GB'
                END + ')'
            ELSE
                'ALTER DATABASE ' + QUOTENAME(DB_NAME(f.database_id)) + 
                ' MODIFY FILE ( NAME = N''' + f.name + ''', FILEGROWTH = ' + 
                CASE
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) < 512 THEN '128MB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 512 AND 2048 THEN '256MB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 2048 AND 5120 THEN '512MB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 5120 AND 20480 THEN '2GB'
                    WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) BETWEEN 20480 AND 51200 THEN '4GB'
                    ELSE '8GB'
                END + ')'
        END AS GrowthMbScript,
        '--USE ' + QUOTENAME(SD.NAME) + '; DBCC SHRINKFILE(' + f.name + ',' + 
        CONVERT(VARCHAR(50), CONVERT(DECIMAL(20, 0), (f.size / 128.0) - 100)) + ')' AS ShrinkFile100Mb,
        f.physical_name AS DbFilePath,
        GETDATE() AS ReportRun
    FROM sys.master_files AS f
        CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS s
        INNER JOIN #DbSpaceUseInfo AS d
            ON f.file_id = d.FileId
            AND f.name = d.FileName
            AND f.database_id = DB_ID(DbName)
        INNER JOIN master..sysaltfiles AS b
            ON b.dbid = f.database_id
            AND b.fileid = f.file_id
        LEFT JOIN #VlfInfo fi
            ON fi.DatabaseName = DB_NAME(f.database_id)
            AND fi.FileId = f.file_id
        LEFT JOIN #FileGroupInfo fg
            ON fg.FileId = f.file_id
            AND fg.PhysicalName = f.physical_name
        INNER JOIN sys.databases SD
            ON SD.database_id = f.database_id
        INNER JOIN #DbSizeInfo HD
            ON DB_NAME(f.database_id) = HD.DbName
			CROSS APPLY sys.dm_db_log_stats(f.database_id) DLS
    WHERE (@Disk IS NULL OR s.volume_mount_point = @Disk)
        AND (@DBName IS NULL OR DB_NAME(f.database_id) = @DBName)
        AND (@LowDiskPct IS NULL OR 
             CAST(CAST(s.available_bytes / 1048576.0 AS DECIMAL(20, 2)) / 
             CAST(s.total_bytes / 1048576.0 AS DECIMAL(20, 2)) * 100 AS DECIMAL(20, 2)) <= @LowDiskPct)

    UNION

    SELECT 
        @@SERVERNAME,
        Drive,
        NULL,
        MbFree,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
		NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    FROM #FixedDriveInfo
    WHERE Drive NOT IN (
        SELECT LEFT(volume_mount_point, 1)
        FROM sys.master_files AS f
            CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS s
    )
    ORDER BY 10 DESC;
IF OBJECT_ID('tempdb..#BackupInfo', 'U') IS NOT NULL
    DROP TABLE #BackupInfo;

IF OBJECT_ID('tempdb..#DbSizeInfo', 'U') IS NOT NULL
    DROP TABLE #DbSizeInfo;

IF OBJECT_ID('tempdb..#DbSpaceUseInfo', 'U') IS NOT NULL
    DROP TABLE #DbSpaceUseInfo;

IF OBJECT_ID('tempdb..#FileGroupInfo', 'U') IS NOT NULL
    DROP TABLE #FileGroupInfo;

IF OBJECT_ID('tempdb..#FixedDriveInfo', 'U') IS NOT NULL
    DROP TABLE #FixedDriveInfo;

IF OBJECT_ID('tempdb..#LogInfo2008', 'U') IS NOT NULL
    DROP TABLE #LogInfo2008;

IF OBJECT_ID('tempdb..#LogInfo2012', 'U') IS NOT NULL
    DROP TABLE #LogInfo2012;

IF OBJECT_ID('tempdb..#VlfInfo', 'U') IS NOT NULL
    DROP TABLE #VlfInfo;

END;
GO

EXEC usp_DiskDBSpaceReport;
