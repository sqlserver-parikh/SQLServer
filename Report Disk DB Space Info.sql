CREATE OR ALTER PROCEDURE usp_DiskDBSpaceReport
AS
SET NOCOUNT ON;
-- Create a temp table to hold transaction log file information
SELECT ISNULL(d.[name], bs.[database_name]) COLLATE SQL_Latin1_General_CP1_CI_AS AS DBName,
       CASE
           WHEN d.recovery_model_desc = 'SIMPLE' THEN
               'Simple Recovery'
           ELSE
               CONVERT(VARCHAR(19),
                       MAX(   CASE
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
           AND bs.backup_finish_date > GETDATE() - 30
GROUP BY ISNULL(d.[name], bs.[database_name]),
         d.recovery_model_desc,
         d.log_reuse_wait_desc
ORDER BY d.recovery_model_desc
OPTION (RECOMPILE);

CREATE TABLE #VLFInfo
(
    server_name VARCHAR(50)
        DEFAULT @@SERVERNAME,
    database_name VARCHAR(100) NOT NULL,
    [file_id] INT NOT NULL,
    [file_name] SYSNAME NOT NULL,
    size_mb INT NOT NULL,
    free_mb INT NULL,
    autogrow_mb VARCHAR(20) NOT NULL,
    autogrow_type CHAR(1),
    vlf_count INT
);
CREATE TABLE #FixedDriveInfo
(
    drive CHAR(1),
    MBfree VARCHAR(10)
);
INSERT INTO #FixedDriveInfo
EXEC xp_fixeddrives;
-- In SQL Server 2012 the DBCC LOGINFO output gained a new column so we'll create
-- 2 temp tables (with and without ResourceUnitId) to accommodate both earlier and
-- later versions of SQL Server.
CREATE TABLE #LogInfo2008
(
    FileID INT,
    FileSize BIGINT,
    StartOffset BIGINT,
    FSeqNo BIGINT,
    [Status] BIGINT,
    Parity BIGINT,
    CreateLSN NUMERIC(38)
);
CREATE TABLE #LogInfo2012
(
    ResourceUnitId BIGINT,
    FileID INT,
    FileSize BIGINT,
    StartOffset BIGINT,
    FSeqNo BIGINT,
    [Status] BIGINT,
    Parity BIGINT,
    CreateLSN NUMERIC(38)
);
create table #FileGroupInfo
(
    DBName sysname,
    FGName sysname NULL,
    file_id int,
    physical_name varchar(256)
)
insert into #FileGroupInfo
exec sp_MSForeachdb 'use [?];
select "?",filegroup_name(data_space_id), file_id, physical_name from sys.database_files '

select db_name(database_id) DBName,
       convert(decimal(15, 2), sum(size) / 128.0) DBSizeMB
into #DBSizeInfo
from sys.master_files
group by db_name(database_id)

-- Gather transaction log file size and auto-growth specs for each database
EXEC master.sys.sp_MSforeachdb '
	USE [?];INSERT INTO #VLFInfo ( database_name, [file_id], [file_name], size_mb, free_mb, autogrow_mb, autogrow_type )
	SELECT  DB_NAME(), file_id, name, (size / 128), (size - FILEPROPERTY(NAME, ''SpaceUsed'')) / 128,
			CASE WHEN is_percent_growth = 1 THEN growth ELSE (growth / 128) END, 
			CASE WHEN is_percent_growth = 1 THEN ''P'' ELSE ''M'' END
	FROM    sys.database_files
	WHERE type = 1;';

EXEC master.dbo.sp_msforeachdb N'Use [?]; 
            IF (SELECT MAX(compatibility_level) FROM sys.databases) >= 110
			BEGIN
				INSERT INTO #LogInfo2012 
				EXEC sp_executesql N''DBCC LOGINFO([?]) WITH NO_INFOMSGS'';

				UPDATE #VLFInfo 
				SET vlf_count = (SELECT COUNT(*) FROM #LogInfo2012 WHERE FileId = #VLFInfo.file_id)
				WHERE database_name = DB_NAME();

				TRUNCATE TABLE #LogInfo2012;	
			END
			ELSE
			BEGIN
				INSERT INTO #LogInfo2008 
				EXEC sp_executesql N''DBCC LOGINFO([?]) WITH NO_INFOMSGS'';

				UPDATE #VLFInfo 
				SET vlf_count = (SELECT COUNT(*) FROM #LogInfo2008 WHERE FileId = #VLFInfo.file_id)
				WHERE database_name = DB_NAME();

				TRUNCATE TABLE #LogInfo2008;	
			END';

DECLARE @version NUMERIC(18, 10);
SET @version
    = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), CHARINDEX(
                                                                                      '.',
                                                                                      CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX))
                                                                                  ) - 1) + '.'
           + REPLACE(
                        RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)))
                                                                                       - CHARINDEX(
                                                                                                      '.',
                                                                                                      CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX))
                                                                                                  )),
                        '.',
                        ''
                    ) AS NUMERIC(18, 10));
IF (@version >= 10.502200)
BEGIN
    CREATE TABLE #DBSpaceUseInfo
    (
        dbname VARCHAR(500),
        filenme VARCHAR(500),
        fileid INT,
        spaceused FLOAT,
        IsPrimaryFile BIT,
        IsLogFile BIT
    );
    INSERT INTO #DBSpaceUseInfo
    EXEC ('sp_MSforeachdb''use [?]; select ''''?'''' dbname, name filenme, fileid, fileproperty(name,''''spaceused'''') spaceused
,fileproperty(name,''''IsPrimaryFile'''') IsPrimaryFile, fileproperty(name,''''IsLogFile'''') from sysfiles''');
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
        --, CAST(s.available_bytes / (1024*1048576.0) as decimal(20,2)) [DriveAvailableGB]
        --, CAST(s.total_bytes / (1024*1048576.0) as decimal(20,2)) [DriveTotalGB] 
        DB_NAME(f.database_id) + ' (DBID:' + convert(varchar(5), f.database_id) + ' ,  RecoveryModel:'
        + sd.recovery_model_desc + ')' AS DatabaseName,
        f.name + ' (FileID:' + convert(varchar(5), f.file_id) + ')' AS FileName,
        case
            when f.type_desc = 'ROWS' then
                'Data'
            when f.type_desc = 'Log' then
                'Log'
            else
                f.type_desc
        end as FileType,
        ISNULL(FG.FGName, 'LogFile') FGName,
        CASE
            WHEN f.type_desc = 'ROWS' THEN
                'Data File'
            ELSE
        (
            SELECT CONVERT(VARCHAR(5), fi.vlf_count) + ' - ' + log_reuse_wait_desc + '('
                   + CONVERT(VARCHAR(128), LogBackupDate) + ')'
            FROM sys.databases x
                left join #BackupInfo bkup
                    on x.name = bkup.DBName
            WHERE x.database_id = f.database_id
        )
        END COLLATE SQL_Latin1_General_CP1_CI_AS VLFInfo,
        HD.DBSizeMB,
        convert(decimal(20, 0), f.size / 128.0) AS FileSizeMB,
        CONVERT(DECIMAL(20, 2), ((f.size / 128.0) / HD.DBSizeMB) * 100) Pct2DBSize,
        CAST(f.size / 128.0 - (d.spaceused / 128.0) AS DECIMAL(15, 2)) AS FileSpaceFreeMB,
        CONVERT(
                   DECIMAL(15, 2),
                   (100 * CAST(f.size / 128.0 - (d.spaceused / 128.0) AS DECIMAL(15, 2))) / (f.size / 128.0)
               ) AS FilePercentFree,
        CAST(CAST(s.available_bytes / 1048576.0 AS DECIMAL(20, 2)) / CAST(s.total_bytes / 1048576.0 AS DECIMAL(20, 2))
             * 100 AS DECIMAL(20, 2)) AS DrivePercentFree,
        CASE
            WHEN b.growth > 100 THEN
                CONVERT(VARCHAR(6), b.growth / 128) + ' MB'
            ELSE
                CONVERT(VARCHAR(4), b.growth) + ' %'
        END AS Growth,
        CASE
            WHEN
            (
                b.growth > 100
                AND b.growth / 128 > 128
            ) THEN
                '--ALTER DATABASE ' + QUOTENAME(DB_NAME(f.database_id)) + ' MODIFY FILE ( NAME = N''' + f.name
                + ''', FILEGROWTH = ' + CASE
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) < 512 THEN
                                                '128MB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 512 AND 2048 THEN
                                                '256MB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 2048 AND 5120 THEN
                                                '512MB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 5120 AND 20480 THEN
                                                '2GB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 20480 AND 51200 THEN
                                                '4GB'
                                            ELSE
                                                '8GB'
                                        END + ')'
            ELSE
                'ALTER DATABASE ' + QUOTENAME(DB_NAME(f.database_id)) + ' MODIFY FILE ( NAME = N''' + f.name
                + ''', FILEGROWTH = ' + CASE
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0) < 512 THEN
                                                '128MB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 512 AND 2048 THEN
                                                '256MB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 2048 AND 5120 THEN
                                                '512MB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 5120 AND 20480 THEN
                                                '2GB'
                                            WHEN CONVERT(DECIMAL(20, 0), f.size / 128.0)
                                                 BETWEEN 20480 AND 51200 THEN
                                                '4GB'
                                            ELSE
                                                '8GB'
                                        END + ')'
        END AS GrowthMBScript,
        '--USE ' + QUOTENAME(SD.NAME) + '; DBCC SHRINKFILE(' + f.name + ','
        + convert(varchar(50), CONVERT(DECIMAL(20, 0), (f.size / 128.0) - 100)) + ')' ShrinkFile100MB,
        f.physical_name AS DBFilePath,
        GETDATE() AS ReportRun
    FROM sys.master_files AS f
        CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS s
        INNER JOIN #DBSpaceUseInfo AS d
            ON f.file_id = d.fileid
               AND f.name = d.filenme
               AND f.database_id = DB_ID(dbname)
        INNER JOIN master..sysaltfiles AS b
            ON b.dbid = f.database_id
               AND b.fileid = f.file_id
        left JOIN #VLFInfo fi
            ON fi.database_name = DB_NAME(f.database_id)
               and fi.file_id = f.file_id
        left join #FileGroupInfo fg
            on fg.file_id = f.file_id
               and fg.physical_name = f.physical_name
        INNER JOIN sys.databases SD
            ON SD.database_id = f.database_id
        inner join #DBSizeInfo HD
            on db_name(f.database_id) = HD.DBName
    --where cast((CAST(s.available_bytes / 1048576.0 as decimal(20,2))) / CAST(s.total_bytes / 1048576.0 as decimal(20,2)) *100 as decimal (20,2))< 20 or convert(decimal(15,2), (100* cast((f.size * 8 / 1024.0) - (d.spaceused / 128.0) as decimal(15,2)))/ ( f.size * 8 / 1024.0 )) < 20.0
    UNION
    SELECT @@SERVERNAME,
           drive,
           NULL,
           MBfree,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           null,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL
    FROM #FixedDriveInfo
    WHERE drive NOT IN (
                           SELECT LEFT(volume_mount_point, 1)
                           FROM sys.master_files AS f
                               CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS s
                       )
    order by 10 DESC;
    DROP TABLE #DBSpaceUseInfo;
END;
GO
usp_DiskDBSpaceReport
