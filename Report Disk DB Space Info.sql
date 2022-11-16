SET NOCOUNT ON;
-- Create a temp table to hold transaction log file information
CREATE TABLE #log_file_info
(server_name   VARCHAR(50) DEFAULT @@SERVERNAME, 
 database_name VARCHAR(100) NOT NULL, 
 [file_id]     INT NOT NULL, 
 [file_name]   SYSNAME NOT NULL, 
 size_mb       INT NOT NULL, 
 free_mb       INT NULL, 
 autogrow_mb   VARCHAR(20) NOT NULL, 
 autogrow_type CHAR(1), 
 vlf_count     INT
);
CREATE TABLE #tmpfixeddrives
(drive  CHAR(1), 
 MBfree VARCHAR(10)
);
INSERT INTO #tmpfixeddrives
EXEC xp_fixeddrives;
-- In SQL Server 2012 the DBCC LOGINFO output gained a new column so we'll create
-- 2 temp tables (with and without ResourceUnitId) to accommodate both earlier and
-- later versions of SQL Server.
CREATE TABLE #dbcc_log_info_2008
(FileID      INT, 
 FileSize    BIGINT, 
 StartOffset BIGINT, 
 FSeqNo      BIGINT, 
 [Status]    BIGINT, 
 Parity      BIGINT, 
 CreateLSN   NUMERIC(38)
);
CREATE TABLE #dbcc_log_info_2012
(ResourceUnitId BIGINT, 
 FileID         INT, 
 FileSize       BIGINT, 
 StartOffset    BIGINT, 
 FSeqNo         BIGINT, 
 [Status]       BIGINT, 
 Parity         BIGINT, 
 CreateLSN      NUMERIC(38)
);
create table #filegroupname
(
    DBName sysname,
   FGName sysname NULL,
    file_id int,
    physical_name varchar(256)
    
)
insert into #filegroupname
exec sp_MSForeachdb 'use [?];
select "?",filegroup_name(data_space_id), file_id, physical_name from sys.database_files '

/******************************************************************************/

-- Gather transaction log file size and auto-growth specs for each database
EXEC master.sys.sp_MSforeachdb 
     '
	USE [?];INSERT INTO #log_file_info ( database_name, [file_id], [file_name], size_mb, free_mb, autogrow_mb, autogrow_type )
	SELECT  DB_NAME(), file_id, name, (size / 128), (size - FILEPROPERTY(NAME, ''SpaceUsed'')) / 128,
			CASE WHEN is_percent_growth = 1 THEN growth ELSE (growth / 128) END, 
			CASE WHEN is_percent_growth = 1 THEN ''P'' ELSE ''M'' END
	FROM    sys.database_files
	WHERE type = 1;';

-- Use DBCC LOGINFO to get the VLF counts for each database (note that you shouldn't
-- have more than 1 log file per database)
EXEC master.dbo.sp_msforeachdb 
     N'Use [?]; 
            IF (SELECT MAX(compatibility_level) FROM sys.databases) >= 110
			BEGIN
				INSERT INTO #dbcc_log_info_2012 
				EXEC sp_executesql N''DBCC LOGINFO([?]) WITH NO_INFOMSGS'';

				UPDATE #log_file_info 
				SET vlf_count = (SELECT COUNT(*) FROM #dbcc_log_info_2012 WHERE FileId = #log_file_info.file_id)
				WHERE database_name = DB_NAME();

				TRUNCATE TABLE #dbcc_log_info_2012;	
			END
			ELSE
			BEGIN
				INSERT INTO #dbcc_log_info_2008 
				EXEC sp_executesql N''DBCC LOGINFO([?]) WITH NO_INFOMSGS'';

				UPDATE #log_file_info 
				SET vlf_count = (SELECT COUNT(*) FROM #dbcc_log_info_2008 WHERE FileId = #log_file_info.file_id)
				WHERE database_name = DB_NAME();

				TRUNCATE TABLE #dbcc_log_info_2008;	
			END';

/******************************************************************************/
/*
	-- An example of a quick way to script ALTER statements to erradicate those 10% autogrows!
	SELECT  'ALTER DATABASE ' + QUOTENAME(db_name(database_id) +
			' MODIFY FILE (NAME = N''' + name + ''', FILEGROWTH = 50MB, SIZE = 100MB) ' + ';'
	FROM    sys.master_files
	--WHERE   (autogrow_mb = 10 AND autogrow_type = 'P' or size_mb < 100)
	ORDER BY database_name;
	*/

-- Clean up
DECLARE @version NUMERIC(18, 10);
SET @version = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX))) - CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)))), '.', '') AS NUMERIC(18, 10));
IF(@version >= 10.502200)
    BEGIN
        CREATE TABLE #tmpspaceusedR2
        (dbname        VARCHAR(500), 
         filenme       VARCHAR(500), 
         fileid        INT, 
         spaceused     FLOAT, 
         IsPrimaryFile BIT, 
         IsLogFile     BIT
        );
        INSERT INTO #tmpspaceusedR2
        EXEC ('sp_MSforeachdb''use [?]; select ''''?'''' dbname, name filenme, fileid, fileproperty(name,''''spaceused'''') spaceused
,fileproperty(name,''''IsPrimaryFile'''') IsPrimaryFile, fileproperty(name,''''IsLogFile'''') from sysfiles''');
        SELECT DISTINCT 
               @@SERVERNAME AS ServerName, 
               s.volume_mount_point AS Drive,
               CASE
                   WHEN total_bytes / 1048576 > 1000
                   THEN CAST(CAST((total_bytes / 1048576) / 1024.0 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' GB'
                   ELSE CAST(CAST(total_bytes / 1048576 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' MB'
               END AS TotalDiskSpace,
               CASE
                   WHEN available_bytes / 1048576 > 1000
                   THEN CAST(CAST((available_bytes / 1048576) / 1024.0 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' GB'
                   ELSE CAST(CAST(available_bytes / 1048576 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' MB'
               END AS AvailableDiskSpace,
               --, CAST(s.available_bytes / (1024*1048576.0) as decimal(20,2)) [DriveAvailableGB]
               --, CAST(s.total_bytes / (1024*1048576.0) as decimal(20,2)) [DriveTotalGB] 
               DB_NAME(f.database_id) + ' (' + convert(varchar(5), f.database_id) + ')' AS DatabaseName, 
               f.name + ' (' + convert(varchar(5), f.file_id) + ')' AS FileName, 
               case when f.type_desc = 'ROWS' then 'Data'
			   when f.type_desc = 'Log' then 'Log' 
			   else f.type_desc end as FileType,
			   ISNULL (FG.FGName, 'LogFile'),
               CASE
                   WHEN f.type_desc = 'ROWS'
                   THEN '-'
                   ELSE
        (
            SELECT CONVERT(VARCHAR(5), fi.vlf_count) + ' - ' + log_reuse_wait_desc
            FROM sys.databases x
            WHERE x.database_id = f.database_id
        )
               END VLFInfo, 
               f.size / 128.0 AS FileSizeMB, 
               CAST(f.size / 128.0 - (d.spaceused / 128.0) AS DECIMAL(15, 2)) AS FileSpaceFreeMB, 
               CONVERT(DECIMAL(15, 2), (100 * CAST(f.size / 128.0 - (d.spaceused / 128.0) AS DECIMAL(15, 2))) / (f.size / 128.0)) AS FilePercentFree, 
               CAST(CAST(s.available_bytes / 1048576.0 AS DECIMAL(20, 2)) / CAST(s.total_bytes / 1048576.0 AS DECIMAL(20, 2)) * 100 AS DECIMAL(20, 2)) AS DrivePercentFree,
               CASE
                   WHEN b.growth > 100
                   THEN CONVERT(VARCHAR(6), b.growth / 128) + ' MB'
                   ELSE CONVERT(VARCHAR(4), b.growth) + ' %'
               END AS Growth,
               CASE
                   WHEN(b.growth > 100
                        AND b.growth / 128 > 128)
                   THEN '--'
                   ELSE 'ALTER DATABASE ' + QUOTENAME(DB_NAME(f.database_id)) + ' MODIFY FILE ( NAME = N''' + f.name + ''', FILEGROWTH = 256MB)' + CHAR(10) + ' GO'
               END AS GrowthMBScript, 
               f.physical_name AS DBFilePath, 
               GETDATE() AS ReportRun
        FROM sys.master_files AS f
             CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS s
             INNER JOIN #tmpspaceusedR2 AS d ON f.file_id = d.fileid
                                                AND f.name = d.filenme
                                                AND f.database_id = DB_ID(dbname)
             INNER JOIN master..sysaltfiles AS b ON b.dbid = f.database_id
                                                    AND b.fileid = f.file_id
             INNER JOIN #log_file_info fi ON fi.database_name = DB_NAME(f.database_id)
			 left join #filegroupname fg on fg.file_id = f.file_id and fg.physical_name = f.physical_name
             INNER JOIN sys.databases SD ON SD.database_id = f.database_id 
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
			   null,
               NULL, 
               NULL, 
               NULL, 
               NULL, 
               NULL, 
               NULL
        FROM #tmpfixeddrives
        WHERE drive NOT IN
        (
            SELECT LEFT(volume_mount_point, 1)
            FROM sys.master_files AS f
                 CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS s
        )
        order by 10 DESC;
        DROP TABLE #tmpspaceusedR2;
END;

/*

get-wmiobject win32_volume 
#-computername ServerName

 | select name, label, BlockSize, @{Name="Capacity(GB)";expression={[math]::round(($_.Capacity/ 1073741824),2)}}, @{Name="FreeSpace(GB)";expression={[math]::round(($_.FreeSpace / 1073741824),2)}},@{Name="Free(%)";expression={[math]::round(((($_.FreeSpace / 1073741824)/($_.Capacity / 1073741824)) * 100),0)}} |format-table


*/

        --$TotalGB = @{Name="Capacity(GB)";expression={[math]::round(($_.Capacity/ 1073741824),2)}}
        --$FreeGB = @{Name="FreeSpace(GB)";expression={[math]::round(($_.FreeSpace / 1073741824),2)}}
        --$FreePerc = @{Name="Free(%)";expression={[math]::round(((($_.FreeSpace / 1073741824)/($_.Capacity / 1073741824)) * 100),0)}}
        --Get-WmiObject $server win32_volume | Where-object {$_.DriveLetter -eq $null} $volumes | Select SystemName, Label, $TotalGB, $FreeGB, $FreePerc | Format-Table -AutoSize
        --------------
    ELSE
    IF(@version >= 9.00)
        BEGIN
            CREATE TABLE #tmpspaceused
            (dbname        VARCHAR(500), 
             filenme       VARCHAR(500), 
             spaceused     FLOAT, 
             IsPrimaryFile BIT, 
             IsLogFile     BIT
            );
            INSERT INTO #tmpspaceused
            EXEC ('sp_MSforeachdb''use [?]; select ''''?'''' dbname, name filenme, fileproperty(name,''''spaceused'''') spaceused
,fileproperty(name,''''IsPrimaryFile'''') IsPrimaryFile, fileproperty(name,''''IsLogFile'''') from sysfiles''');
            IF @@version LIKE '%2000%'
                BEGIN
                    SELECT @@servername AS servername, 
                           c.drive, 
                           '?' AS TotalDiskSpace,
                           CASE
                               WHEN CAST(c.MBfree AS DECIMAL(18, 2)) > 1000
                               THEN CAST(CAST(CAST(c.MBfree AS DECIMAL(18, 2)) / 1024.0 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' GB'
                               ELSE CAST(CAST(CAST(c.MBfree AS DECIMAL(18, 2)) AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' MB'
                           END AS DiskSpaceFree, 
                           a.name AS databasename, 
                           b.name AS filename,
                           CASE
                               WHEN IsLogFile = 0
                               THEN 'Data'
                               ELSE 'Log'
                           END AS filetype,
                           CASE
                               WHEN IsLogFile = 0
                               THEN 'Data File'
                               ELSE
                    (
                        SELECT CONVERT(VARCHAR(5), fi.vlf_count) + ' - ' + log_reuse_wait_desc
                        FROM sys.databases x
                        WHERE x.name = a.name
                    )
                           END VLFInfo, 
                           b.size / 128.0 AS size, 
                           CAST(b.size / 128.0 - (d.spaceused / 128.0) AS DECIMAL(15, 2)) AS spacefree, 
                           CONVERT(DECIMAL(15, 2), (100 * CAST(b.size * 8 / 1024.0 - (d.spaceused / 128.0) AS DECIMAL(15, 2))) / (b.size * 8 / 1024.0)) AS DBPercentFree, 
                           '-' AS DrivePercentFree,
                           CASE
                               WHEN b.growth > 100
                               THEN CONVERT(VARCHAR(6), b.growth / 128) + ' MB'
                               ELSE CONVERT(VARCHAR(4), b.growth) + ' %'
                           END AS growth,
                           CASE
                               WHEN b.growth > 100
                                    AND b.growth / 128 > 128
                               THEN '--'
                               ELSE 'ALTER DATABASE ' + QUOTENAME(a.name) + ' MODIFY FILE ( NAME = N''' + b.name + ''', FILEGROWTH = 256MB)'
                           END AS GrowthMBScript, 
                           b.filename AS physical_name, 
                           GETDATE() AS reportrun
                    --into tempdb..fileinfo
                    FROM master..sysdatabases AS a
                         JOIN sysaltfiles AS b ON a.dbid = b.dbid
                         LEFT JOIN #tmpfixeddrives AS c ON LEFT(b.filename, 1) = c.drive
                         INNER JOIN #log_file_info fi ON fi.database_name = a.name
                         LEFT JOIN #tmpspaceused AS d ON a.name = d.dbname
                                                         AND b.name = d.filenme
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
                           NULL, 
                           NULL, 
                           NULL
                    FROM #tmpfixeddrives
                    WHERE drive NOT IN
                    (
                        SELECT LEFT(b.filename, 1)
                        FROM sysaltfiles AS b
                    )
                    order by 10 DESC;
            END;
                ELSE
                BEGIN
                    SELECT @@servername AS ServerName, 
                           c.drive AS Drive, 
                           '?' AS TotalDiskSpace,
                           CASE
                               WHEN CAST(c.MBfree AS DECIMAL(18, 2)) > 1000
                               THEN CAST(CAST(CAST(c.MBfree AS DECIMAL(18, 2)) / 1024.0 AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' GB'
                               ELSE CAST(CAST(CAST(c.MBfree AS DECIMAL(18, 2)) AS DECIMAL(18, 2)) AS VARCHAR(20)) + ' MB'
                           END AS DiskSpaceFree, 
                           a.name AS databasename, 
                           b.name AS filename,
                           CASE
                               WHEN IsLogFile = 0
                               THEN 'Data'
                               ELSE 'Log'
                           END AS filetype,
                           CASE
                               WHEN IsLogFile = 0
                               THEN 'Data File'
                               ELSE
                    (
                        SELECT CONVERT(VARCHAR(5), fi.vlf_count) + ' - ' + log_reuse_wait_desc
                        FROM sys.databases x
                        WHERE x.name = a.name
                    )
                           END VLFInfo, 
                           b.size / 128.0 AS size, 
                           CAST(b.size / 128.0 - (d.spaceused / 128.0) AS DECIMAL(19, 2)) AS spacefree, 
                           CONVERT(DECIMAL(19, 2), (100 * CAST(cast(b.size as bigint) * 8 / 1024.0 - (d.spaceused / 128.0) AS DECIMAL(19, 2))) / (cast(b.size as bigint) * 8 / 1024.0)) AS DBPercentFree, 
                           '-' AS DrivePercentFree,
                           CASE
                               WHEN b.is_percent_growth = 0
                               THEN CONVERT(VARCHAR(6), b.growth / 128) + ' MB'
                               ELSE CONVERT(VARCHAR(4), b.growth) + ' %'
                           END AS Growth,
                           CASE
                               WHEN b.is_percent_growth = 0
                                    AND b.is_percent_growth / 128 > 128
                               THEN '--'
                               ELSE 'ALTER DATABASE ' + QUOTENAME(a.name) + ' MODIFY FILE ( NAME = N''' + b.name + ''', FILEGROWTH = 256MB)'
                           END AS GrowthMBScript, 
                           b.physical_name DBFilePath, 
                           GETDATE() AS reportrun
                    --into tempdb..fileinfo         
                    FROM master..sysdatabases AS a
                         JOIN sys.master_files AS b ON a.dbid = b.database_id
                         INNER JOIN #log_file_info fi ON fi.database_name = a.name
                         LEFT JOIN #tmpfixeddrives AS c ON LEFT(b.physical_name, 1) = c.drive
                         LEFT JOIN #tmpspaceused AS d ON a.name = d.dbname
                                                         AND b.name = d.filenme
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
                           NULL, 
                           NULL, 
                           NULL
                    FROM #tmpfixeddrives
                    WHERE drive NOT IN
                    (
                        SELECT LEFT(b.physical_name, 1)
                        FROM sys.master_files AS b
                    )
                    order by 10 DESC;
            END;
    END;
GO
IF OBJECT_ID('tempdb..#dbcc_log_info_2008') IS NOT NULL
    DROP TABLE #dbcc_log_info_2008;
IF OBJECT_ID('tempdb..#dbcc_log_info_2012') IS NOT NULL
    DROP TABLE #dbcc_log_info_2012;
IF OBJECT_ID('tempdb..#log_file_info') IS NOT NULL
    DROP TABLE #log_file_info;
IF OBJECT_ID('tempdb..#tmpfixeddrives') IS NOT NULL
    DROP TABLE #tmpfixeddrives;
IF OBJECT_ID('tempdb..#tmpspaceused') IS NOT NULL
    DROP TABLE #tmpspaceused;
IF OBJECT_ID('tempdb..#tmpspaceusedR2') IS NOT NULL
    DROP TABLE #tmpspaceusedR2;
IF OBJECT_ID('tempdb..#filegroupname') IS NOT NULL
    DROP TABLE #filegroupname;
