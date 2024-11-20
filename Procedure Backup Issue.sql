--Schedule this procedure run under agent job. Most of monitoring tool should be able to alert as its logged in application event log.
use tempdb 
go
CREATE OR ALTER PROCEDURE usp_BackupIssue 
(
    @fullbackup int = 168, --Alert if full backup not done in X hours
    @logbackup int = 24, --Alert if log backup not done in X hours
	@logSizeFullMB int = 512000, --Alert if log size is above X MB
    @lookbackdays int = 8
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DBMissingBackup NVARCHAR(MAX);
	        ;WITH LogSizeCTE AS
        (
            SELECT db.[name] AS [Database], 
            CONVERT(DECIMAL(18,2), ls.cntr_value/1024.0) AS [Log Size (MB)], 
            CONVERT(DECIMAL(18,2), lu.cntr_value/1024.0) AS [Log Used (MB)],
            ISNULL(CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT) AS DECIMAL(18,2)) * 100, 0) AS [Log Used %]
            FROM sys.databases AS db WITH (NOLOCK)
            INNER JOIN sys.dm_os_performance_counters AS lu WITH (NOLOCK)
            ON db.name = lu.instance_name
            INNER JOIN sys.dm_os_performance_counters AS ls WITH (NOLOCK)
            ON db.name = ls.instance_name
            LEFT OUTER JOIN sys.dm_database_encryption_keys AS de WITH (NOLOCK)
            ON db.database_id = de.database_id
            WHERE lu.counter_name LIKE N'Log File(s) Used Size (KB)%' 
            AND ls.counter_name LIKE N'Log File(s) Size (KB)%'
            AND ls.cntr_value > 0
        )
        SELECT * 
        INTO #LogSizeTemp
        FROM LogSizeCTE;

    ;WITH CTE AS
    (
        SELECT 
            ISNULL(d.[name], bs.[database_name]) AS DatabaseName, 
            d.recovery_model_desc AS [Recovery Model], 
            d.log_reuse_wait_desc AS [Log Reuse Wait Desc],
            MAX(CASE WHEN [type] = 'D' THEN bs.backup_finish_date ELSE NULL END) AS [Last Full Backup],
            MAX(CASE WHEN [type] = 'I' THEN bs.backup_finish_date ELSE NULL END) AS [Last Differential Backup],
            MAX(CASE WHEN [type] = 'L' THEN bs.backup_finish_date ELSE NULL END) AS [Last Log Backup],
			LST.[Log Used (MB)] LogUsedMB, LST.[Log Used %] LogUsedPCT
        FROM sys.databases AS d WITH (NOLOCK)
        LEFT JOIN msdb.dbo.backupset AS bs WITH (NOLOCK) ON bs.[database_name] = d.[name] 
		LEFT JOIN #LogSizeTemp LST ON LST.[Database] = D.name
        WHERE d.name <> N'tempdb' AND bs.backup_finish_date > DATEADD(DD, -@lookbackdays, GETDATE())
		AND D.is_read_only = 0 and source_database_id is null
        GROUP BY ISNULL(d.[name], bs.[database_name]), d.recovery_model_desc, d.log_reuse_wait_desc, d.[name] 
		, LST.[Log Used (MB)] , LST.[Log Used %] 
    )
    SELECT DatabaseName + '(Full Backup: ' + CONVERT(VARCHAR(18),ISNULL([Last Full Backup],''),120)
				+ ', Log Backup: ' + CONVERT(VARCHAR(18),ISNULL([Last Log Backup],''),120)
				+ ', LogUsedMB: ' + CONVERT(VARCHAR(20),LogUsedMB)
				+ ', LogUsedPct: ' + CONVERT(VARCHAR(20),LogUsedPCT) + ')' as DBName, * 
    INTO #TempCTE
    FROM CTE
    WHERE 
        ([Last Full Backup] < DATEADD(HH, -@fullbackup, GETDATE()) 
        OR ([Last Log Backup] < DATEADD(HH, -@logbackup, GETDATE()) AND [Recovery Model] <> 'SIMPLE')) 
        OR [Last Full Backup] IS NULL 
        OR ([Last Log Backup] IS NULL AND [Recovery Model] <> 'SIMPLE')
		OR LogUsedMB > @logSizeFullMB
    SELECT @DBMissingBackup = STRING_AGG(DBName, ', ')
    FROM #TempCTE;

    IF @DBMissingBackup IS NOT NULL
    BEGIN 
        DECLARE @message NVARCHAR(2048);
        SET @message = 'Either full backup is not completed in ' 
            + CONVERT(VARCHAR(5), @fullbackup) 
            + ' hours or transaction log backup is not taken in last ' 
            + CONVERT(VARCHAR(5), @logbackup) 
            + ' hours or Used log size is above : ' 
			+ CONVERT(VARCHAR(20), CONVERT(DECIMAL(10,2),@logSizeFullMB/1024.0))
			+ 'GB. Databases: ' + @DBMissingBackup;
        RAISERROR(@message, 18, 1) WITH LOG;
    END
    DROP TABLE #TempCTE;
END
