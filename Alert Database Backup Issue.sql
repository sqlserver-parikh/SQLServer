--Schedule this procedure run under agent job. Most of monitoring tool should be able to alert as its logged in application event log.
CREATE OR ALTER PROCEDURE usp_BackupIssue 
(
    @fullbackup int = 36, 
    @logbackup int = 6, 
    @lookbackdays int = 15
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DBMissingBackup NVARCHAR(MAX);

    ;WITH CTE AS
    (
        SELECT 
            ISNULL(d.[name], bs.[database_name]) AS [Database], 
            d.recovery_model_desc AS [Recovery Model], 
            d.log_reuse_wait_desc AS [Log Reuse Wait Desc],
            MAX(CASE WHEN [type] = 'D' THEN bs.backup_finish_date ELSE NULL END) AS [Last Full Backup],
            MAX(CASE WHEN [type] = 'I' THEN bs.backup_finish_date ELSE NULL END) AS [Last Differential Backup],
            MAX(CASE WHEN [type] = 'L' THEN bs.backup_finish_date ELSE NULL END) AS [Last Log Backup]
        FROM sys.databases AS d WITH (NOLOCK)
        LEFT OUTER JOIN msdb.dbo.backupset AS bs WITH (NOLOCK)
            ON bs.[database_name] = d.[name] 
            AND bs.backup_finish_date > DATEADD(DD, -@lookbackdays, GETDATE())
        WHERE d.name <> N'tempdb'
        GROUP BY ISNULL(d.[name], bs.[database_name]), d.recovery_model_desc, d.log_reuse_wait_desc, d.[name] 
    )
    SELECT * 
    INTO #TempCTE
    FROM CTE
    WHERE 
        ([Last Full Backup] < DATEADD(HH, -@fullbackup, GETDATE()) 
        OR ([Last Log Backup] < DATEADD(HH, -@logbackup, GETDATE()) AND [Recovery Model] <> 'SIMPLE')) 
        OR [Last Full Backup] IS NULL 
        OR ([Last Log Backup] IS NULL AND [Recovery Model] <> 'SIMPLE');

    SELECT @DBMissingBackup = STRING_AGG([Database], ', ')
    FROM #TempCTE;

    IF @@ROWCOUNT <> 0 
    BEGIN 
        DECLARE @message NVARCHAR(2048);
        SET @message = 'Either full backup is not completed in ' 
            + CONVERT(VARCHAR(5), @fullbackup) 
            + ' hours or transaction log backup is not taken in last ' 
            + CONVERT(VARCHAR(5), @logbackup) 
            + ' hours. Databases: ' + @DBMissingBackup;
        RAISERROR(@message, 18, 1) WITH LOG;
    END

    DROP TABLE #TempCTE;
END
