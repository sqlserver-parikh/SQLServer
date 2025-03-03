USE tempdb;
GO
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[tblRPODetails]')
          AND type in ( N'U' )
)
    DROP TABLE tblRPODetails
GO
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[usp_RPOWorstCaseMinutes]')
          AND type in ( N'P' )
)
    DROP PROCEDURE usp_RPOWorstCaseMinutes
GO
CREATE PROCEDURE usp_RPOWorstCaseMinutes
(
    @DatabaseNames NVARCHAR(MAX) = '',           -- Comma-separated list of database names; empty or NULL means all databases
    @LookBackDays INT = 7,                       -- Number of days to look back for backup history; must be > 0
    @LogRetentionDays INT = 60,                  -- Retention period for log table in days
    @LogToTable BIT = 0,                         -- 1 to log results to tblRPODetails, 0 to return directly
    @RPOBaselineHoursFull INT = 1,               -- RPO baseline in hours for full recovery model databases
    @RPOBaselineHoursSimple INT = 30             -- RPO baseline in hours for simple recovery model databases
)
AS
BEGIN
    SET NOCOUNT ON;

    -- **Parameter Validation**
    IF @LookBackDays <= 0
    BEGIN
        RAISERROR('LookBackDays must be greater than 0.', 16, 1);
        RETURN;
    END

    IF @LogToTable = 1 AND @LogRetentionDays <= 0
    BEGIN
        RAISERROR('LogRetentionDays must be greater than 0 when LogToTable is 1.', 16, 1);
        RETURN;
    END

    -- **Create Temporary Tables**
    -- Store log backup statistics
    CREATE TABLE #LogBackupStats
    (
        DatabaseName NVARCHAR(128),
        TotalLogBackupSizeMB DECIMAL(18, 2),
        LogBackupCount INT,
        AvgBackupSpeedMBps DECIMAL(18, 2),
        TotalTimeSpentSeconds INT
    );

    -- Store availability group backup preferences
    CREATE TABLE #BackupPreferences
    (
        AvailabilityGroupName NVARCHAR(128),
        DatabaseName NVARCHAR(128),
        BackupPreference NVARCHAR(128),
        ReplicaRole NVARCHAR(128)
    );

    -- Store database metadata
    CREATE TABLE #DatabaseInfo
    (
        DatabaseName NVARCHAR(128),
        DBSizeMB DECIMAL(18, 2),
        CreateDate DATETIME,
        RecoveryModel NVARCHAR(60),
        DBStatus NVARCHAR(128)
    );

    -- Store RPO worst-case results
    CREATE TABLE #RPOWorstCase
    (
        DatabaseName NVARCHAR(128),
        RPOWorstCaseMinutes INT,
        RPOWorstCaseFinishTime DATETIME,
        RPOWorstCasePriorFinishTime DATETIME,
        BreachCount INT
    );

    -- **Populate #LogBackupStats**
    INSERT INTO #LogBackupStats (DatabaseName, TotalLogBackupSizeMB, LogBackupCount, AvgBackupSpeedMBps, TotalTimeSpentSeconds)
    SELECT name, 0, 0, 0, 0
    FROM sys.databases
    WHERE name NOT IN ('tempdb');

    UPDATE #LogBackupStats
    SET TotalLogBackupSizeMB = ISNULL(bs.TotalLogBackupSizeMB, 0),
        LogBackupCount = ISNULL(bs.LogBackupCount, 0),
        AvgBackupSpeedMBps = ISNULL(bs.AvgBackupSpeedMBps, 0),
        TotalTimeSpentSeconds = ISNULL(bs.TotalTimeSpentSeconds, 0)
    FROM #LogBackupStats ls
    LEFT JOIN (
        SELECT database_name,
               SUM(backup_size / 1048576.0) AS TotalLogBackupSizeMB,
               COUNT(*) AS LogBackupCount,
               CASE
                   WHEN SUM(backup_size / 1048576.0) = 0 THEN 0
                   ELSE SUM(backup_size / 1048576.0) / NULLIF(SUM(DATEDIFF(SECOND, backup_start_date, backup_finish_date)), 0)
               END AS AvgBackupSpeedMBps,
               SUM(DATEDIFF(SECOND, backup_start_date, backup_finish_date)) AS TotalTimeSpentSeconds
        FROM msdb.dbo.backupset
        WHERE type = 'L'
              AND backup_finish_date > DATEADD(DAY, -@LookBackDays, GETDATE())
        GROUP BY database_name
    ) bs ON ls.DatabaseName = bs.database_name;

    -- **Populate #BackupPreferences**
    INSERT INTO #BackupPreferences (AvailabilityGroupName, DatabaseName, BackupPreference, ReplicaRole)
    SELECT ag.name,
           dbcs.database_name,
           ag.automated_backup_preference_desc,
           ars.role_desc
    FROM sys.availability_groups ag
    JOIN sys.dm_hadr_availability_replica_states ars ON ag.group_id = ars.group_id
    JOIN sys.dm_hadr_database_replica_cluster_states dbcs ON ars.replica_id = dbcs.replica_id
    WHERE ars.is_local = 1
          AND dbcs.database_name NOT IN ('tempdb')
    ORDER BY ag.name, dbcs.database_name;

    -- **Populate #DatabaseInfo**
    INSERT INTO #DatabaseInfo (DatabaseName, DBSizeMB, CreateDate, RecoveryModel, DBStatus)
    SELECT d.name,
           SUM(mf.size * 8.0 / 1024) AS DBSizeMB,
           d.create_date,
           d.recovery_model_desc,
           CASE
               WHEN d.is_read_only = 1 THEN 'READ ONLY / ' + d.state_desc
               ELSE 'READ WRITE / ' + d.state_desc
           END
    FROM sys.databases d
    LEFT JOIN sys.master_files mf ON d.database_id = mf.database_id
    WHERE d.name NOT IN ('tempdb')
    GROUP BY d.name, d.create_date, d.recovery_model_desc, d.is_read_only, d.state_desc;

    -- **Create or Manage tblRPODetails for Logging**
    IF @LogToTable = 1
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblRPODetails]') AND type = 'U')
        BEGIN
            CREATE TABLE [dbo].[tblRPODetails]
            (
                ServerName NVARCHAR(128),
                DatabaseName NVARCHAR(128),
                DBSizeMB DECIMAL(18, 2),
                DBCreateDate DATETIME,
                AGName NVARCHAR(128),
                BackupPreference NVARCHAR(128),
                ReplicaRole NVARCHAR(128),
                RPOWorstCaseMinutes INT,
                RPOWorstCaseHours AS CAST(RPOWorstCaseMinutes / 60.0 AS DECIMAL(10, 2)),
                RPOWorstCaseDays AS CAST(RPOWorstCaseMinutes / 1440.0 AS DECIMAL(10, 2)),
                RecoveryModel NVARCHAR(60),
                DBStatus NVARCHAR(128),
                TotalLogBackupSizeMB DECIMAL(18, 2),
                LogBackupCount INT,
                AvgBackupSpeedMBps DECIMAL(18, 2),
                LogBackupTimeSeconds INT,
                RPOWorstCaseFinishTime DATETIME,
                RPOWorstCasePriorFinishTime DATETIME,
                LookBackDays INT,
                RunTimeUTC DATETIME,
                RPOBaselineHours INT,
                BreachCount INT
            );

            CREATE NONCLUSTERED INDEX IX_tblRPODetails_RunTimeUTC ON [dbo].[tblRPODetails] (RunTimeUTC);
        END
    END

    -- **Handle Database Filtering**
    IF @DatabaseNames IS NULL OR @DatabaseNames = ''
        SET @DatabaseNames = '%';

    CREATE TABLE #DatabaseFilter (DatabaseName SYSNAME);
    INSERT INTO #DatabaseFilter (DatabaseName)
    SELECT TRIM(value)
    FROM STRING_SPLIT(@DatabaseNames, ',')
    WHERE TRIM(value) != '';

    -- **Calculate RPO Worst Case**
    DECLARE @DynamicSQL NVARCHAR(MAX) = N'
        SELECT bs.database_name, bs.backup_set_id, bs.backup_finish_date,
               bsPrior.backup_finish_date AS PriorFinishDate,
               DATEDIFF(SECOND, bsPrior.backup_finish_date, bs.backup_finish_date) AS BackupGapSeconds
        INTO #BackupGaps
        FROM msdb.dbo.backupset bs WITH (NOLOCK)
        CROSS APPLY (
            SELECT TOP 1 backup_finish_date
            FROM msdb.dbo.backupset bs1 WITH (NOLOCK)
            WHERE bs.database_name = bs1.database_name
                  AND bs.database_guid = bs1.database_guid
                  AND bs.backup_finish_date > bs1.backup_finish_date
                  AND bs.backup_set_id > bs1.backup_set_id
            ORDER BY bs1.backup_finish_date DESC
        ) bsPrior
        WHERE bs.backup_finish_date > DATEADD(DAY, -@LookBackDays, GETDATE())
              AND EXISTS (SELECT 1 FROM #DatabaseFilter df WHERE bs.database_name = df.DatabaseName OR df.DatabaseName = ''%'');

        WITH BreachCounts AS (
            SELECT g.database_name,
                   SUM(CASE WHEN g.BackupGapSeconds > (CASE WHEN d.recovery_model_desc = ''SIMPLE'' THEN @RPOBaselineHoursSimple ELSE @RPOBaselineHoursFull END * 3600) THEN 1 ELSE 0 END) AS BreachCount
            FROM #BackupGaps g
            JOIN sys.databases d ON g.database_name = d.name
            GROUP BY g.database_name
        ),
        MaxGaps AS (
            SELECT database_name,
                   MAX(BackupGapSeconds) AS MaxBackupGapSeconds,
                   MAX(backup_finish_date) AS MaxFinishTime,
                   MAX(PriorFinishDate) AS MaxPriorFinishTime
            FROM #BackupGaps
            GROUP BY database_name
        )
        INSERT INTO #RPOWorstCase (DatabaseName, RPOWorstCaseMinutes, RPOWorstCaseFinishTime, RPOWorstCasePriorFinishTime, BreachCount)
        SELECT mg.database_name,
               mg.MaxBackupGapSeconds / 60,
               mg.MaxFinishTime,
               mg.MaxPriorFinishTime,
               bc.BreachCount
        FROM MaxGaps mg
        JOIN BreachCounts bc ON mg.database_name = bc.database_name;

        DROP TABLE #BackupGaps;
    ';

    BEGIN TRY
        EXEC sp_executesql @DynamicSQL,
                           N'@LookBackDays INT, @RPOBaselineHoursFull INT, @RPOBaselineHoursSimple INT',
                           @LookBackDays, @RPOBaselineHoursFull, @RPOBaselineHoursSimple;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR('Error executing RPO calculation: %s', 16, 1, @ErrorMessage);
        RETURN;
    END CATCH

    -- **Final Results**
    SELECT @@SERVERNAME AS ServerName,
           di.DatabaseName,
           di.DBSizeMB,
           di.CreateDate AS DBCreateDate,
           ISNULL(bp.AvailabilityGroupName, 'Not in AG') AS AGName,
           ISNULL(bp.BackupPreference, 'Not in AG') AS BackupPreference,
           ISNULL(bp.ReplicaRole, 'Not in AG') AS ReplicaRole,
           ISNULL(rpo.RPOWorstCaseMinutes, 0) AS RPOWorstCaseMinutes,
           CAST(ISNULL(rpo.RPOWorstCaseMinutes, 0) / 60.0 AS DECIMAL(10, 2)) AS RPOWorstCaseHours,
           CAST(ISNULL(rpo.RPOWorstCaseMinutes, 0) / 1440.0 AS DECIMAL(10, 2)) AS RPOWorstCaseDays,
           di.RecoveryModel,
           di.DBStatus,
           lbs.TotalLogBackupSizeMB,
           lbs.LogBackupCount,
           lbs.AvgBackupSpeedMBps,
           lbs.TotalTimeSpentSeconds AS LogBackupTimeSeconds,
           rpo.RPOWorstCaseFinishTime,
           rpo.RPOWorstCasePriorFinishTime,
           @LookBackDays AS LookBackDays,
           GETUTCDATE() AS RunTimeUTC,
           CASE WHEN di.RecoveryModel = 'SIMPLE' THEN @RPOBaselineHoursSimple ELSE @RPOBaselineHoursFull END AS RPOBaselineHours,
           ISNULL(rpo.BreachCount, 0) AS BreachCount
    INTO #Results
    FROM #DatabaseInfo di
    LEFT JOIN #RPOWorstCase rpo ON di.DatabaseName = rpo.DatabaseName
    LEFT JOIN #BackupPreferences bp ON di.DatabaseName = bp.DatabaseName
    LEFT JOIN #LogBackupStats lbs ON di.DatabaseName = lbs.DatabaseName
    WHERE EXISTS (SELECT 1 FROM #DatabaseFilter df WHERE di.DatabaseName = df.DatabaseName OR df.DatabaseName = '%');

    -- **Log or Return Results**
    IF @LogToTable = 1
    BEGIN
        INSERT INTO [dbo].[tblRPODetails]
        SELECT * FROM #Results
		WHERE DBStatus NOT LIKE '%READ ONLY%'
		AND DBStatus NOT LIKE '%OFFLINE%'
		AND DBStatus NOT LIKE '%RESTORING%'

        DELETE FROM [dbo].[tblRPODetails]
        WHERE RunTimeUTC < DATEADD(DAY, -@LogRetentionDays, GETUTCDATE());
    END

    SELECT *
    FROM #Results
	WHERE DBStatus NOT LIKE '%READ ONLY%'
		AND DBStatus NOT LIKE '%OFFLINE%'
		AND DBStatus NOT LIKE '%RESTORING%'
    ORDER BY RPOWorstCaseMinutes DESC, DatabaseName;
END
GO
-- Execute the procedure
EXEC usp_RPOWorstCaseMinutes;
