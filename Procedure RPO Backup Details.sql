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
go
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
    @DbNames NVARCHAR(MAX) = '', -- NULL: All DBs
    @LookBackDays INT = 7,       -- Must be > 0
    @LogToTable BIT = 0
)
AS
BEGIN

    -- Temporary table to store log backup statistics
    CREATE TABLE #LogBackupStats
    (
        DatabaseName NVARCHAR(128),
        TotalLogBackupSizeMB DECIMAL(18, 2),
        LogBackupCount INT,
        AvgBackupSpeedMBps DECIMAL(18, 2),
        TotalTimeSpentSeconds INT
    );

    -- Insert all databases into the temporary table
    INSERT INTO #LogBackupStats
    (
        DatabaseName,
        TotalLogBackupSizeMB,
        LogBackupCount,
        AvgBackupSpeedMBps,
        TotalTimeSpentSeconds
    )
    SELECT name AS DatabaseName,
           0 AS TotalLogBackupSizeMB,
           0 AS LogBackupCount,
           0 AS AvgBackupSpeedMBps,
           0 AS TotalTimeSpentSeconds
    FROM sys.databases;

    -- Update the temporary table with log backup statistics
    UPDATE #LogBackupStats
    SET TotalLogBackupSizeMB = ISNULL(backup_data.TotalLogBackupSizeMB, 0),
        LogBackupCount = ISNULL(backup_data.LogBackupCount, 0),
        AvgBackupSpeedMBps = ISNULL(backup_data.AvgBackupSpeedMBps, 0),
        TotalTimeSpentSeconds = ISNULL(backup_data.TotalTimeSpentSeconds, 0)
    FROM #LogBackupStats stats
        LEFT JOIN
        (
            SELECT bs.database_name AS DatabaseName,
                   SUM(bs.backup_size / 1048576.0) AS TotalLogBackupSizeMB, -- Convert bytes to MB
                   COUNT(*) AS LogBackupCount,
                   CASE
                       WHEN SUM(bs.backup_size / 1048576.0) = 0 THEN
                           0
                       ELSE
                           SUM(bs.backup_size / 1048576.0)
                           / NULLIF(SUM(DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date)), 0)
                   END AS AvgBackupSpeedMBps,
                   SUM(DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date)) AS TotalTimeSpentSeconds
            FROM msdb.dbo.backupset bs
            WHERE bs.type = 'L' -- Log backups
                  AND bs.backup_finish_date > DATEADD(DAY, -@LookBackDays, GETDATE())
            GROUP BY bs.database_name
        ) AS backup_data
            ON stats.DatabaseName = backup_data.DatabaseName;

    SELECT ag.name AS AvailabilityGroupName,
           dbcs.database_name AS DatabaseName,
           ag.automated_backup_preference_desc AS BackupPreference,
           ars.role_desc
    INTO #BackupPreference
    FROM sys.availability_groups ag
        JOIN sys.dm_hadr_availability_replica_states ars
            ON ag.group_id = ars.group_id
        JOIN sys.dm_hadr_database_replica_cluster_states dbcs
            ON ars.replica_id = dbcs.replica_id
	WHERE is_local = 1 AND name NOT LIKE 'TEMPDB'
    ORDER BY ag.name,
             dbcs.database_name;

    CREATE TABLE #TempHelpDB
    (
        name NVARCHAR(128),
        db_size NVARCHAR(128),
        owner NVARCHAR(128),
        dbid SMALLINT,
        created DATETIME,
        status NVARCHAR(512),
        compatibility_level TINYINT
    );

    -- Step 2: Insert the results of sp_helpdb into the temporary table
    INSERT INTO #TempHelpDB
    EXEC sp_helpdb;

    IF @LogToTable = 1
    BEGIN
        IF NOT EXISTS
        (
            SELECT *
            FROM sys.objects
            WHERE object_id = OBJECT_ID(N'[dbo].[tblRPODetails]')
                  AND type in ( N'U' )
        )
            CREATE TABLE [dbo].[tblRPODetails]
            (
                [DatabaseName] NVARCHAR(128) NULL,
                [DBSize] NVARCHAR(128) NULL,
                [AGName] NVARCHAR(128) NULL,
                [BackupPreference] NVARCHAR(128) NULL,
                [AGRole] NVARCHAR(128) NULL,
                [RPOWorstCaseMinutes] INT NULL,
                [RPOWorstCaseHour] AS CAST([RPOWorstCaseMinutes] / 60.0 AS DECIMAL(10, 2)),
                [RPOWorstCaseDays] AS CAST([RPOWorstCaseMinutes] / 1440.0 AS DECIMAL(10, 2)),
                [RecoveryModel] NVARCHAR(60) NULL,
                [DBStatus] VARCHAR(30) NOT NULL,
                TotalLogBackupSizeMB DECIMAL(18, 2),
                LogBackupCount INT,
                AvgBackupSpeedMBps DECIMAL(18, 2),
                LogBackupTimeInSec int,
                [RPOWorstCaseBackupSetFinishTime] DATETIME NULL,
                [RPOWorstCaseBackupSetPriorFinishTime] DATETIME NULL,
                [LookBackDays] INT NULL,
				[RunTimeUTC] DATETIME NOT NULL
            ) ON [PRIMARY];

    END

    -- Set default values if parameters are NULL or empty
    IF @DbNames IS NULL
       OR @DbNames = ''
        SET @DbNames = '%';

    -- Split the @DbNames into a temporary table
    CREATE TABLE #DbNameTable (DbName SYSNAME);
    INSERT INTO #DbNameTable
    (
        DbName
    )
    SELECT @DbNames

    -- Validate @LookBackDays
    IF @LookBackDays <= 0
    BEGIN
        RAISERROR('LookBackDays must be greater than 0', 16, 1);
        RETURN;
    END

    -- Temporary table to store RPO calculations
    CREATE TABLE #RPOWorstCase
    (
        DatabaseName NVARCHAR(128),
        RPOWorstCaseMinutes INT,
        RPOWorstCaseBackupSetFinishTime DATETIME,
        RPOWorstCaseBackupSetPriorFinishTime DATETIME
    );

    -- Calculate RPOWorstCaseMinutes
    DECLARE @StringToExecute NVARCHAR(MAX)
        = N'
        SELECT bs.database_name, bs.database_guid, bs.backup_set_id, bsPrior.backup_set_id AS backup_set_id_prior,
               bs.backup_finish_date, bsPrior.backup_finish_date AS backup_finish_date_prior,
               DATEDIFF(ss, bsPrior.backup_finish_date, bs.backup_finish_date) AS backup_gap_seconds
        INTO #backup_gaps
        FROM msdb.dbo.backupset AS bs WITH (NOLOCK)
        CROSS APPLY ( 
            SELECT TOP 1 bs1.backup_set_id, bs1.backup_finish_date
            FROM msdb.dbo.backupset AS bs1 WITH (NOLOCK)
            WHERE bs.database_name = bs1.database_name
                  AND bs.database_guid = bs1.database_guid
                  AND bs.backup_finish_date > bs1.backup_finish_date
                  AND bs.backup_set_id > bs1.backup_set_id
            ORDER BY bs1.backup_finish_date DESC, bs1.backup_set_id DESC 
        ) bsPrior
        WHERE bs.backup_finish_date > DATEADD(DD, -@LookBackDays, GETDATE())
          AND (bs.database_name LIKE @DbNames OR EXISTS (SELECT 1 FROM #DbNameTable WHERE DbName = bs.database_name));

        CREATE CLUSTERED INDEX cx_backup_gaps ON #backup_gaps (database_name, database_guid, backup_set_id, backup_finish_date, backup_gap_seconds);

        WITH max_gaps AS (
            SELECT g.database_name, g.database_guid, g.backup_set_id, g.backup_set_id_prior, g.backup_finish_date_prior, 
                   g.backup_finish_date, MAX(g.backup_gap_seconds) AS max_backup_gap_seconds 
            FROM #backup_gaps AS g
            GROUP BY g.database_name, g.database_guid, g.backup_set_id, g.backup_set_id_prior, g.backup_finish_date_prior, g.backup_finish_date
        )
        INSERT INTO #RPOWorstCase (DatabaseName, RPOWorstCaseMinutes, RPOWorstCaseBackupSetFinishTime, RPOWorstCaseBackupSetPriorFinishTime)
        SELECT bg.database_name, bg.max_backup_gap_seconds / 60.0,
               max(bg.backup_finish_date) AS RPOWorstCaseBackupSetFinishTime,
               max(bg.backup_finish_date_prior) AS RPOWorstCaseBackupSetPriorFinishTime
        FROM max_gaps bg
        LEFT OUTER JOIN max_gaps bgBigger ON bg.database_name = bgBigger.database_name AND bg.database_guid = bgBigger.database_guid AND bg.max_backup_gap_seconds < bgBigger.max_backup_gap_seconds
        WHERE bgBigger.backup_set_id IS NULL
		GROUP BY bg.database_name, bg.max_backup_gap_seconds / 60.0

        DROP TABLE #backup_gaps;
    ';

    EXEC sp_executesql @StringToExecute,
                       N'@DbNames NVARCHAR(MAX), @LookBackDays INT',
                       @DbNames,
                       @LookBackDays;

    -- Select the results
    IF @LogToTable = 1
    BEGIN
        INSERT INTO tblRPODetails
        SELECT D.name,
               THD.db_size,
               ISNULL(AvailabilityGroupName, 'Not part of AG') AGName,
               ISNULL(BP.BackupPreference, 'Not part of AG') BackupPreference,
               ISNULL(bp.role_desc, 'Not part of AG'),
               RPOWorstCaseMinutes,
               D.recovery_model_desc AS RecoveryModel,
               CASE
                   WHEN D.is_read_only = 1 THEN
                       'READ ONLY / ' + state_desc
                   ELSE
                       'READ WRITE / ' + state_desc
               END AS DBStatus,
               TotalLogBackupSizeMB,
               LogBackupCount,
               AvgBackupSpeedMBps,
               TotalTimeSpentSeconds as LogBackupTimeInSec,
               RPOWorstCaseBackupSetFinishTime,
               RPOWorstCaseBackupSetPriorFinishTime,
               @LookBackDays AS LookBackDays,
               GETUTCDATE() AS RunTimeUTC
        FROM sys.databases D
            LEFT JOIN #RPOWorstCase RWC
                ON RWC.DatabaseName = D.name
            LEFT JOIN #TempHelpDB THD
                ON THD.name = D.name
            LEFT JOIN #BackupPreference bp
                ON BP.DATABASENAME = D.NAME
            LEFT JOIN #LogBackupStats LBS
                ON LBS.DatabaseName = d.name
		WHERE D.name NOT LIKE 'tempdb'

        -- Clean up old records
        DELETE FROM tblRPODetails
        WHERE RunTimeUTC < DATEADD(DD, -180, GETUTCDATE());

        SELECT DISTINCT @@SERVERNAME AS ServerName,
               D.name,
               THD.db_size,
               ISNULL(AvailabilityGroupName, 'Not part of AG') AGName,
               ISNULL(BP.BackupPreference, 'Not part of AG') BackupPreference,
               ISNULL(bp.role_desc, 'Not part of AG') AGRole,
               RPOWorstCaseMinutes,
               CONVERT(DECIMAL(10, 2), ([RPOWorstCaseMinutes] / 60.0)) AS [RPOWorstCaseHour],
               CONVERT(DECIMAL(10, 2), ([RPOWorstCaseMinutes] / 1440.0)) AS [RPOWorstCaseDays],
               D.recovery_model_desc AS RecoveryModel,
               CASE
                   WHEN D.is_read_only = 1 THEN
                       'READ ONLY / ' + state_desc
                   ELSE
                       'READ WRITE / ' + state_desc
               END AS DBStatus,
               TotalLogBackupSizeMB,
               LogBackupCount,
               AvgBackupSpeedMBps,
               TotalTimeSpentSeconds as LogBackupTimeInSec,
               RPOWorstCaseBackupSetFinishTime,
               RPOWorstCaseBackupSetPriorFinishTime,
               @LookBackDays AS LookBackDays,
               GETUTCDATE() AS RunTimeUTC
        FROM sys.databases D
            LEFT JOIN #RPOWorstCase RWC
                ON RWC.DatabaseName = D.name
            LEFT JOIN #TempHelpDB THD
                ON THD.name = D.name
            LEFT JOIN #BackupPreference bp
                ON BP.DATABASENAME = D.NAME
            LEFT JOIN #LogBackupStats LBS
                ON LBS.DatabaseName = d.name
		WHERE D.name NOT LIKE 'tempdb'
        ORDER BY RPOWorstCaseMinutes DESC;
    END
    ELSE
    BEGIN
        SELECT DISTINCT @@SERVERNAME AS ServerName,
               D.name,
               THD.db_size DBSize,
               ISNULL(AvailabilityGroupName, 'Not part of AG') AGName,
               ISNULL(BP.BackupPreference, 'Not part of AG') BackupPreference,
               ISNULL(bp.role_desc, 'Not part of AG') AGRole,
               RPOWorstCaseMinutes,
               CONVERT(DECIMAL(10, 2), ([RPOWorstCaseMinutes] / 60.0)) AS [RPOWorstCaseHour],
               CONVERT(DECIMAL(10, 2), ([RPOWorstCaseMinutes] / 1440.0)) AS [RPOWorstCaseDays],
               D.recovery_model_desc AS RecoveryModel,
               CASE
                   WHEN D.is_read_only = 1 THEN
                       'READ ONLY / ' + state_desc
                   ELSE
                       'READ WRITE / ' + state_desc
               END AS DBStatus,
               TotalLogBackupSizeMB,
               LogBackupCount,
               AvgBackupSpeedMBps,
               TotalTimeSpentSeconds as LogBackupTimeInSec,
               RPOWorstCaseBackupSetFinishTime,
               RPOWorstCaseBackupSetPriorFinishTime,
               @LookBackDays AS LookBackDays,
               GETUTCDATE() AS RunTimeUTC
        FROM sys.databases D
            LEFT JOIN #RPOWorstCase RWC
                ON D.name = RWC.DatabaseName
            LEFT JOIN #TempHelpDB THD
                ON THD.name = D.name
            LEFT JOIN #BackupPreference bp
                ON BP.DATABASENAME = D.NAME
            LEFT JOIN #LogBackupStats LBS
                ON LBS.DatabaseName = d.name
		WHERE D.name NOT LIKE 'tempdb'
        ORDER BY TotalLogBackupSizeMB DESC;
    END
END
GO

-- Execute the procedure
EXEC usp_RPOWorstCaseMinutes;
