USE tempdb;
GO

CREATE OR ALTER PROCEDURE usp_RPOWorstCaseMinutes
(
    @DbNames NVARCHAR(MAX) = '', -- NULL: All DBs
    @LookBackDays INT = 4,       -- Must be > 0
    @LogToTable BIT = 0
)
AS
BEGIN

CREATE TABLE #TempHelpDB (
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
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblRPODetails]') AND type in (N'U'))
        CREATE TABLE [dbo].tblRPODetails (
            [DatabaseName] NVARCHAR(128) NULL,
			[DBSize] nvarchar(128) NULL,
            [RPOWorstCaseMinutes] INT NULL,
            [RecoveryModel] NVARCHAR(60) NULL,
            [DBStatus] VARCHAR(20) NOT NULL,
            [RunTimeUTC] DATETIME NOT NULL,
            [RPOWorstCaseBackupSetFinishTime] DATETIME NULL,
            [RPOWorstCaseBackupSetPriorFinishTime] DATETIME NULL,
            [LookBackDays] INT NULL
        ) ON [PRIMARY];
    END

    -- Set default values if parameters are NULL or empty
    IF @DbNames IS NULL OR @DbNames = ''
        SET @DbNames = '%';

    -- Split the @DbNames into a temporary table
    CREATE TABLE #DbNameTable (DbName SYSNAME);
    INSERT INTO #DbNameTable (DbName)
    SELECT value
    FROM STRING_SPLIT(@DbNames, ',');

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
    DECLARE @StringToExecute NVARCHAR(MAX) = N'
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
               bg.backup_finish_date AS RPOWorstCaseBackupSetFinishTime,
               bg.backup_finish_date_prior AS RPOWorstCaseBackupSetPriorFinishTime
        FROM max_gaps bg
        LEFT OUTER JOIN max_gaps bgBigger ON bg.database_name = bgBigger.database_name AND bg.database_guid = bgBigger.database_guid AND bg.max_backup_gap_seconds < bgBigger.max_backup_gap_seconds
        WHERE bgBigger.backup_set_id IS NULL;

        DROP TABLE #backup_gaps;
    ';

    EXEC sp_executesql @StringToExecute, N'@DbNames NVARCHAR(MAX), @LookBackDays INT', @DbNames, @LookBackDays;

    -- Select the results
    IF @LogToTable = 1
    BEGIN
        INSERT INTO tblRPODetails
        SELECT DatabaseName, THD.db_size, RPOWorstCaseMinutes,
               D.recovery_model_desc AS RecoveryModel,
               CASE WHEN D.is_read_only = 1 THEN 'READ ONLY' ELSE 'READ WRITE' END AS DBStatus,
               GETUTCDATE() AS RunTimeUTC,
               RPOWorstCaseBackupSetFinishTime,
               RPOWorstCaseBackupSetPriorFinishTime,
               @LookBackDays AS LookBackDays
        FROM #RPOWorstCase RWC
        LEFT JOIN sys.databases D ON RWC.DatabaseName = D.name
		LEFT JOIN #TempHelpDB THD ON THD.name = D.name;

        -- Clean up old records
        DELETE FROM tblRPODetails
        WHERE RunTimeUTC < DATEADD(DD, -180, GETUTCDATE());

        SELECT @@SERVERNAME AS ServerName, DatabaseName,THD.db_size DBSize, RPOWorstCaseMinutes,
               D.recovery_model_desc AS RecoveryModel,
               CASE WHEN D.is_read_only = 1 THEN 'READ ONLY' ELSE 'READ WRITE' END AS DBStatus,
               GETUTCDATE() AS RunTimeUTC,
               RPOWorstCaseBackupSetFinishTime,
               RPOWorstCaseBackupSetPriorFinishTime,
               @LookBackDays AS LookBackDays
        FROM #RPOWorstCase RWC
        LEFT JOIN sys.databases D ON RWC.DatabaseName = D.name
		LEFT JOIN #TempHelpDB THD ON THD.name = D.name
        ORDER BY RPOWorstCaseMinutes DESC;
    END
    ELSE 
    BEGIN 
        SELECT @@SERVERNAME AS ServerName, DatabaseName,THD.db_size DBSize, RPOWorstCaseMinutes,
               D.recovery_model_desc AS RecoveryModel,
               CASE WHEN D.is_read_only = 1 THEN 'READ ONLY' ELSE 'READ WRITE' END AS DBStatus,
               GETUTCDATE() AS RunTimeUTC,
               RPOWorstCaseBackupSetFinishTime,
               RPOWorstCaseBackupSetPriorFinishTime,
               @LookBackDays AS LookBackDays
        FROM #RPOWorstCase RWC
        LEFT JOIN sys.databases D ON RWC.DatabaseName = D.name
		LEFT JOIN #TempHelpDB THD ON THD.name = D.name
        ORDER BY RPOWorstCaseMinutes DESC;
    END

    -- Clean up
    DROP TABLE #RPOWorstCase;
    DROP TABLE #DbNameTable;
END
GO

-- Execute the procedure
EXEC usp_RPOWorstCaseMinutes;
