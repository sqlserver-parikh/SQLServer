USE tempdb
GO
CREATE OR ALTER PROCEDURE usp_BackupReport
(
    @DbNames NVARCHAR(MAX) = '' -- NULL: All DBs
  , @BackupType CHAR(1) = 'L'   -- D: Full, L: Log, I: Incremental, NULL: All backup types
  , @LookBackDays INT = 7  -- Must be > 0
  , @MinBackupSizeMB INT = 0 -- 0 will show all backups 1024 will show all backups > than 1GB in size
  , @BackupGrowthReport BIT = 1 --If this is 1 then only @dbname parameter is used others are not used
)
AS
BEGIN
    -- Set default values if parameters are NULL or empty
    IF @DbNames IS NULL
       OR @DbNames = ''
        SET @DbNames = '%';
    -- Split the @DbNames into a table
    DECLARE @DbNameTable TABLE (DbName sysname);
    INSERT INTO @DbNameTable
    (
        DbName
    )
    SELECT @DbNames
    IF @BackupGrowthReport = 0
    BEGIN
        -- Validate @LookBackDays
        IF @LookBackDays <= 0
        BEGIN
            RAISERROR('LookBackDays must be greater than 0', 16, 1);
            RETURN;
        END
		IF @MinBackupSizeMB < 0
        BEGIN
            RAISERROR('Minimum backup size must be greater than 0', 16, 1);
            RETURN;
        END
        -- Validate @BackupType
        IF @BackupType IS NOT NULL
           AND @BackupType NOT IN ( '', 'D', 'L', 'I' )
        BEGIN
            RAISERROR('BackupType must be either NULL, '', D, L, or I', 16, 1);
            RETURN;
        END

        IF @BackupType IS NULL
           OR @BackupType = ''
            SET @BackupType = '%';



        -- Select backup report
        SELECT DISTINCT
            server_name                                                                                                              AS ServerName
          , T1.name                                                                                                                  AS DatabaseName
          , T3.backup_start_date                                                                                                     AS Bkp_StartDate
          , T3.backup_finish_date                                                                                                    AS Bkp_FinishDate
          , DATEDIFF(SS, T3.backup_start_date, T3.backup_finish_date)                                                                AS Bkp_Time_Sec
          , T3.type                                                                                                                  AS Bkp_Type
          , (T3.backup_size / 1048576.0)                                                                                             AS BackupSizeMB
          , (T3.compressed_backup_size / 1048576.0)                                                                                  AS CompressedBackupSizeMB
          , (CAST((T3.backup_size / 1048576.0) / (DATEDIFF(SS, T3.backup_start_date, T3.backup_finish_date) + 1) AS DECIMAL(10, 2))) AS MBPS
          , user_name                                                                                                                AS UserName
          , physical_device_name                                                                                                     AS BackupLocation
        FROM master..sysdatabases             AS T1
            LEFT JOIN msdb..backupset         AS T3
                ON T3.database_name = T1.name
            LEFT JOIN msdb..backupmediaset    AS T5
                ON T3.media_set_id = T5.media_set_id
            LEFT JOIN msdb..backupmediafamily AS T6
                ON T6.media_set_id = T5.media_set_id
        WHERE 1 = 1
              AND T3.type LIKE @BackupType
              AND (
                      T1.name LIKE @DbNames
                      OR EXISTS
        (
            SELECT 1 FROM @DbNameTable WHERE DbName = T1.name
        )
                  )
              AND T3.backup_finish_date > DATEADD(DD, -@LookBackDays, GETDATE())
              AND DATABASEPROPERTYEX(T1.name, 'STATUS') = 'ONLINE'
              AND T1.name <> 'tempdb'
			  AND (T3.backup_size / 1048576.0)  > @MinBackupSizeMB
        ORDER BY T3.backup_finish_date DESC
               , T3.backup_start_date DESC;
    END;

    ELSE
    BEGIN

	------------------------------
	 CREATE TABLE #DbNameTable (DbName sysname);
    INSERT INTO #DbNameTable (DbName)
    SELECT value
    FROM STRING_SPLIT(@DbNames, ',');

    -- Temporary table to store RPO calculations
    CREATE TABLE #RPOWorstCase
    (
        DatabaseName NVARCHAR(128),
        RPOWorstCaseMinutes INT,
        RPOWorstCaseBackupSetID INT,
        RPOWorstCaseBackupSetFinishTime DATETIME,
        RPOWorstCaseBackupSetIDPrior INT,
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

	------------------------------

		SELECT 
    ag.name AS AvailabilityGroupName,
    dbcs.database_name AS DatabaseName,
    ag.automated_backup_preference_desc AS BackupPreference,
	ars.role_desc
INTO #BackupPreference
FROM 
    sys.availability_groups ag
JOIN 
    sys.dm_hadr_availability_replica_states ars ON ag.group_id = ars.group_id
JOIN 
    sys.dm_hadr_database_replica_cluster_states dbcs ON ars.replica_id = dbcs.replica_id
WHERE is_local = 1 
ORDER BY 
    ag.name, dbcs.database_name;

        -- Create a temporary table to store the intermediate results
        CREATE TABLE #IntermediateFreeSpace
        (
            DatabaseName NVARCHAR(128)
          , DataFreeMB FLOAT
          , LogFreeMB FLOAT
          , DBSizeMB FLOAT
        );

        DECLARE @db_name NVARCHAR(128);
        DECLARE @sql NVARCHAR(MAX);

        SET @sql = '';

        DECLARE db_cursor CURSOR FOR
        SELECT name
        FROM sys.databases
        WHERE state_desc = 'ONLINE';

        OPEN db_cursor;
        FETCH NEXT FROM db_cursor
        INTO @db_name;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @sql
                = @sql + '
    USE ['             + @db_name
                  + '];
    INSERT INTO #IntermediateFreeSpace (DatabaseName, DataFreeMB, LogFreeMB, DBSizeMB)
    SELECT
        DB_NAME() AS DatabaseName,
        SUM(CASE WHEN type = 0 THEN size * 8.0 / 1024 - FILEPROPERTY(name, ''SpaceUsed'') * 8.0 / 1024 ELSE 0 END) AS DataFreeMB,
        SUM(CASE WHEN type = 1 THEN size * 8.0 / 1024 - FILEPROPERTY(name, ''SpaceUsed'') * 8.0 / 1024 ELSE 0 END) AS LogFreeMB,
		convert(decimal(20,2),SUM (SIZE/128.0)) DBSize
    FROM sys.database_files
    WHERE type IN (0, 1)
    '       ;

            FETCH NEXT FROM db_cursor
            INTO @db_name;
        END;

        CLOSE db_cursor;
        DEALLOCATE db_cursor;

        -- Execute the dynamic SQL
        EXEC sp_executesql @sql;

        -- Create the final temporary table to store the formatted results
        CREATE TABLE #FreeSpace
        (
            DatabaseName NVARCHAR(128)
          , FreeSpace NVARCHAR(256)
        );

        -- Insert formatted results into the final temporary table
        INSERT INTO #FreeSpace
        (
            DatabaseName
          , FreeSpace
        )
        SELECT DatabaseName
             ,' DataFree: '
               + CAST(convert(decimal(20, 2), DataFreeMB) AS NVARCHAR(50)) + 'MB LogFree: '
               + CAST(convert(decimal(20, 2), LogFreeMB) AS NVARCHAR(50)) + 'MB'
        FROM #IntermediateFreeSpace;
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

        -- Select the results from the final temporary table

        DECLARE @startDate DATETIME;
        SET @startDate = GETDATE();
        SELECT PVT.DatabaseName
             , PVT.[0]
             , PVT.[-1]
             , PVT.[-2]
             , PVT.[-3]
             , PVT.[-4]
             , PVT.[-5]
             , PVT.[-6]
             , PVT.[-7]
             , PVT.[-8]
             , PVT.[-9]
             , PVT.[-10]
             , PVT.[-11]
             , PVT.[-12]
        INTO #GROWTH
        FROM
        (
            SELECT BS.database_name                                       AS DatabaseName
                 , DATEDIFF(mm, @startDate, BS.backup_start_date)         AS MonthsAgo
                 , CONVERT(NUMERIC(10, 1), AVG(BF.file_size / 1048576.0)) AS AvgSizeMB
            FROM msdb.dbo.backupset            AS BS
                INNER JOIN msdb.dbo.backupfile AS BF
                    ON BS.backup_set_id = BF.backup_set_id
            WHERE 1 = 1
                  AND BF.[file_type] = 'D'
                  AND BS.backup_start_date
                  BETWEEN DATEADD(yy, -1, @startDate) AND @startDate
            GROUP BY BS.database_name
                   , DATEDIFF(mm, @startDate, BS.backup_start_date)
        ) AS BCKSTAT
        PIVOT
        (
            SUM(BCKSTAT.AvgSizeMB)
            FOR BCKSTAT.MonthsAgo IN ([0], [-1], [-2], [-3], [-4], [-5], [-6], [-7], [-8], [-9], [-10], [-11], [-12])
        ) AS PVT
        ORDER BY PVT.DatabaseName;
        SELECT ISNULL(d.[name], bs.[database_name]) AS DBName
             , d.recovery_model_desc                AS [Recovery Model]
             , d.log_reuse_wait_desc                AS [Log Reuse Wait Desc]
             , MAX(   CASE
                          WHEN [type] = 'D' THEN
                              bs.backup_finish_date
                          ELSE
                              NULL
                      END
                  )                                 AS [Last Full Backup]
             , MAX(   CASE
                          WHEN [type] = 'I' THEN
                              bs.backup_finish_date
                          ELSE
                              NULL
                      END
                  )                                 AS [Last Differential Backup]
             , MAX(   CASE
                          WHEN [type] = 'L' THEN
                              bs.backup_finish_date
                          ELSE
                              NULL
                      END
                  )                                 AS [Last Log Backup]
        INTO #BACKUP
        FROM sys.databases                     AS d WITH (NOLOCK)
            LEFT OUTER JOIN msdb.dbo.backupset AS bs WITH (NOLOCK)
                ON bs.[database_name] = d.[name]
                   AND bs.backup_finish_date > GETDATE() - 90
        WHERE d.name <> N'tempdb'
        GROUP BY ISNULL(d.[name], bs.[database_name])
               , d.recovery_model_desc
               , d.log_reuse_wait_desc
               , d.[name]
        ORDER BY d.recovery_model_desc
               , d.[name]
        OPTION (RECOMPILE);
        SELECT D.name
			, THD.db_size
             , DS.FreeSpace
			 , CASE
                   WHEN D.is_read_only = 1 THEN
                       'READ ONLY / ' + state_desc + ' / ' + user_access_desc
                   ELSE
                       'READ WRITE / ' + state_desc + ' / ' + user_access_desc
               END AS DBStatus
			 , d.compatibility_level
             , d.recovery_model_desc
             , d.log_reuse_wait_desc
			 , ISNULL(AvailabilityGroupName,'Not part of AG') AGName
			 , ISNULL(BP.BackupPreference,'Not part of AG') BackupPreference
			 , ISNULL(role_desc,'Not part of AG') AGRole
             , SUSER_SNAME(owner_sid)    DBOwnerName
             , [Last Full Backup]
             , [Last Differential Backup]
             , [Last Log Backup]
			 , RWC.RPOWorstCaseMinutes
			   ,RWC.RPOWorstCaseBackupSetFinishTime
              , RWC.RPOWorstCaseBackupSetPriorFinishTime
			, LBS.TotalLogBackupSizeMB
			, LBS.AvgBackupSpeedMBps AvgLogBackupMBPS
			  , LBS.LogBackupCount
			  , lbs.TotalTimeSpentSeconds LogBackupTime
			 , @LookBackDays LookBack
             , [0]
             , [-1]
             , [-2]
             , [-3]
             , [-4]
             , [-5]
             , [-6]
             , [-7]
             , [-8]
             , [-9]
             , [-10]
             , [-11]
             , [-12]
			 , getutcdate() ReportTimeUTC
        FROM SYS.DATABASES       D
            LEFT JOIN #BACKUP    B
                on D.name = B.DBName
left join #RPOWorstCase RWC ON D.name = RWC.DatabaseName
				
            LEFT JOIN #GROWTH    G
                ON D.name = G.DatabaseName
            left join #FreeSpace DS
                on D.name = DS.DatabaseName
			LEFT JOIN #BackupPreference bp ON BP.DATABASENAME = D.NAME
			LEFT JOIN #LogBackupStats LBS ON LBS.DatabaseName = D.name
			LEFT JOIN #TempHelpDB THD ON THD.name = D.name
        WHERE D.name <> 'tempdb' and (
                  D.name LIKE @DbNames
                  OR EXISTS
        (
            SELECT 1 FROM @DbNameTable WHERE DbName = D.name
        )
              )
        order by [0] desc
        DROP TABLE #BACKUP
        DROP TABLE #GROWTH
        DROP TABLE #IntermediateFreeSpace;
        DROP TABLE #FreeSpace;
    END
END
GO
EXEC usp_BackupReport
