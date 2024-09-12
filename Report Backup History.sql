CREATE OR ALTER PROCEDURE #usp_BackupReport
(
    @DbNames NVARCHAR(MAX) = '' -- NULL: All DBs
  , @BackupType CHAR(1) = 'D'   -- D: Full, L: Log, I: Incremental, NULL: All backup types
  , @LookBackDays INT = 7       -- Must be > 0
  , @BackupGrowthReport BIT = 0 --If this is 1 then only @dbname parameter is used others are not used
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
    SELECT value
    FROM STRING_SPLIT(@DbNames, ',');
    IF @BackupGrowthReport = 0
    BEGIN
        -- Validate @LookBackDays
        IF @LookBackDays <= 0
        BEGIN
            RAISERROR('LookBackDays must be greater than 0', 16, 1);
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
        ORDER BY T3.backup_finish_date DESC
               , T3.backup_start_date DESC;
    END;

    ELSE
    BEGIN
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
             , 'TotalSize:' + CAST(convert(decimal(20, 2), DBSizeMB) as nvarchar(50)) + ' DataFree: '
               + CAST(convert(decimal(20, 2), DataFreeMB) AS NVARCHAR(50)) + 'MB LogFree: '
               + CAST(convert(decimal(20, 2), LogFreeMB) AS NVARCHAR(50)) + 'MB'
        FROM #IntermediateFreeSpace;

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
             , d.user_access_desc
             , d.state_desc
             , SUSER_SNAME(owner_sid)    DBOwnerName
             , d.compatibility_level
             , d.recovery_model_desc
             , d.log_reuse_wait_desc
             , [Last Full Backup]
             , [Last Differential Backup]
             , [Last Log Backup]
             , DS.FreeSpace
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
        FROM SYS.DATABASES       D
            LEFT JOIN #BACKUP    B
                on D.name = B.DBName
            LEFT JOIN #GROWTH    G
                ON D.name = G.DatabaseName
            left join #FreeSpace DS
                on D.name = DS.DatabaseName
        WHERE (
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
#usp_BackupReport
