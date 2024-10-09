USE tempdb;
GO

CREATE OR ALTER PROCEDURE usp_RestoreDatabase
(
    @DB_NAME SYSNAME = 'dbasupport',                    -- The name of the database to restore
    @RESTORE_TO_DATETIME DATETIME = NULL -- The point in time to restore to (optional, will use latest backup if NULL)
)
AS
BEGIN
    SET NOCOUNT ON;

    -- Default restore time to the current date and time if not provided
    IF @RESTORE_TO_DATETIME IS NULL
    BEGIN
        SET @RESTORE_TO_DATETIME = GETDATE();
    END

    DECLARE @SERVER_NAME NVARCHAR(512);
    SET @SERVER_NAME = CAST(SERVERPROPERTY(N'servername') AS NVARCHAR(512));

    DECLARE @FIRST_FULL_BACKUPSET_ID INTEGER, 
            @FIRST_FULL_BACKUP_STARTDATE DATETIME;

    -- Create a temporary table to hold backup set information
    CREATE TABLE #backupset
    (
        backup_set_id      INTEGER NOT NULL,
        is_in_restore_plan BIT NOT NULL,
        backup_start_date  DATETIME NOT NULL,
        type               CHAR(1) NOT NULL,
        database_name      NVARCHAR(256) NOT NULL
    );

    /**********************************************************************/
    /* IDENTIFY THE FIRST FULL DATABASE BACKUP NEEDED IN THE RESTORE PLAN */
    /**********************************************************************/
    SELECT @FIRST_FULL_BACKUPSET_ID = backupset_outer.backup_set_id,
           @FIRST_FULL_BACKUP_STARTDATE = backupset_outer.backup_start_date
    FROM msdb.dbo.backupset backupset_outer
    WHERE LOWER(backupset_outer.database_name) = LOWER(@DB_NAME)
          AND LOWER(backupset_outer.server_name) = LOWER(@SERVER_NAME)
          AND backupset_outer.type = 'D'  -- FULL DATABASE BACKUP
          AND backupset_outer.backup_start_date =
    (
        SELECT MAX(backupset_inner.backup_start_date)
        FROM msdb.dbo.backupset backupset_inner
        WHERE LOWER(backupset_inner.database_name) = LOWER(backupset_outer.database_name)
              AND LOWER(backupset_inner.server_name) = LOWER(@SERVER_NAME)
              AND backupset_inner.type = backupset_outer.type
              AND backupset_inner.backup_start_date <= @RESTORE_TO_DATETIME
              AND backupset_inner.is_copy_only = 0
    )
          AND backupset_outer.is_copy_only = 0;

    /**************************************************************************/
    /* INSERT THE FULL DATABASE BACKUP INTO THE WORK TABLE (#backupset)        */
    /**************************************************************************/
    INSERT INTO #backupset (backup_set_id, is_in_restore_plan, backup_start_date, type, database_name)
    SELECT backup_set_id, 1, backup_start_date, type, database_name
    FROM msdb.dbo.backupset
    WHERE msdb.dbo.backupset.backup_set_id = @FIRST_FULL_BACKUPSET_ID
          AND LOWER(msdb.dbo.backupset.server_name) = LOWER(@SERVER_NAME);

    /******************************************************************/
    /* INSERT THE LOG AND DIFFERENTIAL BACKUPS AFTER THE FULL BACKUP  */
    /******************************************************************/
    INSERT INTO #backupset (backup_set_id, is_in_restore_plan, backup_start_date, type, database_name)
    SELECT backup_set_id, 0, backup_start_date, type, database_name
    FROM msdb.dbo.backupset
    WHERE LOWER(msdb.dbo.backupset.database_name) = LOWER(@DB_NAME)
          AND LOWER(msdb.dbo.backupset.server_name) = LOWER(@SERVER_NAME)
          AND msdb.dbo.backupset.type IN ('I', 'L')  -- DIFFERENTIAL AND LOG BACKUPS
          AND msdb.dbo.backupset.backup_start_date >= @FIRST_FULL_BACKUP_STARTDATE;

    /********************************************************************/
    /* MARK THE BACKUPS THAT SHOULD BE INCLUDED IN THE RESTORE PLAN      */
    /********************************************************************/
    -- Mark the latest differential backup
    UPDATE #backupset
    SET is_in_restore_plan = 1
    WHERE type = 'I'
          AND backup_start_date =
    (
        SELECT MAX(backupset_inner.backup_start_date)
        FROM #backupset backupset_inner
        WHERE backupset_inner.type = 'I'
              AND backupset_inner.backup_start_date <= @RESTORE_TO_DATETIME
    );

    -- Mark the log backups that are needed after the differential or full
    UPDATE #backupset
    SET is_in_restore_plan = 1
    WHERE type = 'L'
          AND backup_start_date <= @RESTORE_TO_DATETIME
          AND backup_start_date >=
    (
        SELECT backupset_inner.backup_start_date
        FROM #backupset backupset_inner
        WHERE backupset_inner.type = 'I'
              AND backupset_inner.is_in_restore_plan = 1
    );

    /************************************************************************/
    /* IF NO DIFFERENTIAL, INCLUDE ALL LOG BACKUPS AFTER THE FULL BACKUP    */
    /************************************************************************/
    UPDATE #backupset
    SET is_in_restore_plan = 1
    WHERE type = 'L'
          AND backup_start_date <= @RESTORE_TO_DATETIME
          AND NOT EXISTS
    (
        SELECT *
        FROM #backupset backupset_inner
        WHERE backupset_inner.type = 'I'
    );

    -- Prepare the restore commands
    SELECT CASE
               WHEN A.type = 'D' OR A.type = 'I'
               THEN 'RESTORE DATABASE ' + A.database_name + ' FROM DISK = N''' + B.physical_device_name + ''' WITH FILE = ' + CAST(family_sequence_number AS VARCHAR(10)) + ', NORECOVERY;'
               WHEN A.type = 'L'
               THEN 'RESTORE LOG ' + A.database_name + ' FROM DISK = N''' + B.physical_device_name + ''' WITH FILE = ' + CAST(family_sequence_number AS VARCHAR(10)) + ', NORECOVERY;'
           END AS RestoreCommand
    FROM #backupset A
    INNER JOIN msdb.dbo.backupmediafamily B ON A.backup_set_id = B.media_set_id
    WHERE is_in_restore_plan = 1
    UNION ALL
    SELECT 'RESTORE DATABASE ' + @DB_NAME + ' WITH RECOVERY;' AS RestoreCommand;

    -- Clean up temporary tables
    DROP TABLE #backupset;
END;
GO
usp_restoredatabase
