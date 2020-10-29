SET NOCOUNT ON;
DECLARE @databaseName SYSNAME;
CREATE TABLE #TmpCommands
(ID  INT IDENTITY(1, 1),
 Cmd VARCHAR(8000)
);
DECLARE dbnames_cursor CURSOR
FOR SELECT name
    FROM master..sysdatabases
    WHERE name NOT IN('model', 'tempdb', 'pubs', 'northwind')
    AND (status&32) = 0 -- Do not include loading 
    AND (status&64) = 0 -- Do not include loading 
    AND (status&128) = 0 -- Do not include recovering 
    AND (status&256) = 0 -- Do not include not recovered 
    AND (status&512) = 0 -- Do not include offline 
    AND (status&32768) = 0 -- Do not include emergency 
    AND (status&1073741824) = 0; -- Do not include cleanly shutdown 

OPEN dbnames_cursor;
FETCH NEXT FROM dbnames_cursor INTO @databaseName;
WHILE(@@FETCH_STATUS <> -1)
    BEGIN
        IF(@@FETCH_STATUS <> -2)
            BEGIN
                INSERT INTO #TmpCommands(Cmd)
            VALUES('----------------Script to Restore the '+@databaseName+' Database--------------');
                DECLARE @backupStartDate DATETIME;
                DECLARE @backup_set_id_start INT;
                DECLARE @backup_set_id_end INT;
                SELECT @backup_set_id_start = MAX(backup_set_id)
                FROM msdb.dbo.backupset
                WHERE database_name = @databaseName
                      AND type = 'D';
                SELECT @backup_set_id_end = MIN(backup_set_id)
                FROM msdb.dbo.backupset
                WHERE database_name = @databaseName
                      AND type = 'D'
                      AND backup_set_id > @backup_set_id_start;
                IF @backup_set_id_end IS NULL
                    SET @backup_set_id_end = 999999999;
                INSERT INTO #TmpCommands(Cmd)
                       SELECT Cmd
                       FROM
(
    SELECT backup_set_id,
           'RESTORE DATABASE '+@databaseName+' FROM DISK = '''+mf.physical_device_name+''' WITH NORECOVERY --' Cmd
    FROM msdb.dbo.backupset b,
         msdb.dbo.backupmediafamily mf
    WHERE b.media_set_id = mf.media_set_id
          AND b.database_name = @databaseName
          AND b.backup_set_id = @backup_set_id_start
    UNION
    SELECT backup_set_id,
           'RESTORE LOG '+@databaseName+' FROM DISK = '''+mf.physical_device_name+''' WITH FILE = '+CAST(position AS VARCHAR(10))+', NORECOVERY --' Cmd
    FROM msdb.dbo.backupset b,
         msdb.dbo.backupmediafamily mf
    WHERE b.media_set_id = mf.media_set_id
          AND b.database_name = @databaseName
          AND b.backup_set_id >= @backup_set_id_start
          AND b.backup_set_id < @backup_set_id_end
          AND b.type = 'L'
    UNION
    SELECT 999999999 AS backup_set_id,
           'RESTORE DATABASE '+@databaseName+' WITH RECOVERY --' Cmd
) A
                       ORDER BY backup_set_id;
            END;
        FETCH NEXT FROM dbnames_cursor INTO @DatabaseName;
    END;
CLOSE dbnames_cursor;
DEALLOCATE dbnames_cursor;
DECLARE @PrintCommand VARCHAR(8000);
DECLARE Print_cursor CURSOR
FOR SELECT Cmd
    FROM #TmpCommands
    ORDER BY ID;
OPEN Print_cursor;
FETCH NEXT FROM Print_cursor INTO @PrintCommand;
WHILE(@@FETCH_STATUS <> -1)
    BEGIN
        IF(@@FETCH_STATUS <> -2)
            BEGIN
                PRINT @PrintCommand;
            END;
        FETCH NEXT FROM Print_cursor INTO @PrintCommand;
    END;
CLOSE Print_cursor;
DEALLOCATE Print_cursor;
DROP TABLE #TmpCommands; 




-------------------------------------------------------------------------------------------------------------------------


DECLARE @DB_NAME SYSNAME, @RESTORE_TO_DATETIME DATETIME;
SELECT @DB_NAME = N'ADVENTUREWORKSs';
SELECT @RESTORE_TO_DATETIME = GETDATE();
DECLARE @SERVER_NAME NVARCHAR(512);
SET @SERVER_NAME = CAST(SERVERPROPERTY(N'SERVERNAME') AS NVARCHAR(512));
DECLARE @FIRST_FULL_BACKUPSET_ID INTEGER, @FIRST_FULL_BACKUP_STARTDATE DATETIME;
CREATE TABLE #BACKUPSET
(BACKUP_SET_ID      INTEGER NOT NULL,
 IS_IN_RESTORE_PLAN BIT NOT NULL,
 BACKUP_START_DATE  DATETIME NOT NULL,
 TYPE               CHAR(1) NOT NULL,
 DATABASE_NAME      NVARCHAR(256) NOT NULL
);

/**********************************************************************/

/* IDENTIFY THE FIRST */

/**********************************************************************/

SELECT @FIRST_FULL_BACKUPSET_ID = BACKUPSET_OUTER.BACKUP_SET_ID,
       @FIRST_FULL_BACKUP_STARTDATE = BACKUPSET_OUTER.BACKUP_START_DATE
FROM MSDB.DBO.BACKUPSET BACKUPSET_OUTER
WHERE BACKUPSET_OUTER.DATABASE_NAME = @DB_NAME
      AND BACKUPSET_OUTER.SERVER_NAME = @SERVER_NAME
      AND BACKUPSET_OUTER.TYPE = 'D' -- FULL DATABASE BACKUP   
      AND BACKUPSET_OUTER.BACKUP_START_DATE =
(
    SELECT MAX(BACKUPSET_INNER.BACKUP_START_DATE)
    FROM MSDB.DBO.BACKUPSET BACKUPSET_INNER
    WHERE BACKUPSET_INNER.DATABASE_NAME = BACKUPSET_OUTER.DATABASE_NAME
          AND BACKUPSET_INNER.SERVER_NAME = @SERVER_NAME
          AND BACKUPSET_INNER.TYPE = BACKUPSET_OUTER.TYPE
          AND BACKUPSET_INNER.BACKUP_START_DATE <= @RESTORE_TO_DATETIME
          AND BACKUPSET_INNER.IS_COPY_ONLY = 0
)
      AND BACKUPSET_OUTER.IS_COPY_ONLY = 0;

/*******************************************************************************************/

/* FIND THE FIRST FULL DATABASE BACKUP NEEDED IN THE RESTORE PLAN AND STORE ITS ATTRIBUTES */

/* IN #BACKUPSET WORK TABLE */

/*******************************************************************************************/

INSERT INTO #BACKUPSET
(BACKUP_SET_ID,
 IS_IN_RESTORE_PLAN,
 BACKUP_START_DATE,
 TYPE,
 DATABASE_NAME
)
       SELECT BACKUP_SET_ID,
              1
              , -- THE FULL DATABASE BACKUP IS ALWAYS NEEDED FOR THE RESTORE PLAN 
              BACKUP_START_DATE,
              TYPE,
              DATABASE_NAME
       FROM MSDB.DBO.BACKUPSET
       WHERE MSDB.DBO.BACKUPSET.BACKUP_SET_ID = @FIRST_FULL_BACKUPSET_ID
             AND MSDB.DBO.BACKUPSET.SERVER_NAME = @SERVER_NAME;

/***************************************************************/

/* FIND THE LOG AND DIFFERENTIAL BACKUPS THAT OCCURRED AFTER */

/* THE FULL BACKUP AND STORE THEM IN #BACKUPSET WORK TABLE */

/***************************************************************/

INSERT INTO #BACKUPSET
(BACKUP_SET_ID,
 IS_IN_RESTORE_PLAN,
 BACKUP_START_DATE,
 TYPE,
 DATABASE_NAME
)
       SELECT BACKUP_SET_ID,
              0,
              BACKUP_START_DATE,
              TYPE,
              DATABASE_NAME
       FROM MSDB.DBO.BACKUPSET
       WHERE MSDB.DBO.BACKUPSET.DATABASE_NAME = @DB_NAME
             AND MSDB.DBO.BACKUPSET.SERVER_NAME = @SERVER_NAME
             AND MSDB.DBO.BACKUPSET.TYPE IN('I', 'L') -- DIFFERENTIAL, LOG BACKUPS
       AND MSDB.DBO.BACKUPSET.BACKUP_START_DATE >= @FIRST_FULL_BACKUP_STARTDATE;
   
/**********************************************************************************/

/* IDENTIFY AND MARK THE BACKUP LOGS THAT NEED TO BE INCLUDED IN THE RESTORE PLAN */

/**********************************************************************************/

UPDATE #BACKUPSET
  SET
      IS_IN_RESTORE_PLAN = 1
WHERE #BACKUPSET.TYPE = 'I'
      AND #BACKUPSET.BACKUP_START_DATE =
(
    SELECT MAX(BACKUPSET_INNER.BACKUP_START_DATE)
    FROM #BACKUPSET BACKUPSET_INNER
    WHERE BACKUPSET_INNER.TYPE = #BACKUPSET.TYPE
          AND BACKUPSET_INNER.BACKUP_START_DATE <= @RESTORE_TO_DATETIME
);
  
/**************************************************************************************/

/* LOG BACKUPS THAT OCCURRED AFTER THE DIFFERENT ARE ALWAYS PART OF THE RESTORE PLAN. */

/**************************************************************************************/

UPDATE #BACKUPSET
  SET
      IS_IN_RESTORE_PLAN = 1
WHERE #BACKUPSET.TYPE = 'L'
      AND #BACKUPSET.BACKUP_START_DATE <= @RESTORE_TO_DATETIME
      AND #BACKUPSET.BACKUP_START_DATE >=
(
    SELECT BACKUPSET_INNER.BACKUP_START_DATE
    FROM #BACKUPSET BACKUPSET_INNER
    WHERE BACKUPSET_INNER.TYPE = 'I'
          AND BACKUPSET_INNER.IS_IN_RESTORE_PLAN = 1
);
                                           
/**************************************************************************************/

/* IF @RESTORE_TO_DATETIME IS GREATER THAN THE LAST STARTDATE OF THE LAST LOG BACKUP, */

/* INCLUDE THE NEXT LOG BACKUP IN THE RESTORE PLAN */

/**************************************************************************************/

UPDATE #BACKUPSET
  SET
      IS_IN_RESTORE_PLAN = 1
WHERE #BACKUPSET.TYPE = 'L'
      AND #BACKUPSET.BACKUP_START_DATE =
(
    SELECT MIN(BACKUPSET_INNER.BACKUP_START_DATE)
    FROM #BACKUPSET BACKUPSET_INNER
    WHERE BACKUPSET_INNER.TYPE = 'L'
          AND BACKUPSET_INNER.BACKUP_START_DATE > @RESTORE_TO_DATETIME
          AND BACKUPSET_INNER.IS_IN_RESTORE_PLAN = 0
);
                                           
/**************************************************************************************/

/* IF THERE ARE NO DIFFERENTIAL BACKUPS, ALL LOG BACKUPS THAT OCCURRED AFTER THE FULL */

/* BACKUP ARE NEEDED IN THE RESTORE PLAN. */

/**************************************************************************************/

UPDATE #BACKUPSET
  SET
      IS_IN_RESTORE_PLAN = 1
WHERE #BACKUPSET.TYPE = 'L'
      AND #BACKUPSET.BACKUP_START_DATE <= @RESTORE_TO_DATETIME
      AND NOT EXISTS
(
    SELECT *
    FROM #BACKUPSET BACKUPSET_INNER
    WHERE BACKUPSET_INNER.TYPE = 'I'
);
SELECT *
INTO ##RestoreDBPrint
FROM
(
    SELECT TOP 100 PERCENT BKPS.NAME AS [NAME],
                           BKPS.BACKUP_SET_ID AS [ID],
                           BTMP.IS_IN_RESTORE_PLAN AS [ISINRESTOREPLAN],
                           BKPS.BACKUP_SET_UUID AS [BACKUPSETUUID],
                           BKPS.MEDIA_SET_ID AS [MEDIASETID],
                           BKPS.FIRST_FAMILY_NUMBER AS [FIRSTFAMILYNUMBER],
                           BKPS.FIRST_MEDIA_NUMBER AS [FIRSTMEDIANUMBER],
                           BKPS.LAST_FAMILY_NUMBER AS [LASTFAMILYNUMBER],
                           BKPS.LAST_MEDIA_NUMBER AS [LASTMEDIANUMBER],
                           BKPS.CATALOG_FAMILY_NUMBER AS [CATALOGFAMILYNUMBER],
                           BKPS.CATALOG_MEDIA_NUMBER AS [CATALOGMEDIANUMBER],
                           BKPS.POSITION AS [POSITION],
                           BKPS.EXPIRATION_DATE AS [EXPIRATIONDATE],
                           BKPS.SOFTWARE_VENDOR_ID AS [SOFTWAREVENDORID],
                           BKPS.DESCRIPTION AS [DESCRIPTION],
                           BKPS.USER_NAME AS [USERNAME],
                           BKPS.SOFTWARE_MAJOR_VERSION AS [SOFTWAREMAJORVERSION],
                           BKPS.SOFTWARE_MINOR_VERSION AS [SOFTWAREMINORVERSION],
                           BKPS.SOFTWARE_BUILD_VERSION AS [SOFTWAREBUILDVERSION],
                           BKPS.TIME_ZONE AS [TIMEZONE],
                           BKPS.MTF_MINOR_VERSION AS [MTFMINORVERSION],
                           BKPS.FIRST_LSN AS [FIRSTLSN],
                           BKPS.LAST_LSN AS [LASTLSN],
                           BKPS.CHECKPOINT_LSN AS [CHECKPOINTLSN],
                           BKPS.DATABASE_BACKUP_LSN AS [DATABASEBACKUPLSN],
                           BKPS.DATABASE_CREATION_DATE AS [DATABASECREATIONDATE],
                           BKPS.BACKUP_START_DATE AS [BACKUPSTARTDATE],
                           BKPS.BACKUP_FINISH_DATE AS [BACKUPFINISHDATE],
                           BKPS.TYPE AS [TYPE],
                           BKPS.SORT_ORDER AS [SORTORDER],
                           BKPS.CODE_PAGE AS [CODEPAGE],
                           BKPS.COMPATIBILITY_LEVEL AS [COMPATIBILITYLEVEL],
                           BKPS.DATABASE_VERSION AS [DATABASEVERSION],
                           BKPS.BACKUP_SIZE AS [BACKUPSIZE],
                           BKPS.DATABASE_NAME AS [DATABASENAME],
                           BKPS.SERVER_NAME AS [SERVERNAME],
                           BKPS.MACHINE_NAME AS [MACHINENAME],
                           BKPS.FLAGS AS [FLAGS],
                           BKPS.UNICODE_LOCALE AS [UNICODELOCALE],
                           BKPS.UNICODE_COMPARE_STYLE AS [UNICODECOMPARESTYLE],
                           BKPS.COLLATION_NAME AS [COLLATIONNAME],
                           BKPS.IS_COPY_ONLY AS [ISCOPYONLY]
    FROM #BACKUPSET AS BTMP
         INNER JOIN MSDB.DBO.BACKUPSET AS BKPS ON BKPS.BACKUP_SET_ID = BTMP.BACKUP_SET_ID
    ORDER BY [BACKUPFINISHDATE] ASC
) A;
SELECT CASE
           WHEN A.TYPE = 'D'
                OR A.TYPE = 'I'
           THEN 'RESTORE DATABASE '+A.DATABASENAME+' FROM DISK = N'''+B.PHYSICAL_DEVICE_NAME+''' WITH FILE = '+CAST(POSITION AS VARCHAR(10))+', NORECOVERY --'
           WHEN A.TYPE = 'L'
           THEN 'RESTORE LOG '+A.DATABASENAME+' FROM DISK = N'''+B.PHYSICAL_DEVICE_NAME+''' WITH FILE = '+CAST(POSITION AS VARCHAR(10))+', NORECOVERY--'
       END
FROM ##RestoreDBPrint A
     INNER JOIN MSDB.DBO.BACKUPMEDIAFAMILY B ON A.MEDIASETID = B.MEDIA_SET_ID
WHERE ISINRESTOREPLAN = 1
UNION ALL
SELECT 'RESTORE DATABASE '+@DB_NAME+' WITH RECOVERY'
FROM sys.databases
WHERE name = @DB_NAME;
GO
DROP TABLE #BACKUPSET;
DROP TABLE ##RestoreDBPrint;
