--Below script will find all old backups on backup drive and you can delete those backup file by --copying DeleteFiles command

DECLARE @DaysToDelete int = 2;
DECLARE @DefaultBackupDirectory VARCHAR(200);
EXECUTE master..xp_instance_regread
        N'HKEY_LOCAL_MACHINE',
        N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer',
        N'BackupDirectory',
        @DefaultBackupDirectory OUTPUT;
SELECT @DefaultBackupDirectory = SUBSTRING(@DefaultBackupDirectory, 1, 3);
IF OBJECT_ID('tempdb..#DirectoryTree') IS NOT NULL
    DROP TABLE #DirectoryTree;
CREATE TABLE #DirectoryTree
(id           INT IDENTITY(1, 1),
 subdirectory NVARCHAR(512),
 depth        INT,
 isfile       BIT
);
INSERT INTO #DirectoryTree
(subdirectory,
 depth,
 isfile
)
EXEC master.sys.xp_dirtree
     @DefaultBackupDirectory,
     0,
     1;
SELECT CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server,
       'exec xp_delete_file 0,'''+a.physical_device_name+'''' DeleteFileCommand,
       b.database_name,
       b.backup_start_date,
       b.backup_finish_date,
       b.expiration_date,
       CASE b.type
           WHEN 'D'
           THEN 'Database'
           WHEN 'L'
           THEN 'Log'
       END AS backup_type,
       b.backup_size,
       a.logical_device_name,
       b.name AS backupset_name,
       b.description
FROM msdb..backupmediafamily a
     INNER JOIN msdb..backupset b ON a.media_set_id = b.media_set_id
     INNER JOIN #DirectoryTree C ON C.subdirectory = REVERSE(LEFT(REVERSE(A.physical_device_name), CHARINDEX('\', REVERSE(A.physical_device_name))-1))
WHERE(CONVERT(DATETIME, b.backup_start_date, 102) < GETDATE() - @DaysToDelete)
ORDER BY b.backup_finish_date;
