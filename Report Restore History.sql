USE msdb;
SELECT @@SERVERNAME CurrentServerName,
       DBRestored = destination_database_name,
       RestoreDate = restore_date,
       SourceDB = b.database_name,
	RestoredDBBackupFileLocation = bmf.physical_device_name,
       SourceFile = physical_name,
       BackupDate = backup_start_date,
       SourceServer = server_name,
       CONVERT(DECIMAL(18, 2), b.backup_size / 1024.0 / 1024.0) BackupSize,
       h.user_name RestoredBy
FROM msdb..restorehistory h
     INNER JOIN msdb..backupset b ON h.backup_set_id = b.backup_set_id
     INNER JOIN msdb..backupfile f ON f.backup_set_id = b.backup_set_id
     INNER JOIN MSDB..backupmediafamily bmf ON bmf.media_set_id = b.media_set_id
WHERE 1 = 1
      --AND restore_date > DATEADD(DD, -30, GETDATE())
      --AND destination_database_name LIKE 'DBName'
      --AND h.user_name LIKE 'domain\username'
ORDER BY RestoreDate DESC;
