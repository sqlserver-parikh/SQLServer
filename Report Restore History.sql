USE msdb;
GO
CREATE OR ALTER PROCEDURE #GetRestoreHistory
    @RestoreDateFrom DATETIME = NULL,
    @DestinationDBName NVARCHAR(128) = NULL,
    @UserName NVARCHAR(128) = NULL,
    @BackupType CHAR(1) = 'D',
    @BackupSize DECIMAL(18, 2) = 0
AS
BEGIN
    SELECT DISTINCT 
           CurrentServerName = @@SERVERNAME,
           DBRestored = destination_database_name,
           RestoreDate = restore_date,
           SourceDB = b.database_name,
           RestoredDBBackupFileLocation = bmf.physical_device_name,
           --SourceFile = physical_name,
           BackupDate = backup_start_date,
           SourceServer = server_name,
           BackupSize = CONVERT(DECIMAL(18, 2), b.backup_size / 1024.0 / 1024.0),
           RestoredBy = h.user_name,
           BackupType = b.type,
           TotalDatabaseSizeMB = CONVERT(DECIMAL (20,2),SUM(f.file_size) OVER (PARTITION BY destination_database_name, restore_date) / 1024.0 / 1024.0),
		   StoppedAt = h.stop_at
    FROM msdb..restorehistory h
         INNER JOIN msdb..backupset b ON h.backup_set_id = b.backup_set_id
         INNER JOIN msdb..backupfile f ON f.backup_set_id = b.backup_set_id
         INNER JOIN msdb..backupmediafamily bmf ON bmf.media_set_id = b.media_set_id
    WHERE (@RestoreDateFrom IS NULL OR restore_date > @RestoreDateFrom)
          AND (@DestinationDBName IS NULL OR destination_database_name LIKE '%' + @DestinationDBName + '%')
          AND (@UserName IS NULL OR h.user_name LIKE @UserName)
          AND (@BackupType IS NULL OR b.type = @BackupType)
          AND (b.backup_size / 1024.0 / 1024.0 > @BackupSize)
    ORDER BY RestoreDate DESC;
END
GO
#GetRestoreHistory
GO
DROP PROCEDURE #GetRestoreHistory
