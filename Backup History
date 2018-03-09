SELECT DISTINCT
       server_name AS ServerName,
       T1.name,
       T3.backup_start_date AS Bkp_StartDate,
       T3.backup_finish_date AS Bkp_FinishDate,
	     DATEDIFF(SS,T3.backup_start_date, T3.backup_finish_date) Bkp_Time_Sec,
       T3.type Bkp_Type,
       (T3.backup_size / 1048576.0) AS BackupSizeMB,
       (T3.compressed_backup_size / 1048576.0) AS CompressedBackupSizeMB,
       (CAST((T3.backup_size / 1048576.0) / (DATEDIFF(SS, T3.backup_start_date, T3.backup_finish_date) + 1) AS DECIMAL(10, 2))) AS MBPS,
       user_name AS UserName,
       physical_device_name AS BackupLocation
FROM master..sysdatabases AS T1
     LEFT JOIN msdb..backupset AS T3 ON(T3.database_name = T1.name)
     LEFT JOIN msdb..backupmediaset AS T5 ON(T3.media_set_id = T5.media_set_id)
     LEFT JOIN msdb..backupmediafamily AS T6 ON(T6.media_set_id = T5.media_set_id)
WHERE 1 = 1
   --AND T3.type = 'D'
	 --AND T1.name LIKE 'DBName'
	   AND T3.backup_finish_date > DATEADD(DD,-30,GETDATE())
     AND DATABASEPROPERTYEX(T1.name, 'STATUS') = 'ONLINE'
     AND T1.name <> 'tempdb'
ORDER BY 3 DESC,
         4 DESC;
