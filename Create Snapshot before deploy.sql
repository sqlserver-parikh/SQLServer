--If you run below script it will create script to create snapshot of database, restore of created snapshot and drop snapshot
--This will be useful if you are doing deployment and need to take snapshot backup of database
--Snapshot will be created on Backup drive
DECLARE @sql NVARCHAR(max);
DECLARE @DefaultBackupDirectory VARCHAR(1024);
EXECUTE master..xp_instance_regread
        N'HKEY_LOCAL_MACHINE',
        N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer',
        N'BackupDirectory',
        @DefaultBackupDirectory OUTPUT;
SELECT @sql = 'CREATE DATABASE ['+DB_NAME()+'_SNAPSHOT_'+CONVERT( VARCHAR(10), GETDATE(), 112)+'] ON '+STUFF(
       (
       SELECT ', (NAME = '''+name+''', FILENAME = '''+@DefaultBackupDirectory+'\'+name+CONVERT( VARCHAR(10), GETDATE(), 112)+'.ss'')' FROM sys.database_files AS df
              WHERE df.type = 0
              FOR XML PATH('')
       ), 1, 1, '')+
       (
       SELECT ' AS SNAPSHOT OF '+QUOTENAME(DB_NAME())
       )
       WHERE DB_ID() > 4;

PRINT @sql;
--EXEC sp_executesql  @sql;
select @sql = 'RESTORE DATABASE ' + QUOTENAME(DB_NAME()) + ' FROM DATABASE_SNAPSHOT = '''+DB_NAME()+'_SNAPSHOT_'+CONVERT( VARCHAR(10), GETDATE(), 112)+'''' 

print @sql
--EXEC sp_executesql  @sql;

select @sql = 'DROP DATABASE ['+DB_NAME()+'_SNAPSHOT_'+CONVERT( VARCHAR(10), GETDATE(), 112)+']'

print @sql
--EXEC sp_executesql  @sql;

