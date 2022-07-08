--If you run below script it will create script to create snapshot of database, restore of created snapshot and drop snapshot
--This will be useful if you are doing deployment and need to take snapshot backup of database
--Snapshot will be created on Backup drive
ALTER PROCEDURE spDBSnapshot
(
    @dbname sysname,
    @retainhours int = 24,
    @printonly bit = 0
)
as
DECLARE @sql NVARCHAR(max);
DECLARE @DefaultBackupDirectory VARCHAR(1024);
EXECUTE master..xp_instance_regread N'HKEY_LOCAL_MACHINE',
                                    N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer',
                                    N'BackupDirectory',
                                    @DefaultBackupDirectory OUTPUT;

SELECT @sql = 'DROP DATABASE ' + quotename(CONVERT(VARCHAR(128), name)) + ';'
FROM sys.databases
where source_database_id = DB_ID(@dbname)
      and create_date < DATEADD(HOUR, -@retainhours, GETDATE())

IF @sql IS NOT NULL
BEGIN
    IF @printonly = 0
    EXEC sp_executesql @sql;
	PRINT '--' + @sql
END

IF NOT EXISTS (SELECT name FROM sys.databases
where source_database_id = DB_ID(@dbname))
BEGIN
    SELECT @sql
        = 'CREATE DATABASE [' + @dbname + '_SNAPSHOT_' + CONVERT(VARCHAR(10), GETDATE(), 112) + replace(CONVERT(varchar(8),getdate(), 114),':','') + '] ON '
          + STUFF(
            (
                SELECT ', (NAME = ''' + name + ''', FILENAME = ''' + @DefaultBackupDirectory + '\' + name
                       + CONVERT(VARCHAR(10), GETDATE(), 112) + '.ss'')'
                FROM sys.master_files AS df
                WHERE df.type = 0
                      AND database_id = DB_ID(@dbname)
                FOR XML PATH('')
            ),
            1,
            1,
            ''
                 ) +
    (
        SELECT ' AS SNAPSHOT OF ' + @dbname
    )
    IF @printonly = 0
    BEGIN
        EXEC sp_executesql @sql;
    END
    PRINT '--' + @sql;
END

If @sql IS NOT NULL
BEGIN
select @sql
    = 'RESTORE DATABASE ' + QUOTENAME( @dbname ) + ' FROM DATABASE_SNAPSHOT = ' + QUOTENAME(name) 
	  from sys.databases where source_database_id = db_id( @dbname)

print '--' + @sql
--EXEC sp_executesql  @sql;

END

