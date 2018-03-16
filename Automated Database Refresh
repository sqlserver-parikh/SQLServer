--Pre Check
--1. Need Ola Solution for all servers in master database - Source & Destination -- https://ola.hallengren.com
--2. Need sp_DatabaseRestore.sql from First Responder Kit in destination server in master database -- https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/dev/sp_DatabaseRestore.sql
--If Service account don't have access to take backup and copy files from source to destination you have to use proxy account and for that I have created proxy account for powershell to utilize my own domain account when I am running this job. Please modify line 32 with proper account.
--I am scripting out database permission of all databases so if you have too many databases on server where you want to refresh it will take some time to script out permissions.
--If you have Ola Scripts and sp_DatabaseRestore procedure in any user database than just find and replace master.. with UserDB..
/*
Please insert records in RefreshDBs table with ServerName, DatabaseName, and FreshBackup (1 - Take fresh backup or 0 - Utilize last full backup)
INSERT INTO master..RefreshDBs
       SELECT 'ServerName\InstanceName', 'DBName', 1;
*/
USE master
GO

IF OBJECT_ID('master..RefreshDBs', 'U') IS NOT NULL
DROP TABLE master..RefreshDBs
GO
CREATE TABLE master..[RefreshDBs](
 [SourceServer] [sysname] NOT NULL,
 [DBName] [sysname] NOT NULL,
 [FreshBackup] bit NOT NULL --Yes or No
) ON [PRIMARY]

GO

USE [master]
GO
IF EXISTS (SELECT 1 FROM sys.credentials WHERE name LIKE 'RefreshDBs')
DROP CREDENTIAL [RefreshDBs]
GO
--Please modify below with your domain account and password
CREATE CREDENTIAL [RefreshDBs] WITH IDENTITY = N'domain\parikh', SECRET = N'TopSecretPassword1!'
GO

USE [master]
GO
IF EXISTS (SELECT 1 FROM msdb..sysproxies WHERE name LIKE 'RefreshDBs')
BEGIN
EXEC msdb.dbo.sp_reassign_proxy @current_proxy_name=N'RefreshDBs', @target_proxy_name=N''

EXEC msdb.dbo.sp_delete_proxy @proxy_name=N'RefreshDBs'
END
GO
EXEC msdb.dbo.sp_add_proxy @proxy_name=N'RefreshDBs',@credential_name=N'RefreshDBs',
  @enabled=1
GO
EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name=N'RefreshDBs', @subsystem_id=12
GO


USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 10/12/2017 4:05:50 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
IF EXISTS (SELECT 1 FROM msdb..sysjobs WHERE name LIKE N'Automated Database Refresh')
BEGIN
EXEC msdb.dbo.sp_delete_job @job_name = N'Automated Database Refresh',  @delete_unused_schedule=1
END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Automated Database Refresh',
  @enabled=1,
  @notify_level_eventlog=0,
  @notify_level_email=0,
  @notify_level_netsend=0,
  @notify_level_page=0,
  @delete_level=0,
  @description=N'No description available.',
  @category_name=N'Database Maintenance',
  @owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Script Permission]    Script Date: 10/12/2017 4:05:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Script Permission',
  @step_id=1,
  @cmdexec_success_code=0,
  @on_success_action=3,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'PowerShell',
  @command=N'# $databases grabs list of production databases from the SQL_DATABASES table on your Database
$PermissionsFolder = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database master -Query "DECLARE @DefaultBackupDirectory VARCHAR(200);
DECLARE @DBPermissions VARCHAR(300);
DECLARE @DirTree TABLE
       (
       subdirectory NVARCHAR(255),
       depth        INT
       );
EXECUTE master.dbo.xp_instance_regread N''HKEY_LOCAL_MACHINE'', N''SOFTWARE\Microsoft\MSSQLServer\MSSQLServer'', N''BackupDirectory'', @DefaultBackupDirectory OUTPUT;
SET @DBPermissions = @DefaultBackupDirectory+''\DBPermissions'';

INSERT INTO @DirTree
    (subdirectory
       , depth
    )
EXEC master.sys.xp_dirtree @DefaultBackupDirectory;
IF NOT EXISTS
     (
      SELECT 1
      FROM @DirTree
      WHERE subdirectory = ''DBPermissions''
    AND depth = 1
     )
    EXEC master.dbo.xp_create_subdir @DBPermissions;
 SELECT @DBPermissions DBPermissions;"

$databases = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database master -Query "select sd.name from master.sys.databases sd
            WHERE HAS_DBACCESS(sd.[name]) = 1
                  AND sd.[is_read_only] = 0
                  AND sd.[state_desc] = ''ONLINE''
                  AND sd.[user_access_desc] = ''MULTI_USER''
                  AND sd.[is_in_standby] = 0;"


foreach ($database in $databases) #for each separate server / database pair in $databases
{
# This lets us pick out each instance ($inst) and database ($name) as we iterate through each pair of server / database.
#$Inst = $database.INSTANCE #instance from the select query
$DBname = $database.name #databasename from the select query


#generate the output file name for each server/database pair
$filepath = $PermissionsFolder.DBPermissions +"\"
$filename =  $DBname +"_DBPermissions.sql"

# This line can be used if there are named instances in your environment.
#$filename = $filename.Replace("\","$") # Replaces all "\" with "$" so that instance name can be used in file names.

$outfile = ($filepath + $filename) #create out-file file name


#connect to each instance\database and generate security script and output to files
invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database ${DBname} -Query "
DECLARE
    @sql VARCHAR(2048)
    ,@sort INT

DECLARE tmp CURSOR FOR


/*********************************************/
/*********   DB CONTEXT STATEMENT    *********/
/*********************************************/
SELECT ''-- [-- DB CONTEXT --] --'' AS [-- SQL STATEMENTS --],
        1 AS [-- RESULT ORDER HOLDER --]
UNION
SELECT  ''USE'' + '' '' + QUOTENAME(DB_NAME()) AS [-- SQL STATEMENTS --],
        1 AS [-- RESULT ORDER HOLDER --]

UNION

SELECT '''' AS [-- SQL STATEMENTS --],
        2 AS [-- RESULT ORDER HOLDER --]

UNION

/*********************************************/
/*********     DB USER CREATION      *********/
/*********************************************/

SELECT ''-- [-- DB USERS --] --'' AS [-- SQL STATEMENTS --],
        3 AS [-- RESULT ORDER HOLDER --]
UNION
SELECT  ''IF NOT EXISTS (SELECT [name] FROM sys.database_principals WHERE [name] = '' + '' '' + '''''''' + [name] + '''''''' + '') BEGIN CREATE USER '' + '' '' + QUOTENAME([name]) + '' FOR LOGIN '' + QUOTENAME([name]) + '' WITH DEFAULT_SCHEMA = '' + QUOTENAME([default_schema_name]) + '' '' + ''END; '' AS [-- SQL STATEMENTS --],
        4 AS [-- RESULT ORDER HOLDER --]
FROM    sys.database_principals AS rm
WHERE [type] IN (''U'', ''S'', ''G'') -- windows users, sql users, windows groups

UNION

/*********************************************/
/*********    DB ROLE PERMISSIONS    *********/
/*********************************************/
SELECT ''-- [-- DB ROLES --] --'' AS [-- SQL STATEMENTS --],
        5 AS [-- RESULT ORDER HOLDER --]
UNION
SELECT  ''EXEC sp_addrolemember @rolename =''
    + '' '' + QUOTENAME(USER_NAME(rm.role_principal_id), '''''''') + '', @membername ='' + '' '' + QUOTENAME(USER_NAME(rm.member_principal_id), '''''''') AS [-- SQL STATEMENTS --],
        6 AS [-- RESULT ORDER HOLDER --]
FROM    sys.database_role_members AS rm
WHERE   USER_NAME(rm.member_principal_id) IN (
                                                --get user names on the database
                                                SELECT [name]
                                                FROM sys.database_principals
                                                WHERE [principal_id] > 4 -- 0 to 4 are system users/schemas
                                                and [type] IN (''G'', ''S'', ''U'') -- S = SQL user, U = Windows user, G = Windows group
                                              )
--ORDER BY rm.role_principal_id ASC


UNION

SELECT '''' AS [-- SQL STATEMENTS --],
        7 AS [-- RESULT ORDER HOLDER --]

UNION

/*********************************************/
/*********  OBJECT LEVEL PERMISSIONS *********/
/*********************************************/
SELECT ''-- [-- OBJECT LEVEL PERMISSIONS --] --'' AS [-- SQL STATEMENTS --],
        8 AS [-- RESULT ORDER HOLDER --]
UNION
SELECT  CASE
            WHEN perm.state <> ''W'' THEN perm.state_desc
            ELSE ''GRANT''
        END
        + '' '' + perm.permission_name + '' '' + ''ON '' + QUOTENAME(SCHEMA_NAME(obj.schema_id)) + ''.'' + QUOTENAME(obj.name) --select, execute, etc on specific objects
        + CASE
                WHEN cl.column_id IS NULL THEN SPACE(0)
                ELSE ''('' + QUOTENAME(cl.name) + '')''
          END
        + '' '' + ''TO'' + '' '' + QUOTENAME(USER_NAME(usr.principal_id)) COLLATE database_default
        + CASE
                WHEN perm.state <> ''W'' THEN SPACE(0)
                ELSE '' '' + ''WITH GRANT OPTION''
          END
            AS [-- SQL STATEMENTS --],
        9 AS [-- RESULT ORDER HOLDER --]
FROM
    sys.database_permissions AS perm
        INNER JOIN
    sys.objects AS obj
            ON perm.major_id = obj.[object_id]
        INNER JOIN
    sys.database_principals AS usr
            ON perm.grantee_principal_id = usr.principal_id
        LEFT JOIN
    sys.columns AS cl
            ON cl.column_id = perm.minor_id AND cl.[object_id] = perm.major_id
--WHERE usr.name = @OldUser
--ORDER BY perm.permission_name ASC, perm.state_desc ASC



UNION

SELECT '''' AS [-- SQL STATEMENTS --],
    10 AS [-- RESULT ORDER HOLDER --]

UNION

/*********************************************/
/*********    DB LEVEL PERMISSIONS   *********/
/*********************************************/
SELECT ''-- [--DB LEVEL PERMISSIONS --] --'' AS [-- SQL STATEMENTS --],
        11 AS [-- RESULT ORDER HOLDER --]
UNION
SELECT  CASE
            WHEN perm.state <> ''W'' THEN perm.state_desc --W=Grant With Grant Option
            ELSE ''GRANT''
        END
    + '' '' + perm.permission_name --CONNECT, etc
    + '' '' + ''TO'' + '' '' + ''['' + USER_NAME(usr.principal_id) + '']'' COLLATE database_default --TO <user name>
    + CASE
            WHEN perm.state <> ''W'' THEN SPACE(0)
            ELSE '' '' + ''WITH GRANT OPTION''
      END
        AS [-- SQL STATEMENTS --],
        12 AS [-- RESULT ORDER HOLDER --]
FROM    sys.database_permissions AS perm
    INNER JOIN
    sys.database_principals AS usr
    ON perm.grantee_principal_id = usr.principal_id
--WHERE usr.name = @OldUser

WHERE   [perm].[major_id] = 0
    AND [usr].[principal_id] > 4 -- 0 to 4 are system users/schemas
    AND [usr].[type] IN (''G'', ''S'', ''U'') -- S = SQL user, U = Windows user, G = Windows group

UNION

SELECT '''' AS [-- SQL STATEMENTS --],
        13 AS [-- RESULT ORDER HOLDER --]

UNION

SELECT ''-- [--DB LEVEL SCHEMA PERMISSIONS --] --'' AS [-- SQL STATEMENTS --],
        14 AS [-- RESULT ORDER HOLDER --]
UNION
SELECT  CASE
            WHEN perm.state <> ''W'' THEN perm.state_desc --W=Grant With Grant Option
            ELSE ''GRANT''
            END
                + '' '' + perm.permission_name --CONNECT, etc
                + '' '' + ''ON'' + '' '' + class_desc + ''::'' COLLATE database_default --TO <user name>
                + QUOTENAME(SCHEMA_NAME(major_id))
                + '' '' + ''TO'' + '' '' + QUOTENAME(USER_NAME(grantee_principal_id)) COLLATE database_default
                + CASE
                    WHEN perm.state <> ''W'' THEN SPACE(0)
                    ELSE '' '' + ''WITH GRANT OPTION''
                    END
            AS [-- SQL STATEMENTS --],
        15 AS [-- RESULT ORDER HOLDER --]
from sys.database_permissions AS perm
    inner join sys.schemas s
        on perm.major_id = s.schema_id
    inner join sys.database_principals dbprin
        on perm.grantee_principal_id = dbprin.principal_id
WHERE class = 3 --class 3 = schema


ORDER BY [-- RESULT ORDER HOLDER --]


OPEN tmp
FETCH NEXT FROM tmp INTO @sql, @sort
WHILE @@FETCH_STATUS = 0
BEGIN
        SELECT  @sql
        FETCH NEXT FROM tmp INTO @sql, @sort
END

CLOSE tmp
DEALLOCATE tmp" | Format-Table -HideTableHeaders | out-file -width 260 -filepath ($outfile)

} #end foreach loop

',
  @database_name=N'master',
  @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Backup Databases]    Script Date: 10/12/2017 4:05:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Backup Databases',
  @step_id=2,
  @cmdexec_success_code=0,
  @on_success_action=3,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'PowerShell',
  @command=N'$databases = invoke-sqlcmd -ServerInstance  $(ESCAPE_SQUOTE(SRVR)) -Database master -Query "SELECT SourceServer, DBName, FreshBackup from master..RefreshDBs;"

$databases

foreach ($database in $databases) #for each separate server / database pair in $databases
{
# This lets us pick out each instance ($inst) and database ($name) as we iterate through each pair of server / database.
$Inst = $database.SourceServer #instance from the select query
$DBname = $database.DBName #databasename from the select query
$FBackup = $database.FreshBackup

if ( $FBackup -match "True")
{
sqlcmd -E  -S $Inst -d master -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = $DBName, @Directory = NULL, @BackupType = ''FULL'', @Compress = ''Y'', @Verify = ''Y'', @CleanupTime = 48, @CheckSum = ''Y'', @LogToTable = ''Y''"
}

$Inst
$BackupLoation =  invoke-sqlcmd -ServerInstance $Inst  -Database master -Query "
DECLARE @BPath varchar(900);
SELECT TOP 1 @BPath = physical_device_name
FROM msdb.dbo.backupset b
     JOIN msdb.dbo.backupmediafamily m ON b.media_set_id = m.media_set_id
       WHERE database_name like ''$DBName''

             AND b.type = ''D''
       ORDER BY backup_finish_date DESC;

IF @BPath LIKE ''_:\%''
SET @BPath = ''\\''+CONVERT(VARCHAR(128), SERVERPROPERTY(''ComputerNamePhysicalNetBIOS''))+''\''+REPLACE(@BPath, '':'', ''$'');
ELSE IF @BPath like ''\\%''
SET @BPath = @BPath
SELECT LEFT(@BPath, LEN(@BPath)-CHARINDEX(''\'', REVERSE(@BPath))) BackupFolder,
       REVERSE(LEFT(REVERSE(@BPath), CHARINDEX(''\'', REVERSE(@BPath))-1)) BackupFileName;
"

$CopyFolder = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database master -Query "DECLARE @DefaultBackupDirectory VARCHAR(200);
DECLARE @DBBackup VARCHAR(300);
DECLARE @DirTree TABLE
       (
       subdirectory NVARCHAR(255),
       depth        INT
       );
EXECUTE master.dbo.xp_instance_regread N''HKEY_LOCAL_MACHINE'', N''SOFTWARE\Microsoft\MSSQLServer\MSSQLServer'', N''BackupDirectory'', @DefaultBackupDirectory OUTPUT;
SET @DBBackup = @DefaultBackupDirectory;
IF @DBBackup LIKE ''_:\%''
SET @DBBackup = ''\\''+CONVERT(VARCHAR(128), SERVERPROPERTY(''ComputerNamePhysicalNetBIOS''))+''\''+REPLACE(@DBBackup, '':'', ''$'');
ELSE IF @DBBackup like ''\\%''
SET @DBBackup = @DBBackup
SELECT @DBBackup DBBackupFile;"
$BackupLoation.BackupFolder
$BackupLoation.BackupFileName
$CopyFolder.DBBackupFile
#copy-item -Path "$BackupLocation.BackupPath" -Destination "$CopyFolder.DBBackupFile"
Robocopy $BackupLoation.BackupFolder $CopyFolder.DBBackupFile $BackupLoation.BackupFileName
}',
  @database_name=N'master',
  @flags=0,
  @proxy_name=N'RefreshDBs'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Restore Databases]    Script Date: 10/12/2017 4:05:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Restore Databases',
  @step_id=3,
  @cmdexec_success_code=0,
  @on_success_action=3,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'TSQL',
  @command= N'DECLARE @name SYSNAME, @defaultdatapath VARCHAR(1024), @defaultlogpath VARCHAR(1024), @DefaultBackupDirectory VARCHAR(1024);
SET @defaultdatapath = CONVERT(VARCHAR(1024), SERVERPROPERTY(''InstanceDefaultDataPath''));
SET @defaultlogpath = CONVERT(VARCHAR(1024), SERVERPROPERTY(''InstanceDefaultLogPath''));
EXECUTE master.dbo.xp_instance_regread
        N''HKEY_LOCAL_MACHINE'',
        N''SOFTWARE\Microsoft\MSSQLServer\MSSQLServer'',
        N''BackupDirectory'',
        @DefaultBackupDirectory OUTPUT;
DECLARE db_cursor CURSOR
FOR
    SELECT DBName
    FROM master..RefreshDBs;
OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @name;
WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @sql VARCHAR(255);
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @name)
  BEGIN
   SET @sql = ''ALTER DATABASE ''+@name+'' SET SINGLE_USER WITH ROLLBACK IMMEDIATE'';
   EXEC (@sql);
   WAITFOR DELAY ''00:00:15'';
   SET @sql = ''ALTER DATABASE ''+@name+'' SET MULTI_USER'';
   EXEC (@sql);
  END
        EXEC master..sp_DatabaseRestore
             @Database = @name,
             @BackupPathFull = @DefaultBackupDirectory,
             --@MoveDataDrive = @defaultdatapath,
             --@MoveLogDrive = @defaultlogpath,
             --@MoveFiles = 1,
             @RunRecovery = 1;
        FETCH NEXT FROM db_cursor INTO @name;
    END;
CLOSE db_cursor;
DEALLOCATE db_cursor;
GO
',
  @database_name=N'master',
  @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Apply Permissions]    Script Date: 10/12/2017 4:05:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Apply Permissions',
  @step_id=4,
  @cmdexec_success_code=0,
  @on_success_action=3,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'PowerShell',
  @command=N'# $databases grabs list of production databases from the SQL_DATABASES table on your Database
$PermissionsFolder = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database master -Query "DECLARE @DefaultBackupDirectory VARCHAR(200);
DECLARE @DBPermissions VARCHAR(300);
DECLARE @DirTree TABLE
       (
       subdirectory NVARCHAR(255),
       depth        INT
       );
EXECUTE master.dbo.xp_instance_regread N''HKEY_LOCAL_MACHINE'', N''SOFTWARE\Microsoft\MSSQLServer\MSSQLServer'', N''BackupDirectory'', @DefaultBackupDirectory OUTPUT;
SET @DBPermissions = @DefaultBackupDirectory+''\DBPermissions'';

INSERT INTO @DirTree
    (subdirectory
       , depth
    )
EXEC master.sys.xp_dirtree @DefaultBackupDirectory;
IF NOT EXISTS
     (
      SELECT 1
      FROM @DirTree
      WHERE subdirectory = ''DBPermissions''
    AND depth = 1
     )
    EXEC master.dbo.xp_create_subdir @DBPermissions;
 SELECT @DBPermissions DBPermissions;"

$databases = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR)) -Database master -Query "SELECT DBName FROM master..RefreshDBs;"

foreach ($db in $databases) #for each separate server / database pair in $databases
{
# This lets us pick out each instance ($inst) and database ($name) as we iterate through each pair of server / database.
#$Inst = $database.INSTANCE #instance from the select query
 $DBList = $db.DBName

$DBPermissionScript = $PermissionsFolder.DBPermissions +"\" + $DBList +"_DBPermissions.sql"
$DBPermissionScript
if (Test-Path $DBPermissionScript) {
invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR)) -Database master -InputFIle $DBPermissionScript
}
}',
  @database_name=N'master',
  @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Fix Orphan User]    Script Date: 10/12/2017 4:05:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Fix Orphan User',
  @step_id=5,
  @cmdexec_success_code=0,
  @on_success_action=1,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'TSQL',
  @command=N'--Below script will loop through all database and generate script to map user and drop user.

SET NOCOUNT ON;
DECLARE @userid VARCHAR(255);
DECLARE @dbname VARCHAR(128);
DECLARE @script NVARCHAR(MAX);
CREATE TABLE #OrphanUsers
(DBName   VARCHAR(128),
 UserName VARCHAR(128),
 UserSID  NVARCHAR(255)
);
INSERT INTO #OrphanUsers
EXEC sp_MSforeachdb
     ''select "?" DBName,name, sid from [?]..sysusers
            where issqluser = 1
            and   (sid is not null and sid <> 0x0)
            and   (len(sid) <= 16)
            and   suser_sname(sid) is null
            order by name'';
DECLARE FixUser CURSOR
FOR SELECT OU.UserName,
           OU.DBName
    FROM #OrphanUsers OU INNER JOIN master..RefreshDBs RD on OU.DBName = RD.DBName ;
OPEN FixUser;
FETCH NEXT FROM FixUser INTO @userid, @DBName;
WHILE @@FETCH_STATUS = 0
    IF EXISTS
              (
              SELECT 1
              FROM sys.server_principals
                     WHERE name = @userid
              )
        BEGIN
            SET @script = ''USE ''+QUOTENAME(@dbname)+'';''+CHAR(10)+''EXECUTE sp_change_users_login ''''UPDATE_ONE'''', ''''''+@userid+'''''', ''''''+@userid+'''''''';
            EXEC sp_executesql
                 @script;
            PRINT @script;
            FETCH NEXT FROM FixUser INTO @userid, @DBName;
        END;
    ELSE
        BEGIN
            IF EXISTS
                      (
                      SELECT name
                      FROM sys.schemas
                             WHERE principal_id = USER_ID(@userid)
                      )
                BEGIN
                    SET @script = ''USE ''+QUOTENAME(@dbname)+'';''+CHAR(10)+''DROP USER ''+QUOTENAME(@userid)+'';''+CHAR(10);
                   EXEC sp_executesql @script;
                    PRINT @script;
                END;
            FETCH NEXT FROM FixUser INTO @userid, @DBName;
        END;
CLOSE FixUser;
DEALLOCATE FixUser;
DROP TABLE #OrphanUsers;',
  @database_name=N'master',
  @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO
