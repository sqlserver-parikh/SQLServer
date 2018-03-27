--Below job will script out all database permissions and save output to default backup folder and it will create new folder there DBPermissions if not exists.
--https://sqljgood.wordpress.com/2014/09/17/using-powershell-to-loop-through-a-list-of-sql-server-databases/comment-page-1/

USE [msdb]
GO
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Script DB Permissions')
BEGIN
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Script DB Permissions', @delete_unused_schedule=1
END
GO
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Script DB Permissions',
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

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Script Permission',
  @step_id=1,
  @cmdexec_success_code=0,
  @on_success_action=1,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'PowerShell',
  @command=N'# $databases grabs list of production databases from the SQL_DATABASES table on your Database
$PermissionsFolder = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database msdb -Query "DECLARE @DefaultBackupDirectory VARCHAR(200);
DECLARE @DBPermissions VARCHAR(300);
DECLARE @DirTree TABLE
       (
       subdirectory NVARCHAR(255),
       depth        INT
       );
EXECUTE master..xp_instance_regread N''HKEY_LOCAL_MACHINE'', N''SOFTWARE\Microsoft\MSSQLServer\MSSQLServer'', N''BackupDirectory'', @DefaultBackupDirectory OUTPUT;
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

$databases = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database msdb -Query "select name  FROM master.sys.databases sd
            WHERE HAS_DBACCESS(sd.[name]) = 1
                  AND sd.[is_read_only] = 0
                  AND sd.[state_desc] = ''ONLINE''
      AND sd.name not like ''ReportServer%''
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
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Weekly Sunday 9PM',
  @enabled=1,
  @freq_type=8,
  @freq_interval=1,
  @freq_subday_type=1,
  @freq_subday_interval=0,
  @freq_relative_interval=0,
  @freq_recurrence_factor=1,
  @active_start_date=20170622,
  @active_end_date=99991231,
  @active_start_time=210000,
  @active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO
