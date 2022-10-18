--http://dba.stackexchange.com/questions/64567/how-to-clone-a-user-in-sql-server-2008-r2
--- To copy permissions of one user/role to another user/role.

USE DBATasks; -- Use the database from which you want to extract the permissions
GO
SET NOCOUNT ON;
DECLARE @OldUser SYSNAME, @NewUser SYSNAME;
SET @OldUser = 'olduser'; --The user or role from which to copy the permissions from
SET @NewUser = 'userNEW';  --The user or role to which to copy the permissions to

SELECT 'USE'+SPACE(1)+QUOTENAME(DB_NAME()) AS '--Database Context';
SELECT '--Cloning permissions from'+SPACE(1)+QUOTENAME(@OldUser)+SPACE(1)+'to'+SPACE(1)+QUOTENAME(@NewUser) AS '--Comment';
SELECT 'EXEC sp_addrolemember @rolename ='+SPACE(1)+QUOTENAME(USER_NAME(rm.role_principal_id), '''')+', @membername ='+SPACE(1)+QUOTENAME(@NewUser, '''') AS '--Role Memberships'
FROM sys.database_role_members AS rm
WHERE USER_NAME(rm.member_principal_id) = @OldUser
ORDER BY rm.role_principal_id ASC;
SELECT CASE
           WHEN perm.state <> 'W'
           THEN perm.state_desc
           ELSE 'GRANT'
       END+SPACE(1)+perm.permission_name+SPACE(1)+'ON '+QUOTENAME(USER_NAME(obj.schema_id))+'.'+QUOTENAME(obj.name)+CASE
                                                                                                                        WHEN cl.column_id IS NULL
                                                                                                                        THEN SPACE(0)
                                                                                                                        ELSE '('+QUOTENAME(cl.name)+')'
                                                                                                                    END+SPACE(1)+'TO'+SPACE(1)+QUOTENAME(@NewUser) COLLATE database_default+CASE
                                                                                                                                                                                                WHEN perm.state <> 'W'
                                                                                                                                                                                                THEN SPACE(0)
                                                                                                                                                                                                ELSE SPACE(1)+'WITH GRANT OPTION'
                                                                                                                                                                                            END AS '--Object Level Permissions'
FROM sys.database_permissions AS perm
     INNER JOIN sys.objects AS obj ON perm.major_id = obj.[object_id]
     INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
     LEFT JOIN sys.columns AS cl ON cl.column_id = perm.minor_id
                                    AND cl.[object_id] = perm.major_id
WHERE usr.name = @OldUser
ORDER BY perm.permission_name ASC,
         perm.state_desc ASC;
SELECT CASE
           WHEN perm.state <> 'W'
           THEN perm.state_desc
           ELSE 'GRANT'
       END+SPACE(1)+perm.permission_name+SPACE(1)+SPACE(1)+'TO'+SPACE(1)+QUOTENAME(@NewUser) COLLATE database_default+CASE
                                                                                                                          WHEN perm.state <> 'W'
                                                                                                                          THEN SPACE(0)
                                                                                                                          ELSE SPACE(1)+'WITH GRANT OPTION'
                                                                                                                      END AS '--Database Level Permissions'
FROM sys.database_permissions AS perm
     INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
WHERE usr.name = @OldUser
      AND perm.major_id = 0
ORDER BY perm.permission_name ASC,
         perm.state_desc ASC;
         
/*

USE [master]
GO
--============================================
-- Author:      Pavel Pawlowski
-- Created:     2010/04/16
-- Description: Copies rights of old user to new user
--==================================================
CREATE OR ALTER PROCEDURE sp_CloneRights
(
    @oldUser sysname,            --Old user from which to copy right
    @newUser sysname,            --New user to which copy rights
    @printOnly bit = 1,          --When 1 then only script is printed on screen, when 0 then also script is executed, when NULL, script is only executed and not printed
    @NewLoginName sysname = null, --When a NewLogin name is provided also a creation of user is part of the final script
	@ServerLevel bit = 0
)
AS
BEGIN
    SET NOCOUNT ON

    CREATE TABLE #output (command nvarchar(4000))

    DECLARE @command nvarchar(4000),
            @sql nvarchar(max),
            @dbName nvarchar(128),
            @msg nvarchar(max)

    SELECT @sql = N'',
           @dbName = QUOTENAME(DB_NAME())
    SET NOCOUNT ON;
    IF OBJECT_ID(N'tempdb..#ServerLevelRoles') IS NOT NULL
        DROP TABLE #ServerLevelRoles;
    CREATE TABLE #ServerLevelRoles (ScriptToRun VARCHAR(1000));
    INSERT INTO #ServerLevelRoles
    SELECT 'EXEC sp_addsrvrolemember ''' + @newUser + ''',' + p.name + ';'
    FROM sys.server_role_members rm
        JOIN sys.server_principals p
            ON rm.role_principal_id = p.principal_id
        JOIN sys.server_principals m
            ON rm.member_principal_id = m.principal_id
    WHERE m.name = @oldUser;
    INSERT INTO #ServerLevelRoles
    SELECT CASE
               WHEN sp.state_desc = 'GRANT_WITH_GRANT_OPTION' THEN
                   SUBSTRING(state_desc, 0, 6) + ' ' + permission_name + ' to ' + QUOTENAME(@newUser)
                   + 'WITH GRANT OPTION;'
               ELSE
                   state_desc + ' ' + permission_name + ' to ' + QUOTENAME(SPs.name) + ';'
           END
    FROM sys.server_permissions SP
        JOIN sys.server_principals SPs
            ON sp.grantee_principal_id = SPs.principal_id
    WHERE SPs.name NOT LIKE '%##%' --and SPs.name not like '%nt %'
          AND SPs.name = @oldUser
          AND sp.type NOT IN ( 'COSQ', 'CO' );

    declare @permission varchar(1048)
    DECLARE SRoles CURSOR FOR
    SELECT ScriptToRun
    FROM #ServerLevelRoles
    WHERE ScriptToRun IS NOT NULL
          AND ScriptToRun NOT LIKE '%nt Service%'
          AND ScriptToRun NOT LIKE '%nt autho%';
    OPEN SRoles;
    FETCH NEXT FROM SRoles
    INTO @permission;
    WHILE @@FETCH_STATUS = 0
    BEGIN
		if @ServerLevel = 1
		begin
        print @permission;
		end
        FETCH NEXT FROM SRoles
        INTO @permission;
    END
    CLOSE SRoles;
    DEALLOCATE SRoles;

    DROP TABLE #ServerLevelRoles;


    IF (NOT EXISTS
    (
        SELECT 1
        FROM sys.database_principals
        where name = @oldUser
    )
       )
    BEGIN
        SET @msg = '--Source user ' + QUOTENAME(@oldUser) + ' doesn''t exists in database ' + @dbName
       -- RAISERROR(@msg, 11, 1)
	   PRINT @MSG
        RETURN
    END

    INSERT INTO #output
    (
        command
    )
    SELECT '--Database Context' AS command
    UNION ALL
    SELECT 'USE' + SPACE(1) + @dbName
    UNION ALL
    SELECT 'SET XACT_ABORT ON'

    IF (ISNULL(@NewLoginName, '') <> '')
    BEGIN
        SET @sql
            = N'USE ' + @dbName
              + N';
        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @newUser)
        BEGIN
            INSERT INTO #output(command)
            SELECT ''--Create user'' AS command
 
            INSERT INTO #output(command)
            SELECT
                ''CREATE USER '' + QUOTENAME(@NewUser) + '' FOR LOGIN '' + QUOTENAME(@NewLoginName) +
                    CASE WHEN ISNULL(default_schema_name, '''') <> '''' THEN '' WITH DEFAULT_SCHEMA = '' + QUOTENAME(dp.default_schema_name)
                        ELSE ''''
                    END AS Command
            FROM sys.database_principals dp
            INNER JOIN sys.server_principals sp ON dp.sid = sp.sid
            WHERE dp.name = @OldUser
        END'

        EXEC sp_executesql @sql,
                           N'@OldUser sysname, @NewUser sysname, @NewLoginName sysname',
                           @OldUser = @OldUser,
                           @NewUser = @NewUser,
                           @NewLoginName = @NewLoginName
    END

    INSERT INTO #output
    (
        command
    )
    SELECT '--Cloning permissions from' + SPACE(1) + QUOTENAME(@OldUser) + SPACE(1) + 'to' + SPACE(1)
           + QUOTENAME(@NewUser)

    INSERT INTO #output
    (
        command
    )
    SELECT '--Role Memberships' AS command

    SET @sql
        = N'USE ' + @dbName
          + N';
    INSERT INTO #output(command)
    SELECT ''EXEC sp_addrolemember @rolename =''
        + SPACE(1) + QUOTENAME(USER_NAME(rm.role_principal_id), '''''''') + '', @membername ='' + SPACE(1) + QUOTENAME(@NewUser, '''''''') AS command
    FROM    sys.database_role_members AS rm
    WHERE    USER_NAME(rm.member_principal_id) = @OldUser
    ORDER BY rm.role_principal_id ASC'

    EXEC sp_executesql @sql,
                       N'@OldUser sysname, @NewUser sysname',
                       @OldUser = @OldUser,
                       @NewUser = @NewUser

    INSERT INTO #output
    (
        command
    )
    SELECT '--Object Level Permissions'

    SET @sql
        = N'USE ' + @dbName
          + N';
    INSERT INTO #output(command)
    SELECT    CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END
        + SPACE(1) + perm.permission_name + SPACE(1) + ''ON '' + QUOTENAME(SCHEMA_NAME(obj.schema_id)) + ''.'' + QUOTENAME(obj.name)
        + CASE WHEN cl.column_id IS NULL THEN SPACE(0) ELSE ''('' + QUOTENAME(cl.name) + '')'' END
        + SPACE(1) + ''TO'' + SPACE(1) + QUOTENAME(@NewUser) COLLATE database_default
        + CASE WHEN perm.state <> ''W'' THEN SPACE(0) ELSE SPACE(1) + ''WITH GRANT OPTION'' END
    FROM    sys.database_permissions AS perm
        INNER JOIN
        sys.objects AS obj
        ON perm.major_id = obj.[object_id]
        INNER JOIN
        sys.database_principals AS usr
        ON perm.grantee_principal_id = usr.principal_id
        LEFT JOIN
        sys.columns AS cl
        ON cl.column_id = perm.minor_id AND cl.[object_id] = perm.major_id
    WHERE    usr.name = @OldUser
    ORDER BY perm.permission_name ASC, perm.state_desc ASC'

    EXEC sp_executesql @sql,
                       N'@OldUser sysname, @NewUser sysname',
                       @OldUser = @OldUser,
                       @NewUser = @NewUser

    INSERT INTO #output
    (
        command
    )
    SELECT N'--Database Level Permissions'

    SET @sql
        = N'USE ' + @dbName
          + N';
    INSERT INTO #output(command)
    SELECT    CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END
        + SPACE(1) + perm.permission_name + SPACE(1)
        + SPACE(1) + ''TO'' + SPACE(1) + QUOTENAME(@NewUser) COLLATE database_default
        + CASE WHEN perm.state <> ''W'' THEN SPACE(0) ELSE SPACE(1) + ''WITH GRANT OPTION'' END
    FROM    sys.database_permissions AS perm
        INNER JOIN
        sys.database_principals AS usr
        ON perm.grantee_principal_id = usr.principal_id
    WHERE    usr.name = @OldUser
    AND    perm.major_id = 0
    ORDER BY perm.permission_name ASC, perm.state_desc ASC'

    EXEC sp_executesql @sql,
                       N'@OldUser sysname, @NewUser sysname',
                       @OldUser = @OldUser,
                       @NewUser = @NewUser

    DECLARE cr CURSOR FOR SELECT command FROM #output

    OPEN cr

    FETCH NEXT FROM cr
    INTO @command

    SET @sql = ''

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF (@printOnly IS NOT NULL)
            PRINT @command

        SET @sql = @sql + @command + CHAR(13) + CHAR(10)
        FETCH NEXT FROM cr
        INTO @command
    END

    CLOSE cr
    DEALLOCATE cr

    IF (@printOnly IS NULL OR @printOnly = 0) EXEC (@sql)

    DROP TABLE #output
END
/*
declare @olduser varchar(128) = 'testing'
declare @newuser varchar(128) = 'NEWUSER'
declare @dbname varchar(128);
Declare DBs cursor for 
SELECT
  sd.[name] AS 'DBName'
FROM master.sys.databases sd
WHERE HAS_DBACCESS(sd.[name]) = 1
AND sd.[is_read_only] = 0
AND sd.[state_desc] = 'ONLINE'
AND sd.[user_access_desc] = 'MULTI_USER'
AND sd.[is_in_standby] = 0;
declare @command varchar(2048);
    OPEN dbs;
    FETCH NEXT FROM dbs
    INTO @dbname;
	    WHILE @@FETCH_STATUS = 0
    BEGIN
	set @command = 'USE ' + QUOTENAME(@DBNAME) + '; EXECUTE sp_CloneRights ''' + @olduser + '''' + ',''' + @newuser + ''''
	print @command 
	        FETCH NEXT FROM dbs
        INTO @dbname;
    END
    CLOSE dbs;
    DEALLOCATE dbs;
*/

GO
EXECUTE sp_ms_marksystemobject 'dbo.sp_CloneRights'
GO

	GO


*/
