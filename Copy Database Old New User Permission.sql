USE tempdb 
GO
CREATE OR ALTER PROCEDURE dbo.sp_CloneUserRights
(
    @DatabaseName NVARCHAR(128) = 'UserDB',
    @OldUser NVARCHAR(128) = 'OldUser',
    @NewUser NVARCHAR(128) = 'NewUser',
    @NewLoginName NVARCHAR(128) = '',
    @PrintOnly BIT = 1,
    @ServerLevel BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @OldUser IS NULL OR @NewUser IS NULL
    BEGIN
        RAISERROR('User parameters cannot be NULL', 16, 1);
        RETURN;
    END

    DECLARE @DbCursor CURSOR;
    DECLARE @CurrentDb NVARCHAR(128);
    
    SET @DbCursor = CURSOR FOR
        SELECT name
        FROM sys.databases
        WHERE (@DatabaseName IS NULL OR name = @DatabaseName)
        AND HAS_DBACCESS(name) = 1 
        AND state_desc = 'ONLINE'
        AND database_id > 4;

    OPEN @DbCursor;
    FETCH NEXT FROM @DbCursor INTO @CurrentDb;

    WHILE @@FETCH_STATUS = 0
    BEGIN

    DECLARE @CreateTempProc NVARCHAR(MAX) = N'
    CREATE OR ALTER PROCEDURE #sp_CloneRightsInternal
    (
        @oldUser sysname,
        @newUser sysname,
        @NewLoginName sysname = null,
        @printOnly bit = 1,
        @ServerLevel bit = 0
    )
    AS
    BEGIN
        SET NOCOUNT ON;
        
        CREATE TABLE #output (command nvarchar(4000));
        
        IF @ServerLevel = 1
        BEGIN
            CREATE TABLE #ServerLevelRoles (ScriptToRun VARCHAR(1000));
            
            INSERT INTO #ServerLevelRoles
            SELECT ''EXEC sp_addsrvrolemember '''''' + @newUser + '''''','''''' + p.name + '''''';''
            FROM sys.server_role_members rm
            JOIN sys.server_principals p ON rm.role_principal_id = p.principal_id
            JOIN sys.server_principals m ON rm.member_principal_id = m.principal_id
            WHERE m.name = @oldUser;

            INSERT INTO #ServerLevelRoles
            SELECT CASE 
                WHEN sp.state_desc = ''GRANT_WITH_GRANT_OPTION'' 
                THEN ''GRANT '' + permission_name + '' TO '' + QUOTENAME(@newUser) + '' WITH GRANT OPTION;''
                ELSE state_desc + '' '' + permission_name + '' TO '' + QUOTENAME(@newUser) + '';''
            END
            FROM sys.server_permissions sp
            JOIN sys.server_principals sps ON sp.grantee_principal_id = sps.principal_id
            WHERE sps.name = @oldUser
            AND sp.type NOT IN (''COSQ'', ''CO'');

            DECLARE @permission varchar(1000);
            DECLARE srv_cursor CURSOR FOR 
            SELECT ScriptToRun FROM #ServerLevelRoles
            WHERE ScriptToRun IS NOT NULL;

            OPEN srv_cursor;
            FETCH NEXT FROM srv_cursor INTO @permission;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @printOnly = 1 PRINT @permission;
                FETCH NEXT FROM srv_cursor INTO @permission;
            END

            CLOSE srv_cursor;
            DEALLOCATE srv_cursor;
            DROP TABLE #ServerLevelRoles;
        END

        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @oldUser)
        BEGIN
            DECLARE @dbName NVARCHAR(128) = QUOTENAME(DB_NAME());
            RAISERROR(''Source user %s does not exist in database %s'', 11, 1, @oldUser, @dbName);
            RETURN;
        END

        INSERT INTO #output(command)
        VALUES(''USE '' + QUOTENAME(DB_NAME()));

        IF @NewLoginName IS NOT NULL
        BEGIN
            INSERT INTO #output(command)
            SELECT 
                ''IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '' + 
                QUOTENAME(@newUser, '''''''') + '')'' + CHAR(13) + CHAR(10) +
                ''CREATE USER '' + QUOTENAME(@newUser) + '' FOR LOGIN '' + 
                QUOTENAME(@NewLoginName) + 
                COALESCE('' WITH DEFAULT_SCHEMA = '' + QUOTENAME(dp.default_schema_name), '''')
            FROM sys.database_principals dp
            JOIN sys.server_principals sp ON dp.sid = sp.sid
            WHERE dp.name = @oldUser;
        END

        INSERT INTO #output(command)
        SELECT ''EXEC sp_addrolemember @rolename = '' + 
               QUOTENAME(USER_NAME(rm.role_principal_id), '''''''') + 
               '', @membername = '' + QUOTENAME(@newUser, '''''''')
        FROM sys.database_role_members rm
        WHERE USER_NAME(rm.member_principal_id) = @oldUser;

        INSERT INTO #output(command)
        SELECT 
            CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END +
            '' '' + perm.permission_name + '' ON '' + 
            QUOTENAME(SCHEMA_NAME(obj.schema_id)) + ''.'' + QUOTENAME(obj.name) +
            COALESCE(''('' + QUOTENAME(col.name) + '')'', '''') +
            '' TO '' + QUOTENAME(@newUser) +
            CASE WHEN perm.state = ''W'' THEN '' WITH GRANT OPTION'' ELSE '''' END
        FROM sys.database_permissions perm
        JOIN sys.objects obj ON perm.major_id = obj.object_id
        JOIN sys.database_principals usr ON perm.grantee_principal_id = usr.principal_id
        LEFT JOIN sys.columns col ON col.object_id = perm.major_id AND col.column_id = perm.minor_id
        WHERE usr.name = @oldUser;

        INSERT INTO #output(command)
        SELECT 
            CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END +
            '' '' + perm.permission_name + '' TO '' + QUOTENAME(@newUser) +
            CASE WHEN perm.state = ''W'' THEN '' WITH GRANT OPTION'' ELSE '''' END
        FROM sys.database_permissions perm
        JOIN sys.database_principals usr ON perm.grantee_principal_id = usr.principal_id
        WHERE usr.name = @oldUser AND perm.major_id = 0;

        DECLARE @sql NVARCHAR(MAX) = '''';
        DECLARE @command NVARCHAR(4000);
        
        DECLARE cmd_cursor CURSOR FOR 
        SELECT command FROM #output;

        OPEN cmd_cursor;
        FETCH NEXT FROM cmd_cursor INTO @command;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @printOnly = 1 PRINT @command;
            SET @sql = @sql + @command + CHAR(13) + CHAR(10);
            FETCH NEXT FROM cmd_cursor INTO @command;
        END

        CLOSE cmd_cursor;
        DEALLOCATE cmd_cursor;

        IF @printOnly = 0 
        BEGIN
            EXEC(@sql);
        END

        DROP TABLE #output;
    END';

    DECLARE @ExecCmd NVARCHAR(MAX) = N'USE ' + QUOTENAME(@CurrentDb) + N'; 
    EXEC(''' + REPLACE(@CreateTempProc, '''', '''''') + ''');
    EXEC #sp_CloneRightsInternal @oldUser = @p1, @newUser = @p2, @NewLoginName = @p3, @printOnly = @p4, @ServerLevel = @p5;';
    
    EXEC sp_executesql @ExecCmd, 
        N'@p1 sysname, @p2 sysname, @p3 sysname, @p4 bit, @p5 bit',
        @p1 = @OldUser,
        @p2 = @NewUser,
        @p3 = @NewLoginName,
        @p4 = @PrintOnly,
        @p5 = @ServerLevel;
        FETCH NEXT FROM @DbCursor INTO @CurrentDb;

    END

    CLOSE @DbCursor;
    DEALLOCATE @DbCursor;
END;
GO
sp_CloneUserRights
