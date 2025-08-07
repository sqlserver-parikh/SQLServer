USE tempdb
GO

CREATE OR ALTER PROCEDURE dbo.sp_CloneUserRights
(
    @OldUser NVARCHAR(128) = 'UserOld',
    @NewUser NVARCHAR(128) = 'UserNew',
    @NewLoginName NVARCHAR(128) = NULL,
    @DatabaseNames NVARCHAR(MAX) = '%',
    @PermissionType NVARCHAR(10) = 'GRANT', -- 'All', 'Grant', or 'Deny'
    @PrintOnly BIT = 0,
    @ServerLevel BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;

    -- Parameter Validation
    IF @OldUser IS NULL OR @NewUser IS NULL BEGIN RAISERROR('@OldUser and @NewUser cannot be NULL.', 16, 1); RETURN; END
    IF @PermissionType NOT IN ('All', 'Grant', 'Deny') BEGIN RAISERROR('Invalid @PermissionType. Use ''All'', ''Grant'', or ''Deny''.', 16, 1); RETURN; END

    DECLARE @LoginToMap NVARCHAR(128) = ISNULL(NULLIF(@NewLoginName, ''), @NewUser);

    --================================================================================
    -- STEP 1: SERVER-LEVEL PERMISSIONS (RUNS ONCE)
    --================================================================================
    IF @ServerLevel = 1
    BEGIN
        DECLARE @ServerSQL NVARCHAR(MAX) = N'';
        IF @PermissionType IN ('All', 'Grant')
        BEGIN
            SELECT @ServerSQL = @ServerSQL + N'USE [master]; EXEC sp_addsrvrolemember @loginame = ' + QUOTENAME(@LoginToMap, '''') + ', @rolename = ' + QUOTENAME(p.name, '''') + ';' + CHAR(13)+CHAR(10)
            FROM sys.server_role_members rm JOIN sys.server_principals p ON rm.role_principal_id = p.principal_id JOIN sys.server_principals m ON rm.member_principal_id = m.principal_id
            WHERE m.name = @OldUser;
        END

        SELECT @ServerSQL = @ServerSQL + N'USE [master]; ' + (CASE WHEN sp.state = 'W' THEN 'GRANT' ELSE sp.state_desc END) + ' ' + sp.permission_name + ' TO ' + QUOTENAME(@LoginToMap) + CASE WHEN sp.state = 'W' THEN ' WITH GRANT OPTION;' ELSE ';' END + CHAR(13)+CHAR(10)
        FROM sys.server_permissions sp JOIN sys.server_principals sps ON sp.grantee_principal_id = sps.principal_id
        WHERE sps.name = @OldUser AND sp.type NOT IN ('COSQ', 'CO')
          AND (@PermissionType = 'All' OR (@PermissionType = 'Grant' AND sp.state IN ('G', 'W')) OR (@PermissionType = 'Deny' AND sp.state = 'D'));

        IF @PrintOnly = 1
        BEGIN
            PRINT '--==================================';
            PRINT '-- 1. SERVER-LEVEL PERMISSIONS (' + @PermissionType + ')';
            PRINT '-- NOTE: The login ' + QUOTENAME(@LoginToMap) + ' must exist to execute these commands.';
            PRINT '--==================================';
            PRINT ISNULL(NULLIF(@ServerSQL, ''), '-- No server-level permissions found to script.');
        END
        ELSE IF NULLIF(@ServerSQL, '') IS NOT NULL
        BEGIN
            EXEC sp_executesql @ServerSQL;
        END
    END

    --================================================================================
    -- STEP 2: DATABASE-LEVEL PERMISSIONS (LOOPS THROUGH DATABASES)
    --================================================================================
    CREATE TABLE #DbPatterns (Pattern NVARCHAR(128) NOT NULL, IsExclude BIT NOT NULL);
    IF @DatabaseNames IS NOT NULL AND LTRIM(RTRIM(@DatabaseNames)) <> ''
        INSERT INTO #DbPatterns(Pattern, IsExclude) SELECT CASE WHEN LTRIM(RTRIM(value)) LIKE '-%' THEN SUBSTRING(LTRIM(RTRIM(value)), 2, 128) ELSE LTRIM(RTRIM(value)) END, CASE WHEN LTRIM(RTRIM(value)) LIKE '-%' THEN 1 ELSE 0 END FROM STRING_SPLIT(@DatabaseNames, ',');
    ELSE
        INSERT INTO #DbPatterns(Pattern, IsExclude) VALUES ('%', 0), ('master', 1), ('model', 1);

    DECLARE @DbCursor CURSOR, @CurrentDb NVARCHAR(128), @SqlToExec NVARCHAR(MAX);
    SET @DbCursor = CURSOR FORWARD_ONLY STATIC READ_ONLY FOR
        SELECT d.name FROM sys.databases d
        WHERE d.state_desc = 'ONLINE' AND HAS_DBACCESS(d.name) = 1
          AND EXISTS (SELECT 1 FROM #DbPatterns p WHERE p.IsExclude = 0 AND d.name LIKE p.Pattern)
          AND NOT EXISTS (SELECT 1 FROM #DbPatterns p WHERE p.IsExclude = 1 AND d.name LIKE p.Pattern);

    OPEN @DbCursor; FETCH NEXT FROM @DbCursor INTO @CurrentDb;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SqlToExec = N'USE ' + QUOTENAME(@CurrentDb) + N'; SELECT @userExists = 1 FROM sys.database_principals WHERE name = @oldUserName';
        DECLARE @UserExistsInDb BIT = 0;
        EXEC sp_executesql @SqlToExec, N'@oldUserName sysname, @userExists BIT OUTPUT', @oldUserName = @OldUser, @userExists = @UserExistsInDb OUTPUT;

        IF @UserExistsInDb = 0
        BEGIN
            IF @PrintOnly = 1 PRINT CHAR(13) + CHAR(10) + '-- INFO: Source user ' + QUOTENAME(@OldUser) + ' does not exist in database ' + QUOTENAME(@CurrentDb) + '. Skipping.';
            FETCH NEXT FROM @DbCursor INTO @CurrentDb;
            CONTINUE;
        END

        IF @PrintOnly = 1
        BEGIN
             PRINT CHAR(13) + CHAR(10) + '--==================================';
             PRINT '-- DATABASE-LEVEL PERMISSIONS FOR: ' + QUOTENAME(@CurrentDb) + ' (' + @PermissionType + ')';
             PRINT '--==================================';
        END

        -- PHASE 1: ALWAYS GENERATE THE `IF NOT EXISTS...CREATE USER` BLOCK.
        DECLARE @DefaultSchema NVARCHAR(128);
        SET @SqlToExec = N'USE ' + QUOTENAME(@CurrentDb) + N'; SELECT @Schema = default_schema_name FROM sys.database_principals WHERE name = @p_OldUser;';
        EXEC sp_executesql @SqlToExec, N'@p_OldUser sysname, @Schema NVARCHAR(128) OUTPUT', @p_OldUser = @OldUser, @Schema = @DefaultSchema OUTPUT;

        DECLARE @CreateUserCmd NVARCHAR(MAX);
        SET @CreateUserCmd =  N'USE ' + QUOTENAME(@CurrentDb) + N';' + char(13) + N'IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ' + QUOTENAME(@NewUser, '''') + N')' + CHAR(13)+CHAR(10) +
                             N'BEGIN' + CHAR(13)+CHAR(10) +
                             N'    CREATE USER ' + QUOTENAME(@NewUser) + N' FOR LOGIN ' + QUOTENAME(@LoginToMap) +
                             ISNULL(N' WITH DEFAULT_SCHEMA = ' + QUOTENAME(@DefaultSchema), N'') + N';' + CHAR(13)+CHAR(10) +
                             N'END;';

        IF @PrintOnly = 1
            PRINT @CreateUserCmd;
        ELSE
        BEGIN
            SET @SqlToExec = N'USE ' + QUOTENAME(@CurrentDb) + N'; ' + @CreateUserCmd;
            EXEC sp_executesql @SqlToExec;
        END

        -- PHASE 2: Gather and execute all other permissions one-by-one.
        CREATE TABLE #CommandsToRun (Command NVARCHAR(MAX));
        SET @SqlToExec = N'USE ' + QUOTENAME(@CurrentDb) + N';
            INSERT INTO #CommandsToRun (Command)
            SELECT cmd FROM (
                SELECT ''EXEC sp_addrolemember @rolename = '' + QUOTENAME(r.name) + '', @membername = '' + QUOTENAME(@p_NewUser) + '';''
                FROM sys.database_role_members rm JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                WHERE u.name = @p_OldUser AND @p_PermissionType IN (''All'', ''Grant'')
                UNION ALL
                SELECT
                    (CASE WHEN p.state = ''W'' THEN ''GRANT'' ELSE p.state_desc END) + '' '' + p.permission_name + '' ON '' +
                    CASE p.class
                        WHEN 1 THEN QUOTENAME(OBJECT_SCHEMA_NAME(p.major_id)) + ''.'' + QUOTENAME(OBJECT_NAME(p.major_id))
                        WHEN 3 THEN ''SCHEMA::'' + QUOTENAME(s.name)
                        WHEN 4 THEN (CASE pr.type WHEN ''R'' THEN ''ROLE::'' WHEN ''U'' THEN ''USER::'' WHEN ''A'' THEN ''APPLICATION ROLE::'' END) + QUOTENAME(pr.name)
                        WHEN 5 THEN ''ASSEMBLY::'' + QUOTENAME(a.name)
                        WHEN 6 THEN ''TYPE::'' + QUOTENAME(t.name)
                        WHEN 10 THEN ''XML SCHEMA COLLECTION::'' + QUOTENAME(x.name)
                    END +
                    ISNULL(''('' + QUOTENAME(c.name) + '')'', '''') + '' TO '' + QUOTENAME(@p_NewUser) +
                    (CASE WHEN p.state = ''W'' THEN '' WITH GRANT OPTION'' ELSE '''' END) + '';''
                FROM sys.database_permissions p
                JOIN sys.database_principals u ON p.grantee_principal_id = u.principal_id
                LEFT JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id AND p.class = 1
                LEFT JOIN sys.schemas s ON p.major_id = s.schema_id AND p.class = 3
                LEFT JOIN sys.database_principals pr ON p.major_id = pr.principal_id AND p.class = 4
                LEFT JOIN sys.assemblies a ON p.major_id = a.assembly_id AND p.class = 5
                LEFT JOIN sys.types t ON p.major_id = t.user_type_id AND p.class = 6
                LEFT JOIN sys.xml_schema_collections x ON p.major_id = x.xml_collection_id AND p.class = 10
                WHERE u.name = @p_OldUser AND p.major_id <> 0
                  AND (@p_PermissionType = ''All'' OR (@p_PermissionType = ''Grant'' AND p.state IN (''G'',''W'')) OR (@p_PermissionType = ''Deny'' AND p.state = ''D''))
                UNION ALL
                SELECT (CASE WHEN p.state = ''W'' THEN ''GRANT'' ELSE p.state_desc END) + '' '' + p.permission_name + '' TO '' + QUOTENAME(@p_NewUser) + (CASE WHEN p.state = ''W'' THEN '' WITH GRANT OPTION'' ELSE '''' END) + '';''
                FROM sys.database_permissions p
                JOIN sys.database_principals u ON p.grantee_principal_id = u.principal_id
                WHERE u.name = @p_OldUser AND p.major_id = 0 AND p.class = 0
                  AND (@p_PermissionType = ''All'' OR (@p_PermissionType = ''Grant'' AND p.state IN (''G'',''W'')) OR (@p_PermissionType = ''Deny'' AND p.state = ''D''))
            ) AS AllCommands(cmd);';
        EXEC sp_executesql @SqlToExec, N'@p_OldUser sysname, @p_NewUser sysname, @p_PermissionType nvarchar(10)', @p_OldUser = @OldUser, @p_NewUser = @NewUser, @p_PermissionType = @PermissionType;

        DECLARE @cmd NVARCHAR(MAX);
        DECLARE cmd_cursor CURSOR FOR SELECT Command FROM #CommandsToRun 
		WHERE Command IS NOT NULL;

        OPEN cmd_cursor; FETCH NEXT FROM cmd_cursor INTO @cmd;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Your excellent suggestion implemented here: always prefix with USE.
            DECLARE @FinalCmd NVARCHAR(MAX) = N'USE ' + QUOTENAME(@CurrentDb) + N'; ' + @cmd;
            IF @PrintOnly = 1
            BEGIN
                PRINT @FinalCmd;
            END
            ELSE
            BEGIN
                BEGIN TRY
                    EXEC sp_executesql @FinalCmd;
                END TRY
                BEGIN CATCH
                    PRINT N'-- FAILED in ' + QUOTENAME(@CurrentDb) + ': ' + ERROR_MESSAGE();
                    PRINT N'-- COMMAND: ' + @cmd;
                END CATCH
            END
            FETCH NEXT FROM cmd_cursor INTO @cmd;
        END
        CLOSE cmd_cursor; DEALLOCATE cmd_cursor;

        DROP TABLE #CommandsToRun;
        FETCH NEXT FROM @DbCursor INTO @CurrentDb;
    END

    CLOSE @DbCursor; DEALLOCATE @DbCursor;
    DROP TABLE #DbPatterns;
END;
GO
