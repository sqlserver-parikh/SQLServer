CREATE OR ALTER PROCEDURE usp_ScriptPermission
    @DBName NVARCHAR(128) = NULL -- NULL = Loop through ALL databases
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @HostDB NVARCHAR(128) = DB_NAME();
    DECLARE @CurrentDB NVARCHAR(128);
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @InnerScript NVARCHAR(MAX);
    DECLARE @NextSnapID INT;

    ---------------------------------------------------------------------------
    -- 1. DEFINE THE CORE SCRIPT LOGIC
    --    Changes: Removed empty spacer rows; Added ISNULL to all QUOTENAMEs
    ---------------------------------------------------------------------------
    SET @InnerScript = N'
        /*********************************************/
        /*********   DB CONTEXT STATEMENT    *********/
        /*********************************************/
        SELECT ''-- [-- DB CONTEXT --] --'' AS [ScriptLine], 1 AS [SortOrder]
        UNION ALL
        SELECT  ''USE '' + QUOTENAME(DB_NAME()) + '';'', 1

        /*********************************************/
        /*********     DB USER CREATION      *********/
        /*********************************************/
        UNION ALL SELECT ''-- [-- DB USERS --] --'', 3
        UNION ALL
        SELECT  ''IF NOT EXISTS (SELECT [name] FROM sys.database_principals WHERE [name] = '' + N'''''''' + [name] + N'''''''' + '') BEGIN CREATE USER '' + QUOTENAME([name]) + '' FOR LOGIN '' + QUOTENAME([name]) + '' WITH DEFAULT_SCHEMA = '' + ISNULL(QUOTENAME([default_schema_name]), ''[dbo]'') + '' END; '', 4
        FROM    sys.database_principals
        WHERE [type] IN (''U'', ''S'', ''G'')
        AND   [name] IS NOT NULL -- Safety check

        /*********************************************/
        /*********    DB ROLE PERMISSIONS    *********/
        /*********************************************/
        UNION ALL SELECT ''-- [-- DB ROLES --] --'', 6
        UNION ALL
        SELECT  ''EXEC sp_addrolemember @rolename = '' + ISNULL(QUOTENAME(USER_NAME(rm.role_principal_id), ''''''''), ''[UnknownRole]'') + '', @membername = '' + ISNULL(QUOTENAME(USER_NAME(rm.member_principal_id), ''''''''), ''[UnknownUser]'') + '';'', 7
        FROM    sys.database_role_members AS rm
        WHERE   USER_NAME(rm.member_principal_id) IS NOT NULL 
        AND     USER_NAME(rm.role_principal_id) IS NOT NULL

        /*********************************************/
        /*********  OBJECT LEVEL PERMISSIONS *********/
        /*********************************************/
        UNION ALL SELECT ''-- [-- OBJECT LEVEL PERMISSIONS --] --'', 9
        UNION ALL
        SELECT  CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END
                + '' '' + perm.permission_name + '' ON '' + ISNULL(QUOTENAME(SCHEMA_NAME(obj.schema_id)), ''[UnknownSchema]'') + ''.'' + ISNULL(QUOTENAME(obj.name), ''[UnknownObject]'') 
                + CASE WHEN cl.column_id IS NULL THEN '''' ELSE ''('' + ISNULL(QUOTENAME(cl.name), ''UnknownColumn'') + '')'' END
                + '' TO '' + ISNULL(QUOTENAME(USER_NAME(usr.principal_id)), ''[UnknownUser]'') 
                + CASE WHEN perm.state <> ''W'' THEN '''' ELSE '' WITH GRANT OPTION'' END, 10
        FROM    sys.database_permissions AS perm
        INNER JOIN sys.objects AS obj ON perm.major_id = obj.[object_id]
        INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
        LEFT JOIN sys.columns AS cl ON cl.column_id = perm.minor_id AND cl.[object_id] = perm.major_id
        WHERE USER_NAME(usr.principal_id) IS NOT NULL

        /*********************************************/
        /*********    DB LEVEL PERMISSIONS   *********/
        /*********************************************/
        UNION ALL SELECT ''-- [--DB LEVEL PERMISSIONS --] --'', 12
        UNION ALL
        SELECT  CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END
                + '' '' + perm.permission_name + '' TO '' + ''['' + ISNULL(USER_NAME(usr.principal_id), ''UnknownUser'') + '']'' 
                + CASE WHEN perm.state <> ''W'' THEN '''' ELSE '' WITH GRANT OPTION'' END, 13
        FROM    sys.database_permissions AS perm
        INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
        WHERE   [perm].[major_id] = 0 
        AND     [usr].[principal_id] > 4 
        AND     [usr].[type] IN (''G'', ''S'', ''U'') 
        AND     USER_NAME(usr.principal_id) IS NOT NULL

        /*********************************************/
        /*********  SCHEMA PERMISSIONS      *********/
        /*********************************************/
        UNION ALL SELECT ''-- [--DB LEVEL SCHEMA PERMISSIONS --] --'', 15
        UNION ALL
        SELECT  CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END
                + '' '' + perm.permission_name + '' ON '' + class_desc + ''::'' + ISNULL(QUOTENAME(SCHEMA_NAME(major_id)), ''[UnknownSchema]'')
                + '' TO '' + ISNULL(QUOTENAME(USER_NAME(grantee_principal_id)), ''[UnknownUser]'') 
                + CASE WHEN perm.state <> ''W'' THEN '''' ELSE '' WITH GRANT OPTION'' END, 16
        from sys.database_permissions AS perm
        inner join sys.schemas s on perm.major_id = s.schema_id
        inner join sys.database_principals dbprin on perm.grantee_principal_id = dbprin.principal_id
        WHERE class = 3 ';

    ---------------------------------------------------------------------------
    -- 2. DETERMINE TARGET DATABASES
    ---------------------------------------------------------------------------
    DECLARE @TargetDatabases TABLE (DBName NVARCHAR(128));

    INSERT INTO @TargetDatabases (DBName)
    SELECT name 
    FROM sys.databases
    WHERE 
        (@DBName IS NULL OR name = @DBName)
        AND name NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')
        AND state_desc = 'ONLINE'
        AND user_access_desc = 'MULTI_USER'
        AND is_read_only = 0
        AND source_database_id IS NULL;

    ---------------------------------------------------------------------------
    -- 3. EXECUTE LOOP
    ---------------------------------------------------------------------------
    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR 
    SELECT DBName FROM @TargetDatabases;

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @CurrentDB;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Calculate SnapID
        SELECT @NextSnapID = ISNULL(MAX(DBSnapID), 0) + 1
        FROM dbo.tbl_DBPermission
        WHERE DBName = @CurrentDB;

        PRINT 'Processing: ' + @CurrentDB + ' (SnapID: ' + CAST(@NextSnapID AS VARCHAR(10)) + ')';

        SET @DynamicSQL = N'
        USE ' + QUOTENAME(@CurrentDB) + N';
        
        INSERT INTO ' + QUOTENAME(@HostDB) + N'.dbo.tbl_DBPermission (DBName, PermissionScript, ScriptDate, Servername, DBSnapID)
        SELECT 
            ''' + @CurrentDB + N''', 
            [ScriptLine], 
            GETDATE(), 
            @@SERVERNAME,
            ' + CAST(@NextSnapID AS NVARCHAR(20)) + N'
        FROM (
            ' + @InnerScript + N'
        ) AS FinalScript
        ORDER BY [SortOrder];
        ';

        BEGIN TRY
            EXEC sp_executesql @DynamicSQL;
        END TRY
        BEGIN CATCH
            PRINT 'Error processing ' + @CurrentDB + ': ' + ERROR_MESSAGE();
        END CATCH

        FETCH NEXT FROM db_cursor INTO @CurrentDB;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    PRINT 'Done.';
END
GO
