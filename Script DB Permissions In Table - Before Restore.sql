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

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_DBPermission]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[tbl_DBPermission](
        [ID] [int] IDENTITY(1,1) NOT NULL,
        [DBName] [nvarchar](128) NULL,
        [PermissionScript] [nvarchar](max) NULL,
        [ScriptDate] [datetime] NULL CONSTRAINT [DF_tbl_DBPermission_ScriptDate] DEFAULT (GETDATE()),
        [Servername] [nvarchar](128) NULL CONSTRAINT [DF_tbl_DBPermission_Servername] DEFAULT (@@SERVERNAME),
        [DBSnapID] [int] NULL CONSTRAINT [DF_tbl_DBPermission_DBSnapID] DEFAULT ((1)),
        CONSTRAINT [PK_tbl_DBPermission] PRIMARY KEY CLUSTERED 
        (
            [ID] ASC
        ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
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


CREATE OR ALTER PROCEDURE usp_RestorePermission
    @CurrentDBName NVARCHAR(128), -- The DB Name stored in tbl_DBPermission
    @NewDBName NVARCHAR(128),     -- The Target DB to apply permissions to
    @DBSnapID INT,                -- The Snapshot ID to restore
    @Execute BIT = 0              -- 0 = Print Script with Error Handling; 1 = Execute Directly
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Validation
    IF DB_ID(@NewDBName) IS NULL
    BEGIN
        RAISERROR('Target database [%s] does not exist.', 16, 1, @NewDBName);
        RETURN;
    END

    IF NOT EXISTS (SELECT 1 FROM dbo.tbl_DBPermission WHERE DBName = @CurrentDBName AND DBSnapID = @DBSnapID)
    BEGIN
        RAISERROR('No permissions found for DB [%s] with Snapshot ID [%d].', 16, 1, @CurrentDBName, @DBSnapID);
        RETURN;
    END

    DECLARE @ScriptLine NVARCHAR(MAX);
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @ErrorMsg NVARCHAR(MAX);
    DECLARE @SuccessCount INT = 0;
    DECLARE @FailCount INT = 0;

    -- 2. Header Information
    IF @Execute = 1
    BEGIN
        PRINT '-------------------------------------------------------------';
        PRINT 'Restoring Permissions (EXECUTION MODE)';
        PRINT 'Source: ' + @CurrentDBName + ' (SnapID: ' + CAST(@DBSnapID AS VARCHAR(10)) + ')';
        PRINT 'Target: ' + @NewDBName;
        PRINT '-------------------------------------------------------------';
    END
    ELSE
    BEGIN
        PRINT '-- -------------------------------------------------------------';
        PRINT '-- Generated Restoration Script';
        PRINT '-- Source: ' + @CurrentDBName + ' (SnapID: ' + CAST(@DBSnapID AS VARCHAR(10)) + ')';
        PRINT '-- Target: ' + @NewDBName;
        PRINT '-- -------------------------------------------------------------';
        PRINT '';
        PRINT 'USE ' + QUOTENAME(@NewDBName) + ';';
        PRINT 'GO';
        PRINT '';
    END

    -- 3. Cursor to loop through the stored script lines
    DECLARE cur_restore CURSOR LOCAL FAST_FORWARD FOR 
    SELECT PermissionScript 
    FROM dbo.tbl_DBPermission
    WHERE DBName = @CurrentDBName 
      AND DBSnapID = @DBSnapID
    ORDER BY ID;

    OPEN cur_restore;
    FETCH NEXT FROM cur_restore INTO @ScriptLine;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Filter out comments and blank lines
        IF LEFT(LTRIM(@ScriptLine), 2) <> '--' AND LEN(LTRIM(@ScriptLine)) > 0
        BEGIN
            -- Skip the original "USE [OldDB]" statement from the source script
            IF @ScriptLine NOT LIKE 'USE %'
            BEGIN
                
                -- ==========================================================
                -- MODE: EXECUTE (@Execute = 1)
                -- ==========================================================
                IF @Execute = 1
                BEGIN
                    SET @DynamicSQL = N'USE ' + QUOTENAME(@NewDBName) + N'; ' + @ScriptLine;

                    BEGIN TRY
                        EXEC sp_executesql @DynamicSQL;
                        SET @SuccessCount = @SuccessCount + 1;
                    END TRY
                    BEGIN CATCH
                        SET @FailCount = @FailCount + 1;
                        SET @ErrorMsg = ERROR_MESSAGE();
                        PRINT 'FAILED Statement: ' + LEFT(@ScriptLine, 100) + '...';
                        PRINT '   Error: ' + @ErrorMsg;
                        PRINT '-------------------------------------------------------------';
                    END CATCH
                END

                -- ==========================================================
                -- MODE: PRINT / GENERATE SCRIPT (@Execute = 0)
                -- ==========================================================
                ELSE
                BEGIN
                    -- We wrap the statement in TRY/CATCH so manual execution doesn't stop on error
                    PRINT 'BEGIN TRY';
                    PRINT '    ' + @ScriptLine;
                    PRINT 'END TRY';
                    PRINT 'BEGIN CATCH';
                    -- We print the error inside the generated script for the user to see
                    PRINT '    PRINT ''Error Executing: ' + LEFT(REPLACE(@ScriptLine, '''', ''), 50) + '...'';';
                    PRINT '    PRINT ''Reason: '' + ERROR_MESSAGE();';
                    PRINT 'END CATCH';
                    PRINT ''; -- Empty line for readability
                END
            END
        END

        FETCH NEXT FROM cur_restore INTO @ScriptLine;
    END

    CLOSE cur_restore;
    DEALLOCATE cur_restore;

    -- 4. Summary (Only needed for Execution Mode)
    IF @Execute = 1
    BEGIN
        PRINT '-------------------------------------------------------------';
        PRINT 'Restore Complete.';
        PRINT 'Successful Statements: ' + CAST(@SuccessCount AS VARCHAR(10));
        PRINT 'Failed Statements:     ' + CAST(@FailCount AS VARCHAR(10));
        PRINT '-------------------------------------------------------------';
    END
    ELSE
    BEGIN
        PRINT '-- -------------------------------------------------------------';
        PRINT '-- End of Generated Script';
        PRINT '-- -------------------------------------------------------------';
    END
END
GO

