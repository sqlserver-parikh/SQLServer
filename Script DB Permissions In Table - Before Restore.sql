USE tempdb 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_ScriptPermission]
    @DBName        NVARCHAR(128) = NULL,
    @SystemDBs     BIT = 0,
    @RetentionDays INT = 30
AS
/*********************************************************************************
Name:       usp_ScriptPermission
Date:       2024-05-22 (Updated: 2024-05-23)
Version:    1.2
Description: 
    Scripts database-level permissions (Users, Roles, Object-level, Schema-level, 
    and DB-level) and stores them in [dbo].[tbl_DBPermission].
    Includes a cleanup mechanism for old history.

Parameters:
    @DBName:        Specific database name to script. If NULL, scripts all databases.
    @SystemDBs:     If 1, includes master, model, and msdb. (tempdb is always excluded).
    @RetentionDays: Number of days to keep history in tbl_DBPermission. Default is 30.

Usage:
    EXEC [dbo].[usp_ScriptPermission] @DBName = 'SalesDB';
    EXEC [dbo].[usp_ScriptPermission] @RetentionDays = 60; -- Script and keep 60 days
*********************************************************************************/
BEGIN
    SET NOCOUNT ON;

    DECLARE @HostDB NVARCHAR(128) = DB_NAME();
    DECLARE @CurrentDB NVARCHAR(128);
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @InnerScript NVARCHAR(MAX);
    DECLARE @NextSnapID INT;

    -- 1. Ensure logging table exists
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_DBPermission]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[tbl_DBPermission](
            [ID] [int] IDENTITY(1,1) NOT NULL,
            [DBName] [nvarchar](128) NULL,
            [PermissionScript] [nvarchar](max) NULL,
            [ScriptDate] [datetime] NULL CONSTRAINT [DF_tbl_DBPermission_ScriptDate] DEFAULT (GETDATE()),
            [Servername] [nvarchar](128) NULL CONSTRAINT [DF_tbl_DBPermission_Servername] DEFAULT (@@SERVERNAME),
            [DBSnapID] [int] NULL,
            CONSTRAINT [PK_tbl_DBPermission] PRIMARY KEY CLUSTERED ([ID] ASC)
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
    END

    -- 2. Cleanup old records based on retention parameter
    IF @RetentionDays IS NOT NULL AND @RetentionDays >= 0
    BEGIN
        DELETE FROM [dbo].[tbl_DBPermission]
        WHERE [ScriptDate] < DATEADD(DAY, -@RetentionDays, GETDATE());
        
        IF @@ROWCOUNT > 0
            PRINT 'Cleanup: Removed records older than ' + CAST(@RetentionDays AS VARCHAR(10)) + ' days.';
    END

    -- 3. Define the Core Scripting Logic
    SET @InnerScript = N'
        SELECT ''-- [-- DB CONTEXT --] --'' AS [ScriptLine], 1 AS [SortOrder]
        UNION ALL
        SELECT  ''USE '' + QUOTENAME(DB_NAME()) + '';'', 1
        UNION ALL SELECT ''-- [-- DB USERS --] --'', 3
        UNION ALL
        SELECT  ''IF NOT EXISTS (SELECT [name] FROM sys.database_principals WHERE [name] = '' + N'''''''' + [name] + N'''''''' + '') BEGIN CREATE USER '' + QUOTENAME([name]) + '' FOR LOGIN '' + QUOTENAME([name]) + '' WITH DEFAULT_SCHEMA = '' + ISNULL(QUOTENAME([default_schema_name]), ''[dbo]'') + '' END; '', 4
        FROM    sys.database_principals
        WHERE [type] IN (''U'', ''S'', ''G'') AND [name] NOT IN (''sys'', ''INFORMATION_SCHEMA'', ''guest'')
        UNION ALL SELECT ''-- [-- DB ROLES --] --'', 6
        UNION ALL
        SELECT  ''EXEC sp_addrolemember @rolename = '' + ISNULL(QUOTENAME(USER_NAME(rm.role_principal_id), ''''''''), ''[UnknownRole]'') + '', @membername = '' + ISNULL(QUOTENAME(USER_NAME(rm.member_principal_id), ''''''''), ''[UnknownUser]'') + '';'', 7
        FROM    sys.database_role_members AS rm
        WHERE   USER_NAME(rm.member_principal_id) NOT IN (''dbo'')
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
        UNION ALL SELECT ''-- [--DB LEVEL PERMISSIONS --] --'', 12
        UNION ALL
        SELECT  CASE WHEN perm.state <> ''W'' THEN perm.state_desc ELSE ''GRANT'' END
                + '' '' + perm.permission_name + '' TO '' + QUOTENAME(USER_NAME(usr.principal_id)) 
                + CASE WHEN perm.state <> ''W'' THEN '''' ELSE '' WITH GRANT OPTION'' END, 13
        FROM    sys.database_permissions AS perm
        INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
        WHERE   [perm].[major_id] = 0 AND [usr].[principal_id] > 4 AND [usr].[type] IN (''G'', ''S'', ''U'') 
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

    -- 4. Identify Target Databases
    DECLARE @TargetDatabases TABLE (DBName NVARCHAR(128));
    INSERT INTO @TargetDatabases (DBName)
    SELECT name FROM sys.databases
    WHERE (@DBName IS NULL OR name = @DBName)
      AND name NOT IN ('tempdb', 'distribution')
      AND (
            @SystemDBs = 1 
            OR (name NOT IN ('master', 'model', 'msdb'))
          )
      AND state_desc = 'ONLINE'
      AND is_read_only = 0;

    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT DBName FROM @TargetDatabases;
    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @CurrentDB;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @NextSnapID = ISNULL(MAX(DBSnapID), 0) + 1 FROM dbo.tbl_DBPermission WHERE DBName = @CurrentDB;

        SET @DynamicSQL = N'USE ' + QUOTENAME(@CurrentDB) + N';
        INSERT INTO ' + QUOTENAME(@HostDB) + N'.dbo.tbl_DBPermission (DBName, PermissionScript, ScriptDate, Servername, DBSnapID)
        SELECT ''' + @CurrentDB + N''', [ScriptLine], GETDATE(), @@SERVERNAME, ' + CAST(@NextSnapID AS NVARCHAR(10)) + N'
        FROM (' + @InnerScript + N') AS FinalScript ORDER BY [SortOrder];';

        BEGIN TRY
            EXEC sp_executesql @DynamicSQL;
            PRINT 'Success: ' + @CurrentDB + ' (Snapshot ' + CAST(@NextSnapID AS VARCHAR(5)) + ')';
        END TRY
        BEGIN CATCH
            PRINT 'Error processing ' + @CurrentDB + ': ' + ERROR_MESSAGE();
        END CATCH

        FETCH NEXT FROM db_cursor INTO @CurrentDB;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_RestorePermission]
    @CurrentDBName NVARCHAR(128) = 'lIVEBACKUP', 
    @NewDBName     NVARCHAR(128) = NULL, 
    @DBSnapID      INT = NULL,            
    @Print         BIT = 1,              
    @Execute       BIT = 1               
AS
/*********************************************************************************
Name:       usp_RestorePermission
Date:       2024-05-22
Version:    1.1
Description: 
    Restores permissions from [dbo].[tbl_DBPermission] to a target database.

Parameters:
    @CurrentDBName: The database name as recorded in the logging table.
    @NewDBName:     The target DB to apply permissions to. If NULL, uses @CurrentDBName.
    @DBSnapID:      Specific snapshot ID. If NULL, defaults to the LATEST snapshot.
    @Print:         If 1, prints the T-SQL script to the message window.
    @Execute:       If 1, executes the T-SQL script against the target database.

Usage:
    -- Preview latest permissions for a DB
    EXEC [dbo].[usp_RestorePermission] @CurrentDBName = 'SalesDB';

    -- Restore latest permissions to a different DB (Execute)
    EXEC [dbo].[usp_RestorePermission] @CurrentDBName = 'SalesDB', @NewDBName = 'SalesDB_New', @Execute = 1, @Print = 0;
*********************************************************************************/
BEGIN
    SET NOCOUNT ON;

    -- Default: If NewDBName is null, target is the original DB
    IF @NewDBName IS NULL SET @NewDBName = @CurrentDBName;

    -- Default: If SnapID is null, get the latest one
    IF @DBSnapID IS NULL
    BEGIN
        SELECT @DBSnapID = MAX(DBSnapID) 
        FROM dbo.tbl_DBPermission 
        WHERE DBName = @CurrentDBName;
    END

    -- Validation
    IF DB_ID(@NewDBName) IS NULL
    BEGIN
        RAISERROR('Target database [%s] does not exist or is offline.', 16, 1, @NewDBName);
        RETURN;
    END

    IF @DBSnapID IS NULL OR NOT EXISTS (SELECT 1 FROM dbo.tbl_DBPermission WHERE DBName = @CurrentDBName AND DBSnapID = @DBSnapID)
    BEGIN
        RAISERROR('No permission history found for DB [%s] with Snapshot ID [%d].', 16, 1, @CurrentDBName, @DBSnapID);
        RETURN;
    END

    DECLARE @ScriptLine NVARCHAR(MAX);
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @SuccessCount INT = 0;
    DECLARE @FailCount INT = 0;

    -- Header
    IF @Print = 1
    BEGIN
        PRINT '-- =============================================================';
        PRINT '-- RESTORE SCRIPT FOR: ' + @NewDBName;
        PRINT '-- SOURCE DATA: ' + @CurrentDBName + ' (Snapshot: ' + CAST(@DBSnapID AS VARCHAR(10)) + ')';
        PRINT '-- GENERATED: ' + CAST(GETDATE() AS VARCHAR(30));
        PRINT '-- =============================================================';
        PRINT 'USE ' + QUOTENAME(@NewDBName) + ';';
        PRINT 'GO';
    END

    DECLARE cur_restore CURSOR LOCAL FAST_FORWARD FOR 
    SELECT PermissionScript 
    FROM dbo.tbl_DBPermission
    WHERE DBName = @CurrentDBName AND DBSnapID = @DBSnapID
    ORDER BY ID;

    OPEN cur_restore;
    FETCH NEXT FROM cur_restore INTO @ScriptLine;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Ignore comments and 'USE' statements from the source log
        IF LEFT(LTRIM(@ScriptLine), 2) <> '--' AND @ScriptLine NOT LIKE 'USE %' AND LEN(LTRIM(@ScriptLine)) > 0
        BEGIN
            
            -- EXECUTION LOGIC
            IF @Execute = 1
            BEGIN
                SET @DynamicSQL = N'USE ' + QUOTENAME(@NewDBName) + N'; ' + @ScriptLine;
                BEGIN TRY
                    EXEC sp_executesql @DynamicSQL;
                    SET @SuccessCount = @SuccessCount + 1;
                END TRY
                BEGIN CATCH
                    SET @FailCount = @FailCount + 1;
                    PRINT 'EXECUTION ERROR on line: ' + @ScriptLine;
                    PRINT 'ERROR DETAIL: ' + ERROR_MESSAGE();
                END CATCH
            END

            -- PRINT LOGIC
            IF @Print = 1
            BEGIN
                PRINT 'BEGIN TRY ' + @ScriptLine + ' END TRY';
                PRINT 'BEGIN CATCH PRINT ''Error on: ' + REPLACE(@ScriptLine, '''', '''''') + ''' + ERROR_MESSAGE() END CATCH;';
            END
        END
        ELSE IF @Print = 1 -- Still print comments for readability if they exist
        BEGIN
            PRINT @ScriptLine;
        END

        FETCH NEXT FROM cur_restore INTO @ScriptLine;
    END

    CLOSE cur_restore;
    DEALLOCATE cur_restore;

    -- Summary
    IF @Execute = 1
    BEGIN
        PRINT '-------------------------------------------------------------';
        PRINT 'Execution Summary: ' + CAST(@SuccessCount AS VARCHAR(10)) + ' succeeded, ' + CAST(@FailCount AS VARCHAR(10)) + ' failed.';
        PRINT '-------------------------------------------------------------';
    END
END
GO
