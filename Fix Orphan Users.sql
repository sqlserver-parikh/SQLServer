USE [tempdb]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_FixOrphanUsers]') AND type in (N'P', N'PC'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[usp_FixOrphanUsers] AS' 
END
GO

ALTER PROCEDURE [dbo].[usp_FixOrphanUsers]
    @DatabaseName NVARCHAR(255) = NULL,
    @Print        BIT = 0,
    @Execute      BIT = 1,
    @RaiseError   BIT = 0 -- If 1, fails the batch/job on error. If 0, logs and continues.
AS
/*********************************************************************************
    Ola Hallengren Style Documentation
    
    Description: Identifies and fixes orphaned users.
                 - If a matching Login exists, it re-links the User.
                 - If no matching Login exists, it transfers all owned schema 
                   and custom role ownership to dbo, then drops the User.

    Parameters:
    @DatabaseName: Specific database to process. If NULL/empty, all user databases.
    @Print:        Print the generated T-SQL commands.
    @Execute:      Execute the generated T-SQL commands.
    @RaiseError:   If 1, unhandled errors break execution (great for SQL Agent).
*********************************************************************************/
BEGIN
    SET NOCOUNT ON;

    -- Validate input database if specified
    IF @DatabaseName IS NOT NULL AND @DatabaseName <> ''
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName AND state_desc = 'ONLINE')
        BEGIN
            RAISERROR('The database %s does not exist or is not online.', 16, 1, @DatabaseName);
            RETURN;
        END
    END

    DECLARE @CurrentDBName NVARCHAR(128);
    DECLARE @UserName      NVARCHAR(128);
    DECLARE @DynamicSQL    NVARCHAR(MAX);
    DECLARE @ErrorMessage  NVARCHAR(MAX);

    IF OBJECT_ID('tempdb..#OrphanUsers') IS NOT NULL 
        DROP TABLE #OrphanUsers;

    CREATE TABLE #OrphanUsers (
        DBName   NVARCHAR(128),
        UserName NVARCHAR(128)
    );

    -- Database Discovery (Safer database filtering exclusion)
    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT name
    FROM sys.databases
    WHERE state_desc = 'ONLINE'
      AND is_read_only = 0
      AND name NOT IN ('master', 'model', 'msdb', 'tempdb')
      AND (name = @DatabaseName OR @DatabaseName IS NULL OR @DatabaseName = '');

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @CurrentDBName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @DynamicSQL = N'
            INSERT INTO #OrphanUsers (DBName, UserName)
            SELECT
                @DBName AS DBName,
                name AS UserName
            FROM ' + QUOTENAME(@CurrentDBName) + N'.sys.database_principals
            WHERE type IN (''S'', ''U'')
              AND authentication_type IN (1, 2)
              AND sid IS NOT NULL AND sid <> 0x0
              AND SUSER_SNAME(sid) IS NULL
              AND name NOT IN (''dbo'', ''guest'', ''INFORMATION_SCHEMA'', ''sys'')';
        
        EXEC sp_executesql @DynamicSQL, N'@DBName NVARCHAR(128)', @DBName = @CurrentDBName;
        
        FETCH NEXT FROM db_cursor INTO @CurrentDBName;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    -- Processing Orphans
    DECLARE user_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT UserName, DBName FROM #OrphanUsers;

    OPEN user_cursor;
    FETCH NEXT FROM user_cursor INTO @UserName, @CurrentDBName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if a matching server login exists
        IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @UserName AND type IN ('S', 'U'))
        BEGIN
            -- Relink User to Login
            SET @DynamicSQL = N'ALTER USER ' + QUOTENAME(@UserName) + N' WITH LOGIN = ' + QUOTENAME(@UserName) + N';';
        END
        ELSE
        BEGIN
            -- Dynamic script built cleanly utilizing STRING_AGG to avoid nested cursors entirely
            SET @DynamicSQL = N'
                DECLARE @SchemaFixes NVARCHAR(MAX) = '''';
                DECLARE @RoleFixes   NVARCHAR(MAX) = '''';
                DECLARE @FinalDrop   NVARCHAR(MAX) = N''DROP USER '' + QUOTENAME(' + QUOTENAME(@UserName, '''') + N');;

                -- Aggregating all schema ownership transfers into a single block
                SELECT @SchemaFixes = COALESCE(STRING_AGG(N''ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(name) + N'' TO [dbo];'', CHAR(13)), '''')
                FROM sys.schemas 
                WHERE principal_id = USER_ID(' + QUOTENAME(@UserName, '''') + N');

                -- Aggregating all custom role ownership transfers into a single block
                SELECT @RoleFixes = COALESCE(STRING_AGG(N''ALTER AUTHORIZATION ON ROLE::'' + QUOTENAME(name) + N'' TO [dbo];'', CHAR(13)), '''')
                FROM sys.database_principals 
                WHERE type = ''R'' AND owning_principal_id = USER_ID(' + QUOTENAME(@UserName, '''') + N');

                -- Combine and execute everything sequentially in one execution window
                DECLARE @FullTask NVARCHAR(MAX) = ISNULL(@SchemaFixes, '''') + CHAR(13) + ISNULL(@RoleFixes, '''') + CHAR(13) + @FinalDrop;
                EXEC sp_executesql @FullTask;';
        END

        -- Output Formatting
        IF @Print = 1
        BEGIN
            PRINT '-- =================================================='
            PRINT '-- Database: ' + @CurrentDBName + ' | User: ' + @UserName;
            PRINT '-- =================================================='
            PRINT @DynamicSQL;
        END

        IF @Execute = 1
        BEGIN
            BEGIN TRY
                DECLARE @ExecContext NVARCHAR(500) = QUOTENAME(@CurrentDBName) + N'..sp_executesql';
                EXEC @ExecContext @DynamicSQL;
            END TRY
            BEGIN CATCH
                SET @ErrorMessage = 'Error in DB ' + @CurrentDBName + ' for User ' + @UserName + ': ' + ERROR_MESSAGE();
                PRINT @ErrorMessage;
                
                -- Re-throw if the calling system needs to know it failed
                IF @RaiseError = 1
                BEGIN
                    THROW;
                END
            END CATCH
        END

        FETCH NEXT FROM user_cursor INTO @UserName, @CurrentDBName;
    END

    CLOSE user_cursor;
    DEALLOCATE user_cursor;
    DROP TABLE #OrphanUsers;
END
GO
