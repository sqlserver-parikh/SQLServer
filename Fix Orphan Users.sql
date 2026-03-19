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
    @Execute      BIT = 1
AS
/*********************************************************************************
    Ola Hallengren Style Documentation
    
    Description: Identifies and fixes orphaned users.
                 - If a matching Login exists, it re-links the User.
                 - If no matching Login exists, it transfers schema ownership 
                   to dbo and drops the User.

    Parameters:
    @DatabaseName: Specific database to process. If NULL or empty, all databases.
    @Print:        Print the generated T-SQL commands.
    @Execute:      Execute the generated T-SQL commands.
*********************************************************************************/
BEGIN
    SET NOCOUNT ON;

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

    -- Database discovery
    DECLARE db_cursor CURSOR FOR
    SELECT name
    FROM sys.databases
    WHERE state_desc = 'ONLINE'
      AND is_read_only = 0
      AND database_id > 4 
      AND (name = @DatabaseName OR @DatabaseName IS NULL OR @DatabaseName = '');

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @CurrentDBName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @DynamicSQL = N'
            INSERT INTO #OrphanUsers (DBName, UserName)
            SELECT
                N''' + REPLACE(@CurrentDBName, '''', '''''') + N''' AS DBName,
                name AS UserName
            FROM ' + QUOTENAME(@CurrentDBName) + N'.sys.database_principals
            WHERE type IN (''S'', ''U'')
              AND authentication_type IN (1, 2)
              AND sid IS NOT NULL AND sid <> 0x0
              AND SUSER_SNAME(sid) IS NULL
              AND name NOT IN (''dbo'', ''guest'', ''INFORMATION_SCHEMA'', ''sys'')';
        
        EXEC sp_executesql @DynamicSQL;
        FETCH NEXT FROM db_cursor INTO @CurrentDBName;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    -- Processing Orphans
    DECLARE user_cursor CURSOR FOR
    SELECT UserName, DBName FROM #OrphanUsers;

    OPEN user_cursor;
    FETCH NEXT FROM user_cursor INTO @UserName, @CurrentDBName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @UserName)
        BEGIN
            -- Relink User to Login
            SET @DynamicSQL = N'USE ' + QUOTENAME(@CurrentDBName) + N'; ALTER USER ' + QUOTENAME(@UserName) + N' WITH LOGIN = ' + QUOTENAME(@UserName) + N';';
        END
        ELSE
        BEGIN
            -- Fix Ownership then Drop
            SET @DynamicSQL = N'USE ' + QUOTENAME(@CurrentDBName) + N'; 
                DECLARE @schemaName NVARCHAR(128);
                DECLARE @dropSQL NVARCHAR(MAX);
                SELECT @schemaName = name FROM sys.schemas WHERE principal_id = USER_ID(' + QUOTENAME(@UserName, '''') + N');
                IF @schemaName IS NOT NULL
                BEGIN
                    SET @dropSQL = N''ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(@schemaName) + N'' TO [dbo];'';
                    EXEC sp_executesql @dropSQL;
                END
                SET @dropSQL = N''DROP USER '' + QUOTENAME(' + QUOTENAME(@UserName, '''') + N' ) + N'';'';
                EXEC sp_executesql @dropSQL;';
        END

        IF @Print = 1
        BEGIN
            PRINT '-- Database: ' + @CurrentDBName + ' | User: ' + @UserName;
            PRINT @DynamicSQL;
        END

        IF @Execute = 1
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @DynamicSQL;
            END TRY
            BEGIN CATCH
                SET @ErrorMessage = 'Error in DB ' + @CurrentDBName + ' for User ' + @UserName + ': ' + ERROR_MESSAGE();
                PRINT @ErrorMessage;
            END CATCH
        END

        FETCH NEXT FROM user_cursor INTO @UserName, @CurrentDBName;
    END

    CLOSE user_cursor;
    DEALLOCATE user_cursor;
    DROP TABLE #OrphanUsers;
END
GO
