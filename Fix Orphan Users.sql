USE TEMPDB
GO

CREATE OR ALTER PROCEDURE usp_FixOrphanUsers
    @DatabaseName NVARCHAR(255) = '',
    @PrintOnly BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @userid NVARCHAR(128);
    DECLARE @dbname NVARCHAR(128);
    DECLARE @sql NVARCHAR(MAX);

    -- Table to collect orphaned users
    IF OBJECT_ID('tempdb..#OrphanUsers') IS NOT NULL DROP TABLE #OrphanUsers;
    CREATE TABLE #OrphanUsers (
        DBName   NVARCHAR(128),
        UserName NVARCHAR(128)
    );

    -- Cursor for databases
    DECLARE db_cursor CURSOR FOR
        SELECT name
        FROM sys.databases
        WHERE
            state_desc = 'ONLINE'
            AND is_read_only = 0
            AND database_id > 4 -- Exclude system DBs
            AND (@DatabaseName = '' OR name = @DatabaseName);

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @dbname;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'
            INSERT INTO #OrphanUsers (DBName, UserName)
            SELECT
                N''' + REPLACE(@dbname, '''', '''''') + N''' AS DBName,
                name AS UserName
            FROM ' + QUOTENAME(@dbname) + N'.sys.database_principals
            WHERE type IN (''S'', ''U'')
              AND authentication_type IN (1, 2) -- SQL or Windows users
              AND sid IS NOT NULL AND sid <> 0x0
              AND SUSER_SNAME(sid) IS NULL
              AND name NOT IN (''dbo'', ''guest'', ''INFORMATION_SCHEMA'', ''sys'')
        ';
        EXEC sp_executesql @sql;
        
        FETCH NEXT FROM db_cursor INTO @dbname;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    -- Cursor for orphaned users
    DECLARE user_cursor CURSOR FOR
        SELECT UserName, DBName FROM #OrphanUsers;

    OPEN user_cursor;
    FETCH NEXT FROM user_cursor INTO @userid, @dbname;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- If login exists, fix orphan
        IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @userid)
        BEGIN
            SET @sql = N'USE ' + QUOTENAME(@dbname) + N';
                IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''' + REPLACE(@userid, '''', '''''') + N''')
                BEGIN
                    ALTER USER ' + QUOTENAME(@userid) + N' WITH LOGIN = ' + QUOTENAME(@userid) + N';
                END
            ';
            IF @PrintOnly = 1
                PRINT @sql;
            ELSE
                BEGIN TRY
                    EXEC sp_executesql @sql;
                    PRINT @sql;
                END TRY
                BEGIN CATCH
                    PRINT 'Error: ' + ERROR_MESSAGE();
                END CATCH
        END
        ELSE
        BEGIN
            -- Drop user if no login exists, handle schema ownership
            SET @sql = N'
            USE ' + QUOTENAME(@dbname) + N';
            DECLARE @schema NVARCHAR(128);
            SELECT @schema = name FROM sys.schemas WHERE principal_id = USER_ID(N''' + REPLACE(@userid, '''', '''''') + N''');
            IF @schema IS NOT NULL
            BEGIN
                DECLARE @auth_sql NVARCHAR(200);
                SET @auth_sql = N''ALTER AUTHORIZATION ON SCHEMA '' + QUOTENAME(@schema) + N'' TO dbo;'';
                EXEC sp_executesql @auth_sql;
            END
            IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''' + REPLACE(@userid, '''', '''''') + N''')
            BEGIN
                DROP USER ' + QUOTENAME(@userid) + N';
            END
            ';
            IF @PrintOnly = 1
                PRINT @sql;
            ELSE
                BEGIN TRY
                    EXEC sp_executesql @sql;
                    PRINT @sql;
                END TRY
                BEGIN CATCH
                    PRINT 'Error: ' + ERROR_MESSAGE();
                END CATCH
        END

        FETCH NEXT FROM user_cursor INTO @userid, @dbname;
    END

    CLOSE user_cursor;
    DEALLOCATE user_cursor;

    DROP TABLE #OrphanUsers;
END
GO
