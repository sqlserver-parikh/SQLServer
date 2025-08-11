USE tempdb;
GO

CREATE OR ALTER PROCEDURE dbo.sp_GetDatabaseRoles
    @DatabaseName NVARCHAR(128) = NULL,             -- Optional: Filter by specific database
    @LoginName NVARCHAR(128) = NULL,               -- Optional: Filter by specific login
    @RoleName NVARCHAR(128) = NULL,                -- Optional: Filter by specific role
    @IncludeSystemDatabases BIT = 0,               -- Optional: Include system databases
    @ExcludeWindowsGroups BIT = 0,                 -- Optional: Exclude Windows Groups
    @ShowOnlyOrphanedUsers BIT = 0,                -- Optional: Show only orphaned users
    @Debug BIT = 0                                 -- Optional: Show debug information
AS
BEGIN
    SET NOCOUNT ON;

    -- Create all temp tables at the start to ensure they exist for the CATCH block.
    CREATE TABLE #temprole (
        DBName sysname,
        UserName sysname,
        LoginSID VARBINARY(85),
        RoleName VARCHAR(2000),
        CreateDate DATETIME,
        UpdateDate DATETIME,
        UserType CHAR(1)
    );

    CREATE TABLE #DBUsers (
        DBName sysname,
        Username sysname
    );
    
    CREATE TABLE #tmpDatabaseRoles (
        ServerName sysname,
        DBName sysname,
        UserName sysname,
        LoginSID VARBINARY(85),
        RoleName VARCHAR(2000),
        CreateDate DATETIME,
        UpdateDate DATETIME,
        type_desc NVARCHAR(60)
    );

    CREATE TABLE #tmpGroupDetail (
        AccountName sysname,
        Type VARCHAR(128),
        Privilege VARCHAR(128),
        Mapped_Login_Name sysname,
        Permission_Path sysname
    );


    BEGIN TRY
        -- Use a reliable cursor instead of the undocumented sp_MSforeachdb
        DECLARE @db_name sysname;
        DECLARE @SqlCmd NVARCHAR(MAX);

        DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR 
            SELECT name 
            FROM sys.databases
            WHERE state_desc = 'ONLINE'
              AND HAS_DBACCESS(name) = 1
              AND (@DatabaseName IS NULL OR name = @DatabaseName)
              AND (@IncludeSystemDatabases = 1 OR database_id > 4);

        OPEN db_cursor;
        FETCH NEXT FROM db_cursor INTO @db_name;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SqlCmd = N'
            USE ' + QUOTENAME(@db_name) + ';

            WITH DBRoles AS (
                SELECT 
                    u.name AS UserName,
                    u.sid AS LoginSID,
                    r.name AS RoleName,
                    u.create_date AS CreateDate,
                    u.modify_date AS UpdateDate,
                    u.type AS UserType
                FROM sys.database_role_members rm
                JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id
                WHERE u.type IN (''S'', ''U'', ''G'') -- SQL_USER, WINDOWS_USER, WINDOWS_GROUP
                  AND u.principal_id > 4 -- Exclude system users
                  ' + CASE WHEN @LoginName IS NOT NULL THEN ' AND u.name = @pLoginName' ELSE '' END + '
                  ' + CASE WHEN @RoleName  IS NOT NULL THEN ' AND r.name = @pRoleName'  ELSE '' END + '
            )
            INSERT INTO #temprole (DBName, UserName, LoginSID, RoleName, CreateDate, UpdateDate, UserType)
            SELECT
                DB_NAME() AS DBName,
                a.UserName,
                a.LoginSID,
                STUFF((SELECT '', '' + b.RoleName
                       FROM DBRoles b
                       WHERE a.UserName = b.UserName
                       FOR XML PATH('''')), 1, 2, '''') AS RoleName,
                a.CreateDate,
                a.UpdateDate,
                a.UserType
            FROM DBRoles a
            GROUP BY a.UserName, a.LoginSID, a.CreateDate, a.UpdateDate, a.UserType;';
            
            IF @Debug = 1 PRINT @SqlCmd;

            EXEC sp_executesql @SqlCmd, 
                               N'@pLoginName NVARCHAR(128), @pRoleName NVARCHAR(128)', 
                               @pLoginName = @LoginName, @pRoleName = @RoleName;

            FETCH NEXT FROM db_cursor INTO @db_name;
        END

        CLOSE db_cursor;
        DEALLOCATE db_cursor;


        -- Handle orphaned users
        IF @ShowOnlyOrphanedUsers = 1
        BEGIN
            DECLARE @OrphanSql NVARCHAR(MAX) = N'';
            
            SELECT @OrphanSql = @OrphanSql + 
                'SELECT ' + QUOTENAME([name], '''') + ' AS [DBName], 
                        u.[name] ' +
                'FROM ' + QUOTENAME([name]) + '.sys.database_principals u ' +
                'WHERE u.type = ''S'' ' + -- SQL_USER
                'AND u.sid IS NOT NULL AND u.sid <> 0x01 ' + -- sid is valid
                'AND NOT EXISTS (SELECT 1 FROM sys.server_principals l WHERE l.sid = u.sid) '
                + CASE WHEN @LoginName IS NOT NULL THEN ' AND u.name = @LoginName' ELSE '' END + '
                UNION ALL '
            FROM sys.databases sd
            WHERE HAS_DBACCESS(sd.name) = 1
                AND sd.state_desc = 'ONLINE'
                AND (@DatabaseName IS NULL OR sd.name = @DatabaseName)
                AND (@IncludeSystemDatabases = 1 OR sd.database_id > 4);

            IF LEN(@OrphanSql) > 0
            BEGIN
                SET @OrphanSql = LEFT(@OrphanSql, LEN(@OrphanSql) - 10); -- Remove last 'UNION ALL '
                IF @Debug = 1 PRINT @OrphanSql;
                INSERT INTO #DBUsers (DBName, Username)
                EXEC sp_executesql @OrphanSql, N'@LoginName NVARCHAR(128)', @LoginName;
            END
        END

        -- Populate final results
        INSERT INTO #tmpDatabaseRoles (ServerName, DBName, UserName, LoginSID, RoleName, CreateDate, UpdateDate, type_desc)
        SELECT 
            @@SERVERNAME,
            t.DBName,
            t.UserName,
            t.LoginSID,
            t.RoleName,
            t.CreateDate,
            t.UpdateDate,
            sp.type_desc
        FROM #temprole t
        INNER JOIN sys.server_principals sp ON sp.sid = t.LoginSID
        WHERE 1 = 1
            AND (@ExcludeWindowsGroups = 0 OR sp.type <> 'G')
            AND (@ShowOnlyOrphanedUsers = 0 OR EXISTS (SELECT 1 FROM #DBUsers o WHERE o.DBName = t.DBName AND o.Username = t.UserName));

        -- Final output logic
        IF @ExcludeWindowsGroups = 0
        BEGIN
            DECLARE @lname sysname;
            
            DECLARE Roles CURSOR LOCAL FAST_FORWARD FOR
                SELECT UserName
                FROM #tmpDatabaseRoles
                WHERE type_desc = 'WINDOWS_GROUP';

            OPEN Roles;
            FETCH NEXT FROM Roles INTO @lname;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    INSERT INTO #tmpGroupDetail (AccountName, Type, Privilege, Mapped_Login_Name, Permission_Path)
                    EXEC xp_logininfo @lname, 'members';
                END TRY
                BEGIN CATCH
                    PRINT 'Could not expand group: ' + @lname + '. Error: ' + ERROR_MESSAGE();
                END CATCH
                FETCH NEXT FROM Roles INTO @lname;
            END

            CLOSE Roles;
            DEALLOCATE Roles;

            SELECT
                A.ServerName,
                A.DBName,
                A.UserName AS LoginName,
                ISNULL(B.AccountName, 'Individual Login') AS GroupMembers,
                A.RoleName AS DatabaseRoles,
                A.CreateDate,
                A.UpdateDate,
                A.type_desc
            FROM #tmpDatabaseRoles A
            LEFT JOIN #tmpGroupDetail B ON A.UserName = B.Permission_Path
            ORDER BY A.type_desc, A.DBName, A.UserName;
        END
        ELSE
        BEGIN
            SELECT
                ServerName,
                DBName,
                UserName AS LoginName,
                'Individual Login' AS GroupMembers,
                RoleName AS DatabaseRoles,
                CreateDate,
                UpdateDate,
                type_desc
            FROM #tmpDatabaseRoles
            ORDER BY type_desc, DBName, UserName;
        END

    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'db_cursor') >= 0
        BEGIN
            CLOSE db_cursor;
            DEALLOCATE db_cursor;
        END
        
        IF CURSOR_STATUS('local', 'Roles') >= 0
        BEGIN
            CLOSE Roles;
            DEALLOCATE Roles;
        END

        SELECT
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() AS ErrorState,
            ERROR_PROCEDURE() AS ErrorProcedure,
            ERROR_LINE() AS ErrorLine,
            ERROR_MESSAGE() AS ErrorMessage;
    END CATCH;

    DROP TABLE IF EXISTS #temprole;
    DROP TABLE IF EXISTS #DBUsers;
    DROP TABLE IF EXISTS #tmpDatabaseRoles;
    DROP TABLE IF EXISTS #tmpGroupDetail;
END;
GO
