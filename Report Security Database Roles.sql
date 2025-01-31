USE tempdb;
GO

CREATE OR ALTER PROCEDURE dbo.sp_GetDatabaseRoles
    @DatabaseName NVARCHAR(128) = NULL,              -- Optional: Filter by specific database
    @LoginName NVARCHAR(128) = NULL,                -- Optional: Filter by specific login
    @RoleName NVARCHAR(128) = NULL,                 -- Optional: Filter by specific role
    @IncludeSystemDatabases BIT = 0,                -- Optional: Include system databases
    @ExcludeWindowsGroups BIT = 0,                  -- Optional: Exclude Windows Groups
    @ShowOnlyOrphanedUsers BIT = 0,                 -- Optional: Show only orphaned users
    @Debug BIT = 0                                  -- Optional: Show debug information
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Temporary tables
        CREATE TABLE #temprole (
            DBName varchar(128),
            UserName varchar(128),
            LoginSID varbinary(128),
            RoleName varchar(2000),
            CreateDate datetime,
            UpdateDate datetime
        );

        -- Dynamic SQL for database iteration
        DECLARE @SqlCmd NVARCHAR(MAX) = N'
        IF EXISTS (
            SELECT TOP (1) 1 
            FROM sys.databases sd 
            WHERE sd.name = "?"
            AND HAS_DBACCESS(sd.name) = 1
            AND sd.is_read_only = 0
            AND sd.state_desc = ''ONLINE''
            AND sd.user_access_desc = ''MULTI_USER''
            AND sd.is_in_standby = 0'
            + CASE 
                WHEN @IncludeSystemDatabases = 0 
                THEN ' AND sd.name NOT IN (''master'', ''model'', ''msdb'', ''tempdb'')' 
                ELSE '' 
              END
            + CASE 
                WHEN @DatabaseName IS NOT NULL 
                THEN ' AND sd.name = @DatabaseName' 
                ELSE '' 
              END + '
        ) 
        BEGIN 
            WITH cte AS (
                SELECT 
                    "?" DBName,
                    b.name AS UserName,
                    b.sid,
                    c.name AS RoleName,
                    b.createdate,
                    b.updatedate
                FROM [?].dbo.sysmembers a
                JOIN [?].dbo.sysusers b ON a.memberuid = b.uid 
                JOIN [?].dbo.sysusers c ON a.groupuid = c.uid
                WHERE 1=1 '
                + CASE 
                    WHEN @LoginName IS NOT NULL 
                    THEN ' AND b.name LIKE @LoginName' 
                    ELSE '' 
                  END
                + CASE 
                    WHEN @RoleName IS NOT NULL 
                    THEN ' AND c.name LIKE @RoleName' 
                    ELSE '' 
                  END + '
            )
            SELECT DISTINCT 
                DBName,
                UserName,
                sid,
                RoleName = SUBSTRING(
                    (SELECT ('', '' + RoleName)
                     FROM cte b
                     WHERE a.UserName = b.UserName
                     FOR XML PATH ('''')
                    ), 3, 8000
                ),
                createdate,
                updatedate 
            FROM cte a
        END';

        IF @Debug = 1
            PRINT @SqlCmd;

        -- Execute for each database
        INSERT INTO #temprole
        EXEC sp_MSforeachdb @SqlCmd;

        -- Handle orphaned users
        CREATE TABLE #DBUsers (
            DBName varchar(255),
            Username varchar(255)
        );

        DECLARE @OrphanSql NVARCHAR(MAX) = N'';
        
        SELECT @OrphanSql = @OrphanSql + 
            'SELECT ' + QUOTENAME([name], '''') + ' COLLATE Latin1_General_CI_AS AS [DBName], 
                    u.[name] COLLATE Latin1_General_CI_AS ' +
            'FROM ' + QUOTENAME([name]) + '.dbo.sysusers u ' +
            'WHERE u.issqluser = 1 ' +
            'AND (u.sid is not null AND u.sid <> 0x0) ' +
            'AND NOT EXISTS (SELECT 1 FROM master.dbo.syslogins l WHERE l.sid = u.sid) '
            + CASE 
                WHEN @LoginName IS NOT NULL 
                THEN ' AND u.name LIKE @LoginName' 
                ELSE '' 
              END + '
            UNION ALL '
        FROM master.dbo.sysdatabases sd
        WHERE HAS_DBACCESS(sd.name) = 1
        AND (@DatabaseName IS NULL OR sd.name = @DatabaseName)
        AND (@IncludeSystemDatabases = 1 OR sd.name NOT IN ('master', 'model', 'msdb', 'tempdb'));

        SET @OrphanSql = LEFT(@OrphanSql, LEN(@OrphanSql) - 9);

        IF @Debug = 1
            PRINT @OrphanSql;

        IF @ShowOnlyOrphanedUsers = 1
        BEGIN
            INSERT INTO #DBUsers
            EXEC sp_executesql @OrphanSql, 
                N'@LoginName NVARCHAR(128)', 
                @LoginName;
        END

        -- Final results table
        CREATE TABLE #tmpDatabaseRoles (
            ServerName nvarchar(128),
            DBName varchar(128),
            UserName varchar(128),
            LoginSID varbinary(128),
            RoleName varchar(2000),
            CreateDate datetime,
            UpdateDate datetime,
            type_desc nvarchar(60)
        );

        -- Populate final results
        INSERT INTO #tmpDatabaseRoles
        SELECT 
            @@SERVERNAME,
            t.*,
            sp.type_desc
        FROM #temprole t
        LEFT JOIN #DBUsers d ON t.DBName = d.DBName AND t.UserName = d.Username
        INNER JOIN sys.server_principals sp ON sp.sid = t.LoginSID
        WHERE t.UserName NOT LIKE 'dbo'
        AND t.UserName NOT LIKE '##%'
        AND (@ExcludeWindowsGroups = 0 OR sp.type_desc != 'WINDOWS_GROUP');

        -- Handle Windows Groups if needed
        IF @ExcludeWindowsGroups = 0
        BEGIN
            DECLARE @lname varchar(256);
            
            CREATE TABLE #tmpGroupDetail (
                AccountName varchar(256),
                Type varchar(128),
                Privilege varchar(128),
                Mapped_Login_Name varchar(256),
                Permission_Path varchar(256)
            );

            DECLARE Roles CURSOR FOR
                SELECT UserName
                FROM #tmpDatabaseRoles
                WHERE type_desc = 'WINDOWS_GROUP';

            OPEN Roles;
            FETCH NEXT FROM Roles INTO @lname;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                INSERT INTO #tmpGroupDetail
                EXEC xp_logininfo @lname, 'members';
                FETCH NEXT FROM Roles INTO @lname;
            END

            -- Final result set with group details
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
            LEFT JOIN #tmpGroupDetail B ON a.UserName = b.Permission_Path
            ORDER BY A.type_desc, A.DBName, A.UserName;

            CLOSE Roles;
            DEALLOCATE Roles;
        END
        ELSE
        BEGIN
            -- Final result set without group details
            SELECT
                ServerName,
                DBName,
                UserName AS LoginName,
                'Individual Login' AS GroupMembers,
                RoleName AS DatabaseRoles,
                CreateDate,
                UpdateDate,
                type_desc,
				GETUTCDATE() ReportRunUTC
            FROM #tmpDatabaseRoles
            ORDER BY type_desc, DBName, UserName;
        END

    END TRY
    BEGIN CATCH
        SELECT
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() AS ErrorState,
            ERROR_PROCEDURE() AS ErrorProcedure,
            ERROR_LINE() AS ErrorLine,
            ERROR_MESSAGE() AS ErrorMessage;

        -- Simplified result set in case of error
        SELECT
            A.ServerName,
            A.DBName,
            A.UserName AS LoginName,
            CASE
                WHEN type_desc = 'WINDOWS_GROUP' THEN 'Not able to populate group members'
                ELSE 'Individual Login'
            END AS GroupMembers,
            A.RoleName AS DatabaseRole,
            A.CreateDate,
            A.UpdateDate,
            A.type_desc
        FROM #tmpDatabaseRoles A
        ORDER BY type_desc;
    END CATCH;

    -- Cleanup
    DROP TABLE IF EXISTS #temprole;
    DROP TABLE IF EXISTS #DBUsers;
    DROP TABLE IF EXISTS #tmpDatabaseRoles;
    DROP TABLE IF EXISTS #tmpGroupDetail;
END;
GO

-- Example usage:
-- Get all database roles
 EXEC dbo.sp_GetDatabaseRoles;

-- Get roles for specific database
-- EXEC dbo.sp_GetDatabaseRoles @DatabaseName = 'YourDatabase';

-- Get roles for specific login
-- EXEC dbo.sp_GetDatabaseRoles @LoginName = 'YourLogin';

-- Get roles excluding Windows Groups
-- EXEC dbo.sp_GetDatabaseRoles @ExcludeWindowsGroups = 1;

-- Get only orphaned users
-- EXEC dbo.sp_GetDatabaseRoles @ShowOnlyOrphanedUsers = 1;

-- Get roles with debug information
-- EXEC dbo.sp_GetDatabaseRoles @Debug = 1;
