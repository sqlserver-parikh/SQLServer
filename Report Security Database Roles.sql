USE [master];
GO

CREATE OR ALTER PROCEDURE dbo.usp_ReportLoginUserRoleInventory
    @ReportType VARCHAR(10) = 'ALL',               
    @OutputMode VARCHAR(10) = 'MULTI',             
    @Databases NVARCHAR(MAX) = 'USER_DATABASES',   
    @LoginName NVARCHAR(128) = NULL,               
    @RoleName NVARCHAR(128) = NULL,                
    @ExactMatch BIT = 0,                           
    @ExpandWindowsGroups BIT = 0,                  
    @ShowOnlyOrphanedUsers BIT = 0,                
    @Debug BIT = 0                                 
AS
/*
----------------------------------------------------------------------------------------------------
-- OBJECT NAME:        dbo.usp_ReportLoginUserRoleInventory
--
-- DESCRIPTION:        Inventories Server and Database principals, user mappings, explicit role 
--                     memberships, and flags potentially orphaned users. 
--
--                     NOTE: This procedure maps principals and roles. It does NOT inventory 
--                     granular explicit permissions (e.g., GRANT SELECT, DENY CONTROL, Ownership).
--
-- REQUIRED PERMISSIONS:
--                     - VIEW ANY DEFINITION
--                     - VIEW SERVER SECURITY DEFINITION (SQL Server 2022+)
--                     - ALTER ANY LOGIN or securityadmin/sysadmin for complete login visibility
--                     - Access to each target database
--                     - Permission to execute xp_logininfo when @ExpandWindowsGroups = 1
--
-- PARAMETERS:         
--   @ReportType       (VARCHAR)  Determines the scope of the inventory.
--                                'ALL'      : (Default) Both Server and Database levels.
--                                'SERVER'   : Server principals and roles only.
--                                'DATABASE' : Database principals and roles only.
--
--   @OutputMode       (VARCHAR)  Determines the shape of the result sets.
--                                'MULTI'    : (Default) Returns multiple normalized, human-readable grids.
--                                'FLAT'     : Returns a single, wide, denormalized grid for CSV/Excel/BI export.
--
--   @Databases        (NVARCHAR) Basic Ola-style database filter syntax.
--                                Supported: SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES.
--                                Exclusions (-), Wildcards (%), and comma-separated lists.
--                                E.g., 'USER_DATABASES, -ReportServer', '%Client%, Db1'
--
--   @LoginName        (NVARCHAR) Filter by a specific login or user name.
--
--   @RoleName         (NVARCHAR) Filter to include only principals that possess this specific 
--                                server or database role. (Output will still list all of their roles).
--
--   @ExactMatch       (BIT)      0: (Default) Uses LIKE '%Value%' for @LoginName and @RoleName.
--                                1: Uses exact string matching.
--
--   @ExpandWindowsGroups (BIT)   0: (Default) Skips AD expansion.
--                                1: Uses xp_logininfo to expand AD group members. (Warning: Can be slow).
--
--   @ShowOnlyOrphanedUsers (BIT) 0: (Default) Shows all users.
--                                1: Shows ONLY users mapped to a database without a matching Server SID.
--                                (Excludes DATABASE auth types and Entra ID/External users).
--
--   @Debug            (BIT)      1: Outputs the dynamic SQL being executed into the result sets.
--
-- VERSION HISTORY:
--   0.9  | 2026-07-09 | Release Candidate. Fixed collation, role aggregation, and orphan false positives.
----------------------------------------------------------------------------------------------------
*/
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2 = SYSDATETIME();

    ---------------------------------------------------------------------------
    -- 1. Input Validation
    ---------------------------------------------------------------------------
    SET @ReportType = UPPER(LTRIM(RTRIM(@ReportType)));
    SET @OutputMode = UPPER(LTRIM(RTRIM(@OutputMode)));

    IF @ReportType NOT IN ('ALL', 'SERVER', 'DATABASE')
    BEGIN
        RAISERROR('Invalid @ReportType. Valid values are ALL, SERVER, DATABASE.', 16, 1);
        RETURN;
    END;

    IF @OutputMode NOT IN ('MULTI', 'FLAT')
    BEGIN
        RAISERROR('Invalid @OutputMode. Valid values are MULTI, FLAT.', 16, 1);
        RETURN;
    END;

    ---------------------------------------------------------------------------
    -- 2. Setup Temporary Tables
    ---------------------------------------------------------------------------
    DROP TABLE IF EXISTS #tmpServerPrincipals;
    DROP TABLE IF EXISTS #tmpDatabaseRoles;
    DROP TABLE IF EXISTS #tmpGroupDetail;
    DROP TABLE IF EXISTS #tmpWarnings;
    DROP TABLE IF EXISTS #SelectedDatabases;

    CREATE TABLE #tmpWarnings (
        WarningID INT IDENTITY(1,1),
        WarningTime DATETIME2 DEFAULT SYSDATETIME(),
        ScopeType VARCHAR(30),
        ScopeName SYSNAME NULL,
        ErrorNumber INT NULL,
        ErrorMessage NVARCHAR(MAX)
    );

    CREATE TABLE #SelectedDatabases (
        DatabaseName SYSNAME NOT NULL PRIMARY KEY
    );

    CREATE TABLE #tmpServerPrincipals (
        ServerName SYSNAME,
        PrincipalName SYSNAME,
        PrincipalSID VARBINARY(85),
        TypeDesc NVARCHAR(60),
        ServerRoles NVARCHAR(MAX),
        CreateDate DATETIME,
        ModifyDate DATETIME,
        PasswordLastSetTime DATETIME,
        DaysSincePasswordSet INT,
        IsDisabled BIT,
        DaysUntilExpiration INT,
        IsExpired INT,
        IsMustChange INT,
        PolicyChecked VARCHAR(3),
        ExpirationChecked VARCHAR(3)
    );

    CREATE TABLE #tmpDatabaseRoles (
        ServerName SYSNAME,
        DBName SYSNAME,
        UserName SYSNAME,
        LoginSID VARBINARY(85),
        UserTypeCode CHAR(1),
        UserTypeDesc NVARCHAR(60),
        AuthTypeDesc NVARCHAR(60),
        DatabaseRoles NVARCHAR(MAX),
        CreateDate DATETIME,
        UpdateDate DATETIME,
        IsPotentiallyOrphaned BIT DEFAULT 0,
        OrphanReason NVARCHAR(128) NULL
    );

    CREATE TABLE #tmpGroupDetail (
        AccountName SYSNAME,
        Type VARCHAR(128),
        Privilege VARCHAR(128),
        Mapped_Login_Name SYSNAME,
        Permission_Path SYSNAME
    );

    ---------------------------------------------------------------------------
    -- 3. Parse Ola-Style Basic @Databases Filter
    ---------------------------------------------------------------------------
    DECLARE
        @DatabaseToken NVARCHAR(4000),
        @DatabaseTokenList NVARCHAR(MAX),
        @CommaPosition INT,
        @IsExclusion BIT;

    SET @Databases = NULLIF(LTRIM(RTRIM(@Databases)), '');
    IF @Databases IS NULL SET @Databases = 'USER_DATABASES';
    SET @DatabaseTokenList = @Databases + N',';

    WHILE LEN(@DatabaseTokenList) > 0
    BEGIN
        SET @CommaPosition = CHARINDEX(',', @DatabaseTokenList);
        SET @DatabaseToken = LTRIM(RTRIM(SUBSTRING(@DatabaseTokenList, 1, @CommaPosition - 1)));
        SET @DatabaseTokenList = SUBSTRING(@DatabaseTokenList, @CommaPosition + 1, LEN(@DatabaseTokenList));

        IF @DatabaseToken = '' CONTINUE;

        SET @IsExclusion = 0;
        IF LEFT(@DatabaseToken, 1) = '-'
        BEGIN
            SET @IsExclusion = 1;
            SET @DatabaseToken = LTRIM(RTRIM(SUBSTRING(@DatabaseToken, 2, LEN(@DatabaseToken))));
        END;

        IF LEFT(@DatabaseToken, 1) = '[' AND RIGHT(@DatabaseToken, 1) = ']'
            SET @DatabaseToken = SUBSTRING(@DatabaseToken, 2, LEN(@DatabaseToken) - 2);

        IF @IsExclusion = 0
        BEGIN
            IF @DatabaseToken = 'SYSTEM_DATABASES'
                INSERT INTO #SelectedDatabases (DatabaseName)
                SELECT name FROM sys.databases WHERE database_id BETWEEN 1 AND 4
                AND NOT EXISTS (SELECT 1 FROM #SelectedDatabases s WHERE s.DatabaseName = sys.databases.name);
            ELSE IF @DatabaseToken = 'USER_DATABASES'
                INSERT INTO #SelectedDatabases (DatabaseName)
                SELECT name FROM sys.databases WHERE database_id > 4
                AND NOT EXISTS (SELECT 1 FROM #SelectedDatabases s WHERE s.DatabaseName = sys.databases.name);
            ELSE IF @DatabaseToken = 'ALL_DATABASES'
                INSERT INTO #SelectedDatabases (DatabaseName)
                SELECT name FROM sys.databases 
                WHERE NOT EXISTS (SELECT 1 FROM #SelectedDatabases s WHERE s.DatabaseName = sys.databases.name);
            ELSE
                INSERT INTO #SelectedDatabases (DatabaseName)
                SELECT name FROM sys.databases WHERE name LIKE @DatabaseToken
                AND NOT EXISTS (SELECT 1 FROM #SelectedDatabases s WHERE s.DatabaseName = sys.databases.name);
        END
        ELSE
        BEGIN
            IF @DatabaseToken = 'SYSTEM_DATABASES'
                DELETE s FROM #SelectedDatabases s INNER JOIN sys.databases d ON s.DatabaseName = d.name WHERE d.database_id BETWEEN 1 AND 4;
            ELSE IF @DatabaseToken = 'USER_DATABASES'
                DELETE s FROM #SelectedDatabases s INNER JOIN sys.databases d ON s.DatabaseName = d.name WHERE d.database_id > 4;
            ELSE IF @DatabaseToken = 'ALL_DATABASES'
                DELETE FROM #SelectedDatabases;
            ELSE
                DELETE FROM #SelectedDatabases WHERE DatabaseName LIKE @DatabaseToken;
        END
    END;

    IF NOT EXISTS (SELECT 1 FROM #SelectedDatabases)
    BEGIN
        INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorMessage)
        VALUES ('DatabaseSelection', @Databases, 'No databases matched @Databases selection.');
    END

    ---------------------------------------------------------------------------
    -- 4. Gather Server-Level Information
    ---------------------------------------------------------------------------
    IF @ReportType IN ('ALL', 'SERVER') AND @ShowOnlyOrphanedUsers = 0
    BEGIN
        BEGIN TRY
            WITH cte AS (
                SELECT 
                    b.name AS LoginName,
                    b.sid AS LoginSID,
                    c.name AS ServerRole,
                    b.type_desc,
                    b.create_date AS CreateDate,
                    b.modify_date AS ModifyDate,
                    b.is_disabled AS Disabled,
                    CASE 
                        WHEN b.type_desc <> 'SQL_LOGIN' THEN 'N/A'
                        WHEN d.is_policy_checked = 1 THEN 'Yes' ELSE 'No' 
                    END AS PolicyChecked,
                    CASE 
                        WHEN b.type_desc <> 'SQL_LOGIN' THEN 'N/A'
                        WHEN d.is_expiration_checked = 1 THEN 'Yes' ELSE 'No' 
                    END AS ExpirationChecked
                FROM sys.server_principals b
                LEFT JOIN sys.server_role_members a ON a.member_principal_id = b.principal_id
                LEFT JOIN sys.server_principals c ON a.role_principal_id = c.principal_id
                LEFT JOIN sys.sql_logins d ON b.name = d.name
                WHERE b.type_desc NOT IN ('SERVER_ROLE', 'CERTIFICATE_MAPPED_LOGIN')
                  AND b.name NOT LIKE '##%'
                  AND b.name NOT LIKE 'NT %'
                  AND (
                      @LoginName IS NULL 
                      OR (@ExactMatch = 1 AND b.name = @LoginName)
                      OR (@ExactMatch = 0 AND b.name LIKE '%' + @LoginName + '%')
                  )
                  -- Filter using EXISTS so we still capture all roles for the matched principal
                  AND (
                      @RoleName IS NULL 
                      OR EXISTS (
                          SELECT 1 FROM sys.server_role_members rm2
                          JOIN sys.server_principals r2 ON rm2.role_principal_id = r2.principal_id
                          WHERE rm2.member_principal_id = b.principal_id
                            AND (
                                (@ExactMatch = 1 AND r2.name = @RoleName)
                                OR (@ExactMatch = 0 AND r2.name LIKE '%' + @RoleName + '%')
                            )
                      )
                  )
            )
            INSERT INTO #tmpServerPrincipals
            SELECT 
                @@SERVERNAME AS ServerName,
                LoginName,
                LoginSID,
                type_desc,
                ServerRoles = STUFF((
                    SELECT DISTINCT ', ' + b.ServerRole
                    FROM cte b
                    WHERE a.LoginSID = b.LoginSID AND b.ServerRole IS NOT NULL
                    FOR XML PATH(''), TYPE
                ).value('.', 'NVARCHAR(MAX)'), 1, 2, ''),
                CreateDate,
                ModifyDate,
                CASE WHEN type_desc = 'SQL_LOGIN' THEN CONVERT(DATETIME, LOGINPROPERTY(LoginName, 'PasswordLastSetTime')) ELSE NULL END,
                CASE WHEN type_desc = 'SQL_LOGIN' THEN DATEDIFF(DAY, CONVERT(DATETIME, LOGINPROPERTY(LoginName, 'PasswordLastSetTime')), GETDATE()) ELSE NULL END,
                Disabled,
                CASE WHEN type_desc = 'SQL_LOGIN' THEN CONVERT(INT, LOGINPROPERTY(LoginName, 'DaysUntilExpiration')) ELSE NULL END,
                CASE WHEN type_desc = 'SQL_LOGIN' THEN CONVERT(INT, LOGINPROPERTY(LoginName, 'IsExpired')) ELSE NULL END,
                CASE WHEN type_desc = 'SQL_LOGIN' THEN CONVERT(INT, LOGINPROPERTY(LoginName, 'IsMustChange')) ELSE NULL END,
                PolicyChecked,
                ExpirationChecked 
            FROM cte a
            GROUP BY LoginName, LoginSID, type_desc, CreateDate, ModifyDate, Disabled, PolicyChecked, ExpirationChecked;
        END TRY
        BEGIN CATCH
            INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorNumber, ErrorMessage)
            VALUES ('Server', @@SERVERNAME, ERROR_NUMBER(), ERROR_MESSAGE());
        END CATCH
    END

    ---------------------------------------------------------------------------
    -- 5. Gather Database-Level Information
    ---------------------------------------------------------------------------
    IF @ReportType IN ('ALL', 'DATABASE')
    BEGIN
        INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorMessage)
        SELECT 'DatabaseSelection', s.DatabaseName, 'Skipped: Offline or Inaccessible'
        FROM #SelectedDatabases s
        JOIN sys.databases d ON s.DatabaseName = d.name
        WHERE d.state_desc <> 'ONLINE' OR HAS_DBACCESS(d.name) = 0;

        DELETE s
        FROM #SelectedDatabases s
        JOIN sys.databases d ON s.DatabaseName = d.name
        WHERE d.state_desc <> 'ONLINE' OR HAS_DBACCESS(d.name) = 0;

        BEGIN TRY 
            DECLARE @db_name SYSNAME;
            DECLARE @SqlCmd NVARCHAR(MAX);

            DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR 
                SELECT DatabaseName 
                FROM #SelectedDatabases
                ORDER BY DatabaseName;

            OPEN db_cursor;
            FETCH NEXT FROM db_cursor INTO @db_name;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @SqlCmd = N'
                    USE ' + QUOTENAME(@db_name) + N';

                    WITH DBRoles AS (
                        SELECT 
                            u.name AS UserName,
                            u.sid AS LoginSID,
                            r.name AS RoleName,
                            u.create_date AS CreateDate,
                            u.modify_date AS UpdateDate,
                            u.type AS UserTypeCode,
                            u.type_desc AS UserTypeDesc,
                            u.authentication_type_desc AS AuthTypeDesc
                        FROM sys.database_principals u
                        LEFT JOIN sys.database_role_members rm ON u.principal_id = rm.member_principal_id
                        LEFT JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                        -- Includes S (SQL User), U (Windows User), G (Windows Group), E (External User), X (External Group)
                        WHERE u.type IN (''S'', ''U'', ''G'', ''E'', ''X'') 
                          AND u.principal_id > 4 
                          AND (
                              @pLoginName IS NULL 
                              OR (@pExactMatch = 1 AND u.name COLLATE DATABASE_DEFAULT = @pLoginName COLLATE DATABASE_DEFAULT)
                              OR (@pExactMatch = 0 AND u.name COLLATE DATABASE_DEFAULT LIKE N''%'' + @pLoginName COLLATE DATABASE_DEFAULT + N''%'')
                          )
                          -- Filter using EXISTS to capture all roles for the matching principal
                          AND (
                              @pRoleName IS NULL 
                              OR EXISTS (
                                  SELECT 1 FROM sys.database_role_members rm2
                                  JOIN sys.database_principals r2 ON rm2.role_principal_id = r2.principal_id
                                  WHERE rm2.member_principal_id = u.principal_id
                                    AND (
                                        (@pExactMatch = 1 AND r2.name COLLATE DATABASE_DEFAULT = @pRoleName COLLATE DATABASE_DEFAULT)
                                        OR (@pExactMatch = 0 AND r2.name COLLATE DATABASE_DEFAULT LIKE N''%'' + @pRoleName COLLATE DATABASE_DEFAULT + N''%'')
                                    )
                              )
                          )
                    )
                    INSERT INTO #tmpDatabaseRoles (ServerName, DBName, UserName, LoginSID, DatabaseRoles, CreateDate, UpdateDate, UserTypeCode, UserTypeDesc, AuthTypeDesc)
                    SELECT
                        @@SERVERNAME,
                        DB_NAME(),
                        a.UserName,
                        a.LoginSID,
                        STUFF((
                            SELECT DISTINCT '', '' + b.RoleName
                            FROM DBRoles b
                            WHERE a.UserName = b.UserName AND b.RoleName IS NOT NULL
                            FOR XML PATH(''''), TYPE
                        ).value(''.'', ''NVARCHAR(MAX)''), 1, 2, ''''),
                        a.CreateDate,
                        a.UpdateDate,
                        a.UserTypeCode,
                        a.UserTypeDesc,
                        a.AuthTypeDesc
                    FROM DBRoles a
                    GROUP BY a.UserName, a.LoginSID, a.CreateDate, a.UpdateDate, a.UserTypeCode, a.UserTypeDesc, a.AuthTypeDesc;';
                    
                    IF @Debug = 1 SELECT @SqlCmd AS DebugSql_DB_Context;

                    EXEC sp_executesql @SqlCmd, 
                                       N'@pLoginName NVARCHAR(128), @pRoleName NVARCHAR(128), @pExactMatch BIT', 
                                       @pLoginName = @LoginName, @pRoleName = @RoleName, @pExactMatch = @ExactMatch;
                END TRY
                BEGIN CATCH
                    INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorNumber, ErrorMessage)
                    VALUES ('DatabaseScan', @db_name, ERROR_NUMBER(), ERROR_MESSAGE());
                END CATCH

                FETCH NEXT FROM db_cursor INTO @db_name;
            END
        END TRY
        BEGIN CATCH
            INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorNumber, ErrorMessage)
            VALUES ('DatabaseCursorOuter', @@SERVERNAME, ERROR_NUMBER(), ERROR_MESSAGE());
        END CATCH
        
        IF CURSOR_STATUS('local', 'db_cursor') >= 0
        BEGIN
            CLOSE db_cursor;
            DEALLOCATE db_cursor;
        END

        -- Flag Potentially Orphaned Users
        UPDATE d
        SET IsPotentiallyOrphaned = 1,
            OrphanReason = 'Missing Server Login (SID mismatch/missing)'
        FROM #tmpDatabaseRoles d
        WHERE d.LoginSID IS NOT NULL 
          AND d.LoginSID <> 0x01
          AND d.AuthTypeDesc <> 'DATABASE' 
          AND d.UserTypeCode NOT IN ('E', 'X') -- Exclude Entra ID / External
          AND NOT EXISTS (SELECT 1 FROM sys.server_principals s WHERE s.sid = d.LoginSID);
    END

    ---------------------------------------------------------------------------
    -- 6. Expand Windows Group Members
    ---------------------------------------------------------------------------
    IF @ExpandWindowsGroups = 1
    BEGIN
        BEGIN TRY 
            DECLARE @lname SYSNAME;
            
            DECLARE Roles CURSOR LOCAL FAST_FORWARD FOR
                SELECT DISTINCT PrincipalName FROM #tmpServerPrincipals WHERE TypeDesc = 'WINDOWS_GROUP'
                UNION
                SELECT DISTINCT UserName FROM #tmpDatabaseRoles WHERE UserTypeCode = 'G';

            OPEN Roles;
            FETCH NEXT FROM Roles INTO @lname;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    INSERT INTO #tmpGroupDetail (AccountName, Type, Privilege, Mapped_Login_Name, Permission_Path)
                    EXEC xp_logininfo @lname, 'members';
                END TRY
                BEGIN CATCH
                    INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorNumber, ErrorMessage)
                    VALUES ('GroupExpansion', @lname, ERROR_NUMBER(), ERROR_MESSAGE());
                END CATCH
                FETCH NEXT FROM Roles INTO @lname;
            END
        END TRY
        BEGIN CATCH
            INSERT INTO #tmpWarnings (ScopeType, ScopeName, ErrorNumber, ErrorMessage)
            VALUES ('GroupCursorOuter', @@SERVERNAME, ERROR_NUMBER(), ERROR_MESSAGE());
        END CATCH

        IF CURSOR_STATUS('local', 'Roles') >= 0
        BEGIN
            CLOSE Roles;
            DEALLOCATE Roles;
        END
    END

    ---------------------------------------------------------------------------
    -- 7. Final Result Sets
    ---------------------------------------------------------------------------
    DECLARE @EndTime DATETIME2 = SYSDATETIME();

    IF @OutputMode = 'MULTI'
    BEGIN
        IF @ReportType IN ('ALL', 'SERVER') AND EXISTS (SELECT 1 FROM #tmpServerPrincipals)
            SELECT 'Server Principals' AS ResultSet, * FROM #tmpServerPrincipals ORDER BY PrincipalName;

        IF @ReportType IN ('ALL', 'DATABASE') AND EXISTS (SELECT 1 FROM #tmpDatabaseRoles)
            SELECT 'Database Principals' AS ResultSet, * FROM #tmpDatabaseRoles 
            WHERE (@ShowOnlyOrphanedUsers = 0 OR IsPotentiallyOrphaned = 1)
            ORDER BY DBName, UserName;

        IF @ExpandWindowsGroups = 1 AND EXISTS (SELECT 1 FROM #tmpGroupDetail)
            SELECT 'AD Group Members' AS ResultSet, * FROM #tmpGroupDetail ORDER BY Permission_Path, AccountName;

        IF EXISTS (SELECT 1 FROM #tmpWarnings)
            SELECT 'Warnings/Errors' AS ResultSet, * FROM #tmpWarnings ORDER BY WarningTime DESC;

        SELECT 
            'Audit Metadata' AS ResultSet,
            @StartTime AS ReportStartTime,
            @EndTime AS ReportEndTime,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS ElapsedSeconds,
            ORIGINAL_LOGIN() AS ReportRunBy,
            @@SERVERNAME AS ServerName,
            SERVERPROPERTY('ProductVersion') AS ProductVersion,
            @OutputMode AS Parameters_OutputMode,
            @Databases AS Parameters_Databases,
            @ExpandWindowsGroups AS Parameters_ADExpanded;
    END
    ELSE IF @OutputMode = 'FLAT'
    BEGIN
        SELECT 
            COALESCE(s.ServerName, d.ServerName) AS ServerName,
            ISNULL(d.DBName, 'N/A (Server Level)') AS DBName,
            s.PrincipalName AS ServerPrincipalName,
            d.UserName AS DatabaseUserName,
            ISNULL(g.AccountName, 'Directly Assigned') AS ExpandedGroupMemberAccountName,
            ISNULL(s.TypeDesc, d.UserTypeDesc) AS PrincipalType,
            d.AuthTypeDesc AS DBAuthType,
            ISNULL(s.ServerRoles, 'None') AS ServerRoles,
            ISNULL(NULLIF(d.DatabaseRoles, ''), 'public only') AS DatabaseRoles,
            ISNULL(d.IsPotentiallyOrphaned, 0) AS IsPotentiallyOrphaned,
            d.OrphanReason,
            s.IsDisabled,
            s.DaysSincePasswordSet,
            s.DaysUntilExpiration
        FROM #tmpServerPrincipals s
        FULL OUTER JOIN #tmpDatabaseRoles d ON s.PrincipalSID = d.LoginSID
        LEFT JOIN #tmpGroupDetail g ON COALESCE(s.PrincipalName, d.UserName) = g.Permission_Path
        WHERE (@ShowOnlyOrphanedUsers = 0 OR d.IsPotentiallyOrphaned = 1)
        ORDER BY DBName, ServerPrincipalName, DatabaseUserName, ExpandedGroupMemberAccountName;

        IF EXISTS (SELECT 1 FROM #tmpWarnings)
            SELECT 'Warnings/Errors' AS ResultSet, * FROM #tmpWarnings ORDER BY WarningTime DESC;
    END

    ---------------------------------------------------------------------------
    -- Cleanup
    ---------------------------------------------------------------------------
    DROP TABLE IF EXISTS #tmpServerPrincipals;
    DROP TABLE IF EXISTS #tmpDatabaseRoles;
    DROP TABLE IF EXISTS #tmpGroupDetail;
    DROP TABLE IF EXISTS #tmpWarnings;
    DROP TABLE IF EXISTS #SelectedDatabases;
END;
GO
