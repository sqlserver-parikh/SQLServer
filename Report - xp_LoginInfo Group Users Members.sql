CREATE OR ALTER PROCEDURE dbo.usp_FindUserInGroups
    -- The user name pattern to search for. Accepts wildcards like %, _, [].
    @UserNamePattern sysname ='%User%'
AS
BEGIN
    SET NOCOUNT ON;

    -- Setup Temp Tables --
    CREATE TABLE #MembershipResults (
        FoundUser               sysname,
        PermissionGrantingRole  sysname,
        MembershipSource        sysname,
        MembershipType          VARCHAR(64),
        Context                 sysname
    );
    CREATE TABLE #CurrentGroupMembers (
        account_name    sysname, type char(8), privilege char(8),
        mapped_login    sysname, permission_path sysname NULL
    );

    PRINT 'Searching for users matching pattern: ''' + @UserNamePattern + '''';

    BEGIN TRY
        -- =================================================================================
        -- 1. CHECK SERVER-LEVEL GROUPS
        -- =================================================================================
        PRINT '--- Checking Server-Level AD Groups ---';
        DECLARE @ServerGroupName sysname;
        DECLARE group_cursor CURSOR FOR
        SELECT name FROM sys.server_principals WHERE type = 'G' AND is_disabled = 0;

        OPEN group_cursor;
        FETCH NEXT FROM group_cursor INTO @ServerGroupName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            TRUNCATE TABLE #CurrentGroupMembers;
            BEGIN TRY
                INSERT INTO #CurrentGroupMembers EXEC xp_logininfo @ServerGroupName, 'members';

                IF EXISTS (SELECT 1 FROM #CurrentGroupMembers WHERE account_name LIKE @UserNamePattern)
                BEGIN
                    PRINT 'Found matching user(s) in server group: ' + @ServerGroupName;
                    INSERT INTO #MembershipResults (FoundUser, PermissionGrantingRole, MembershipSource, MembershipType, Context)
                    SELECT cm.account_name, @ServerGroupName, cm.permission_path, 'SERVER AD GROUP', 'SERVER LEVEL'
                    FROM #CurrentGroupMembers AS cm WHERE cm.account_name LIKE @UserNamePattern;
                END
            END TRY
            BEGIN CATCH
                PRINT 'Warning: Could not expand members for group ''' + ISNULL(@ServerGroupName, 'UNKNOWN') + '''. Error: ' + ERROR_MESSAGE();
            END CATCH
            FETCH NEXT FROM group_cursor INTO @ServerGroupName;
        END
        CLOSE group_cursor; DEALLOCATE group_cursor;

        -- =================================================================================
        -- 2. CHECK DATABASE-LEVEL MEMBERSHIPS
        -- =================================================================================
        PRINT '--- Checking Database-Level Roles ---';
        DECLARE @DBName sysname, @DynamicSQL NVARCHAR(MAX);
        DECLARE db_cursor CURSOR FOR
        SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' AND HAS_DBACCESS(name) = 1;

        OPEN db_cursor;
        FETCH NEXT FROM db_cursor INTO @DBName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @DynamicSQL = N'
                USE ' + QUOTENAME(@DBName) + N';

                -- A) Direct user membership in a database role
                INSERT INTO #MembershipResults (FoundUser, PermissionGrantingRole, MembershipSource, MembershipType, Context)
                SELECT user_p.name, role_p.name, user_p.name, ''DATABASE ROLE (Direct User)'', DB_NAME()
                FROM sys.database_role_members AS rm
                JOIN sys.database_principals AS role_p ON rm.role_principal_id = role_p.principal_id
                JOIN sys.database_principals AS user_p ON rm.member_principal_id = user_p.principal_id
                WHERE user_p.name LIKE @pUserNamePattern;

                -- B) Database roles granted to the AD groups we already found
                INSERT INTO #MembershipResults (FoundUser, PermissionGrantingRole, MembershipSource, MembershipType, Context)
                SELECT DISTINCT m_res.FoundUser, role_p.name, group_p.name, ''DATABASE ROLE (via Group)'', DB_NAME()
                FROM sys.database_role_members AS rm
                JOIN sys.database_principals AS role_p ON rm.role_principal_id = role_p.principal_id
                JOIN sys.database_principals AS group_p ON rm.member_principal_id = group_p.principal_id
                JOIN #MembershipResults AS m_res
                    --  *** THIS IS THE FIX FOR THE COLLATION CONFLICT ***
                    ON group_p.name = m_res.PermissionGrantingRole COLLATE DATABASE_DEFAULT
                    AND m_res.Context = ''SERVER LEVEL''
                WHERE group_p.type = ''G'';';

            EXEC sp_executesql @DynamicSQL, N'@pUserNamePattern sysname', @pUserNamePattern = @UserNamePattern;
            FETCH NEXT FROM db_cursor INTO @DBName;
        END
        CLOSE db_cursor; DEALLOCATE db_cursor;

        -- =================================================================================
        -- 3. RETURN RESULTS
        -- =================================================================================
        IF NOT EXISTS (SELECT 1 FROM #MembershipResults)
        BEGIN
            SELECT 'No users matching pattern ''' + @UserNamePattern + ''' found in any groups.' AS Result, 'N/A' AS Context;
        END
        ELSE
        BEGIN
            SELECT DISTINCT FoundUser, PermissionGrantingRole, MembershipSource, MembershipType, Context
            FROM #MembershipResults
            ORDER BY Context, FoundUser, PermissionGrantingRole;
        END
    END TRY
    BEGIN CATCH
        PRINT 'An unexpected error occurred:';
        PRINT ERROR_MESSAGE();
        IF CURSOR_STATUS('global', 'group_cursor') >= 0 BEGIN CLOSE group_cursor; DEALLOCATE group_cursor; END
        IF CURSOR_STATUS('global', 'db_cursor') >= 0 BEGIN CLOSE db_cursor; DEALLOCATE db_cursor; END
        IF OBJECT_ID('tempdb..#MembershipResults') IS NOT NULL DROP TABLE #MembershipResults;
        IF OBJECT_ID('tempdb..#CurrentGroupMembers') IS NOT NULL DROP TABLE #CurrentGroupMembers;
        THROW;
    END CATCH;

    IF OBJECT_ID('tempdb..#MembershipResults') IS NOT NULL DROP TABLE #MembershipResults;
    IF OBJECT_ID('tempdb..#CurrentGroupMembers') IS NOT NULL DROP TABLE #CurrentGroupMembers;
END
GO
