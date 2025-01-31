USE tempdb
GO
CREATE OR ALTER PROCEDURE sp_GetDetailedLoginInfo
    @FilterLoginName VARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- First CTE to get server role information
    WITH cte AS (
        SELECT
            b.name LoginName,
            ISNULL(c.name, 'Public') ServerRole,
            b.type_desc,
            b.create_date CreateDate,
            b.modify_date ModifyDate,
            b.is_disabled Disabled,
            CASE WHEN d.is_policy_checked = 1 THEN 'Yes' ELSE 'No' END PolicyChecked,
            CASE WHEN d.is_expiration_checked = 1 THEN 'Yes' ELSE 'No' END ExpirationChecked
        FROM sys.server_role_members a
        RIGHT JOIN sys.server_principals b ON a.member_principal_id = b.principal_id
        LEFT JOIN sys.server_principals c ON a.role_principal_id = c.principal_id
        LEFT JOIN sys.sql_logins d ON b.name = d.name
    )
    -- Create temp table for server roles
    SELECT DISTINCT
        @@SERVERNAME ServerName,
        LoginName,
        ServerRole = SUBSTRING((
            SELECT (', ' + ServerRole)
            FROM cte b
            WHERE a.LoginName = b.LoginName
            FOR XML PATH('')
        ), 3, 8000),
        CreateDate,
        ModifyDate,
        type_desc,
        CONVERT(varchar(3), DATEDIFF(dd, modifydate, GETDATE())) + ' Days ago' AS PasswordChanged,
        Disabled,
        LOGINPROPERTY(loginname, 'DaysUntilExpiration') DaysUntilExpiration,
        LOGINPROPERTY(loginname, 'IsExpired') IsExpired,
        LOGINPROPERTY(loginname, 'IsMustChange') IsMustChange,
        PolicyChecked,
        ExpirationChecked 
    INTO #tmpServerRoles
    FROM cte a
    WHERE type_desc NOT IN ('SERVER_ROLE', 'CERTIFICATE_MAPPED_LOGIN')
    AND LoginName NOT LIKE '##%'
    AND LoginName NOT LIKE 'NT %';

    -- Create temp table for group details
    CREATE TABLE #tmpGroupDetail (
        AccountName varchar(256),
        Type varchar(128),
        Privilege varchar(128),
        Mapped_Login_Name varchar(256),
        Permission_Path varchar(256)
    );

    -- Process Windows groups
    BEGIN TRY
        DECLARE @lname varchar(256);
        DECLARE Roles CURSOR FOR
            SELECT LoginName
            FROM #tmpServerRoles
            WHERE type_desc = 'WINDOWS_GROUP';
        
        OPEN Roles;
        FETCH NEXT FROM Roles INTO @lname;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            INSERT INTO #tmpGroupDetail
            EXEC xp_logininfo @lname, members;
            FETCH NEXT FROM Roles INTO @lname;
        END;

        -- Final result with optional filtering
        IF @FilterLoginName IS NULL
        BEGIN
            SELECT
                a.ServerName,
                a.LoginName,
                ISNULL(b.AccountName, 'Individual Login') GroupMembers,
                a.type_desc,
                a.ServerRole,
                a.CreateDate,
                a.ModifyDate,
                a.PasswordChanged,
                a.Disabled,
                a.DaysUntilExpiration,
                a.IsExpired,
                a.IsMustChange,
                a.PolicyChecked,
                a.ExpirationChecked
            FROM #tmpServerRoles a
            LEFT JOIN #tmpGroupDetail b ON a.LoginName = b.Permission_Path
            ORDER BY type_desc;
        END
        ELSE
        BEGIN
            SELECT
                a.ServerName,
                a.LoginName,
                ISNULL(b.AccountName, 'Individual Login') GroupMembers,
                a.type_desc,
                a.ServerRole,
                a.CreateDate,
                a.ModifyDate,
                a.PasswordChanged,
                a.Disabled,
                a.DaysUntilExpiration,
                a.IsExpired,
                a.IsMustChange,
                a.PolicyChecked,
                a.ExpirationChecked
            FROM #tmpServerRoles a
            LEFT JOIN #tmpGroupDetail b ON a.LoginName = b.Permission_Path
            WHERE a.LoginName LIKE '%' + @FilterLoginName + '%'
               OR b.AccountName LIKE '%' + @FilterLoginName + '%'
            ORDER BY type_desc;
        END;

        CLOSE Roles;
        DEALLOCATE Roles;
    END TRY
    BEGIN CATCH
        SELECT 
            a.ServerName,
            a.LoginName,
            CASE WHEN type_desc = 'WINDOWS_GROUP' 
                 THEN 'Not able to populate group members'
                 ELSE 'Individual Login' 
            END GroupMembers,
            a.type_desc,
            a.ServerRole,
            a.CreateDate,
            a.ModifyDate,
            a.PasswordChanged,
            a.Disabled,
            a.DaysUntilExpiration,
            a.IsExpired,
            a.IsMustChange,
            a.PolicyChecked,
            a.ExpirationChecked
        FROM #tmpServerRoles a
        WHERE (@FilterLoginName IS NULL OR a.LoginName LIKE '%' + @FilterLoginName + '%')
        ORDER BY type_desc;

        IF CURSOR_STATUS('global','Roles') >= 0
        BEGIN
            CLOSE Roles;
            DEALLOCATE Roles;
        END;
    END CATCH;

    -- Cleanup
    DROP TABLE IF EXISTS #tmpGroupDetail;
    DROP TABLE IF EXISTS #tmpServerRoles;
END;
GO

-- Example execution
EXEC sp_GetDetailedLoginInfo;
-- With filter
-- EXEC sp_GetDetailedLoginInfo @FilterLoginName = 'admin';
