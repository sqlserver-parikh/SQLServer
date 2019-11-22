
;
WITH cte
AS (SELECT
  b.name LoginName,
  ISNULL(c.name, 'Public') ServerRole,
  b.type_desc,
  b.create_date CreateDate,
  b.modify_date ModifyDate,
  b.is_disabled Disabled,
  --Audit.HostName,
  CASE
    WHEN d.is_policy_checked = 1 THEN 'Yes'
    ELSE 'No'
  END PolicyChecked,
  CASE
    WHEN d.is_expiration_checked = 1 THEN 'Yes'
    ELSE 'No'
  END ExpirationChecked
FROM sys.server_role_members a
RIGHT JOIN sys.server_principals b
  ON a.member_principal_id = b.principal_id
LEFT JOIN sys.server_principals c
  ON a.role_principal_id = c.principal_id
LEFT JOIN sys.sql_logins d
  ON b.name = d.name
--     LEFT JOIN
--(
--    SELECT DISTINCT
--           I.loginname,
--           I.HostName
--    FROM sys.traces T
--         CROSS APPLY ::fn_trace_gettable
--    (CASE
--         WHEN CHARINDEX('_', T.[path]) <> 0
--         THEN SUBSTRING(T.PATH, 1, CHARINDEX('_', T.[path])-1)+'.trc'
--         ELSE T.[path]
--     END, T.max_files
--    ) I
--         LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.loginsid) = S.sid
--    WHERE T.id = 1
--          AND I.LoginSid IS NOT NULL
--          AND HostName IS NOT NULL
--) Audit ON Audit.LoginName = b.name
)
SELECT DISTINCT
  @@SERVERNAME ServerName,
  LoginName,
  ServerRole = SUBSTRING((SELECT
    (', ' + ServerRole)
  FROM cte b
  WHERE a.LoginName = b.LoginName
  FOR xml PATH ('')), 3, 8000),
  --HostName = SUBSTRING(
  --                    (
  --                        SELECT(', '+HostName)
  --                        FROM cte b
  --                        WHERE a.LoginName = b.LoginName FOR XML PATH('')
  --                    ), 3, 8000),
  CreateDate,
  ModifyDate,
  type_desc,
  CONVERT(varchar(3), DATEDIFF(dd, modifydate, GETDATE())) + ' Days ago' AS PasswordChanged,
  Disabled,
  LOGINPROPERTY(loginname, 'DaysUntilExpiration') DaysUntilExpiration,
  LOGINPROPERTY(loginname, 'IsExpired') IsExpired,
  LOGINPROPERTY(loginname, 'IsMustChange') IsMustChange,
  PolicyChecked,
  ExpirationChecked INTO #tmpServerRoles
FROM cte a
WHERE type_desc NOT IN ('SERVER_ROLE', 'CERTIFICATE_MAPPED_LOGIN')
AND LoginName NOT LIKE '##%'
AND LoginName NOT LIKE 'NT %';
BEGIN TRY 
DECLARE @lname varchar(256)
CREATE TABLE #tmpGroupDetail (
  AccountName varchar(256),
  Type varchar(128),
  Privilege varchar(128),
  Mapped_Login_Name varchar(256),
  Permission_Path varchar(256)
)

DECLARE Roles CURSOR FOR
SELECT
  LoginName
FROM #tmpServerRoles
WHERE type_desc = 'WINDOWS_GROUP'
OPEN Roles;
FETCH NEXT FROM Roles INTO @lname;
WHILE @@FETCH_STATUS = 0
BEGIN
  INSERT INTO #tmpGroupDetail
  EXEC xp_logininfo @lname,
                    members
  FETCH NEXT FROM Roles INTO @lname;

END
SELECT
  a.ServerName,
  a.LoginName,
  ISNULL(b.AccountName, 'Individual Login') GroupMembers ,
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
LEFT JOIN #tmpGroupDetail b
  ON a.LoginName = b.Permission_Path
ORDER BY type_desc
CLOSE Roles;
DEALLOCATE Roles;
END TRY 
BEGIN CATCH 
SELECT 
  a.ServerName,
  a.LoginName,
  case when type_desc = 'WINDOWS_GROUP' then 'Not able to populate group members'
  else 'Individual Login' end GroupMembers,
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
ORDER BY type_desc
END CATCH
DROP TABLE #tmpGroupDetail
DROP TABLE #tmpServerRoles
