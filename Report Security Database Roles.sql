USE master;
GO
IF OBJECT_ID(N'vwDBList') IS NOT NULL
  DROP VIEW [dbo].[vwDBList];
GO
CREATE VIEW [dbo].[vwDBList]
AS
SELECT
  sd.[name] AS 'DBName',
  SUBSTRING(SUSER_SNAME(sd.[owner_sid]), 1, 24) AS 'Owner'
FROM master.sys.databases sd
WHERE HAS_DBACCESS(sd.[name]) = 1
AND sd.[is_read_only] = 0
AND sd.[state_desc] = 'ONLINE'
AND sd.[user_access_desc] = 'MULTI_USER'
AND sd.[is_in_standby] = 0;
GO
CREATE TABLE #temprole (
  DBName varchar(128),
  UserName varchar(128),
  LoginSID varbinary(128),
  RoleName varchar(2000),
  CreateDate datetime,
  UpdateDate datetime
);
INSERT INTO #temprole
EXEC sp_MSforeachdb 'IF EXISTS (select TOP (1) 1 from sys.databases sd inner join dbo.vwDBList vw on sd.name = vw.DBName
where sd.name = "?" ) BEGIN with cte
as
(
select "?" DBName, b.name as UserName,b.sid, c.name as RoleName , b.createdate,b.updatedate
from [?].dbo.sysmembers a
 join [?].dbo.sysusers b
 on a.memberuid = b.uid join [?].dbo.sysusers c
on a.groupuid = c.uid
--where b.createdate > CONVERT(varchar(8),getdate(),112)
)
select DISTINCT DBName, UserName, sid, RoleName = substring((select ( '', '' + RoleName)
from cte b
where a.UserName = b.UserName
FOR XML PATH ('''')
),3,8000),createdate, updatedate FROM cte a
end
';
DECLARE @SQL varchar(max);
SET @SQL = '';
SELECT
  @SQL = @SQL + 'SELECT ' + QUOTENAME([name], '''') + ' COLLATE Latin1_General_CI_AS AS [DBName], u.[name] COLLATE Latin1_General_CI_AS ' + 'FROM ' + QUOTENAME([name]) + '.dbo.sysusers u ' + 'WHERE u.issqluser = 1 ' + 'AND (u.sid is not null AND u.sid <> 0x0) ' + 'AND NOT EXISTS (SELECT 1 FROM master.dbo.syslogins l WHERE l.sid = u.sid) ' + 'UNION '
FROM master.dbo.sysdatabases sd
INNER JOIN master..vwDBList vw
  ON sd.name = vw.dbname;
SET @SQL = LEFT(@SQL, LEN(@SQL) - 5);
CREATE TABLE #DBUsers (
  DBName varchar(255),
  Username varchar(255)
);
INSERT INTO #DBUsers (DBName,
Username)
EXEC (@SQL);
SELECT
  @@SERVERNAME ServerName,
  t.*,
  sp.type_desc INTO #tmpDatabaseRoles
FROM #temprole t
LEFT JOIN #DBUsers d
  ON t.DBName = d.DBName
  AND t.UserName = d.Username
INNER JOIN sys.server_principals sp
  ON sp.sid = t.LoginSID

WHERE t.UserName NOT LIKE 'dbo'
AND t.UserName NOT LIKE '##%';
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
    UserName
  FROM #tmpDatabaseRoles
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
    A.ServerName,
    A.DBName,
    A.UserName LoginName,
    ISNULL(B.AccountName, 'Individual Login') GroupMembers,
    A.RoleName DatabaseRoles,
    A.CreateDate,
    A.UpdateDate,
    A.type_desc
  FROM #tmpDatabaseRoles A
  LEFT JOIN #tmpGroupDetail B
    ON a.UserName = b.Permission_Path
  ORDER BY type_desc
  CLOSE Roles;
  DEALLOCATE Roles;
END TRY
BEGIN CATCH
--SELECT ERROR_NUMBER() AS ErrorNumber
--     ,ERROR_SEVERITY() AS ErrorSeverity
--     ,ERROR_STATE() AS ErrorState
--     ,ERROR_PROCEDURE() AS ErrorProcedure
--     ,ERROR_LINE() AS ErrorLine
--     ,ERROR_MESSAGE() AS ErrorMessage;
  SELECT
    A.ServerName,
    A.DBName,
    A.UserName LoginName,
    CASE
      WHEN type_desc = 'WINDOWS_GROUP' THEN 'Not able to populate group members'
      ELSE 'Individual Login'
    END GroupMembers,

    A.RoleName DatabaseRole,
    A.CreateDate,
    A.UpdateDate,
    A.type_desc
  FROM #tmpDatabaseRoles A
  ORDER BY type_desc
END CATCH
GO
DROP TABLE #temprole;
DROP TABLE #DBUsers;
DROP TABLE #tmpDatabaseRoles
DROP TABLE #tmpGroupDetail
GO
