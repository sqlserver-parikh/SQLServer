USE master;
GO
IF OBJECT_ID(N'vwDBList') IS NOT NULL
    DROP VIEW [dbo].[vwDBList];
GO
CREATE VIEW [dbo].[vwDBList]
AS
     SELECT sd.[name] AS 'DBName',
            SUBSTRING(SUSER_SNAME(sd.[owner_sid]), 1, 24) AS 'Owner'
     FROM master.sys.databases sd
     WHERE HAS_DBACCESS(sd.[name]) = 1
           AND sd.[is_read_only] = 0
           AND sd.[state_desc] = 'ONLINE'
           AND sd.[user_access_desc] = 'MULTI_USER'
           AND sd.[is_in_standby] = 0;
GO
CREATE TABLE #temprole
(DBName     VARCHAR(128),
 UserName   VARCHAR(128),
 LoginSID   VARBINARY(128),
 RoleName   VARCHAR(2000),
 CreateDate DATETIME,
 UpdateDate DATETIME
);
INSERT INTO #temprole
EXEC sp_MSforeachdb
'IF EXISTS (select TOP (1) 1 from sys.databases sd inner join dbo.vwDBList vw on sd.name = vw.DBName
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
DECLARE @SQL VARCHAR(MAX);
SET @SQL = '';
SELECT @SQL = @SQL+'SELECT '+QUOTENAME([name], '''')+' COLLATE Latin1_General_CI_AS AS [DBName], u.[name] COLLATE Latin1_General_CI_AS '+'FROM '+QUOTENAME([name])+'.dbo.sysusers u '+'WHERE u.issqluser = 1 '+'AND (u.sid is not null AND u.sid <> 0x0) '+'AND NOT EXISTS (SELECT 1 FROM master.dbo.syslogins l WHERE l.sid = u.sid) '+'UNION '
FROM master.dbo.sysdatabases sd
     INNER JOIN master..vwDBList vw ON sd.name = vw.dbname;
SET @SQL = LEFT(@SQL, LEN(@SQL) - 5);
CREATE TABLE #DBUsers
(DBName   VARCHAR(255),
 Username VARCHAR(255)
);
INSERT INTO #DBUsers
(DBName,
 Username
)
EXEC (@SQL);
SELECT @@SERVERNAME ServerName,
       t.*,
       d.Username,
       sp.type_desc
FROM #temprole t
     LEFT JOIN #DBUsers d ON t.DBName = d.DBName
                             AND t.UserName = d.Username
     INNER JOIN sys.server_principals sp ON sp.sid = t.LoginSID
WHERE t.UserName NOT LIKE 'dbo'
      AND t.UserName NOT LIKE '##%';
GO
DROP TABLE #temprole;
DROP TABLE #DBUsers;
DROP VIEW [vwDBList];
GO
