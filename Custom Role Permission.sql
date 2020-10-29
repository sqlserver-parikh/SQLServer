USE DBATasks;
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
 CustomRole VARCHAR(128),
 ACTION     VARCHAR(2000),
 Object     VARCHAR(512)
);
INSERT INTO #temprole
EXEC sp_MSforeachdb
'IF EXISTS (select TOP (1) 1 from sys.databases sd inner join dbo.vwDBList vw on sd.name = vw.DBName
where sd.name = "?" ) BEGIN SELECT  ''?'' DatabaseName,
    dps.name AS CustomRole,
   permission_name AS Action,
   CASE class
      WHEN 0 THEN ''Database::'' + "?"
      WHEN 1 THEN OBJECT_NAME(major_id, db_id("?"))
      WHEN 3 THEN ''Schema::'' + SCHEMA_NAME(major_id) END as Object
FROM [?].sys.database_permissions dp
INNER JOIN [?].sys.database_principals dps on dp.grantee_principal_id = dps.principal_id
WHERE class IN (0, 1, 3) and  is_fixed_role = 0 and type_desc like ''Database_Role'' and USER_NAME(grantee_principal_id) not like ''public''
AND minor_id = 0
END';
SELECT *
FROM #temprole
WHERE Object IS NOT NULL
      AND CustomRole NOT IN('SQLAgentUserRole', 'TargetServersRole','db_ssisadmin','DatabaseMailUserRole')
ORDER BY 1,
         2,
         3;
GO
DROP TABLE #temprole; 
