CREATE VIEW [dbo].[vwAdmin_DB_List]
AS
     SELECT sd.[name] AS 'DBName',
            SUBSTRING(SUSER_SNAME(sd.[owner_sid]), 1, 24) AS 'Owner'
     FROM master.sys.databases sd
     WHERE HAS_DBACCESS(sd.[name]) = 1
           AND sd.[is_read_only] = 0
           AND sd.[state_desc] = 'ONLINE'
           AND sd.[user_access_desc] = 'MULTI_USER'
           AND sd.[is_in_standby] = 0;

go
CREATE PROCEDURE spAdmin_duplicateIndex
  @databasename SYSNAME = 'ALL'
AS
 CREATE TABLE #dblist
 (
      dbname SYSNAME
 );
 IF(@databasename = 'all')
     BEGIN
     INSERT INTO #dblist
     SELECT name
     FROM   sys.databases AS sd
    INNER JOIN vwAdmin_DB_List AS vw ON sd.name = vw.dbname;
     END;
 ELSE
 INSERT INTO #dblist
 SELECT name
 FROM   sys.databases AS sd
    INNER JOIN vwAdmin_DB_List AS vw ON sd.name = vw.dbname
 WHERE  name LIKE @databasename;
 CREATE TABLE #duplicateIndex
 (
      DBName         SYSNAME,
      TableName      SYSNAME,
      IndexName      SYSNAME,
      ExactDuplicate SYSNAME
 );
 INSERT INTO #duplicateIndex
 EXEC sp_msforeachdb
  'IF EXISTS (select TOP (1) 1 from #dblist
where dbname = "?" ) BEGIN use [?]; WITH indexcols
 AS (SELECT    object_id AS id
     , index_id AS  indid
     , name
     ,
     (
     SELECT CASE keyno
        WHEN 0
        THEN NULL
        ELSE colid
    END AS [data()]
     FROM    sys.sysindexkeys AS k
     WHERE  k.id = i.object_id
    AND k.indid = i.index_id
     ORDER BY keyno
    , colid
     FOR XML PATH('''')
     ) AS                   cols
     ,
     (
     SELECT CASE keyno
        WHEN 0
        THEN colid
        ELSE NULL
    END AS [data()]
     FROM   sys.sysindexkeys AS k
     WHERE  k.id = i.object_id
    AND k.indid = i.index_id
     ORDER BY colid
     FOR XML PATH('''')
     ) AS                   inc
     FROM sys.indexes AS i)
 SELECT DB_NAME(DB_ID()) AS                               "DBName"
  , OBJECT_SCHEMA_NAME(c1.id) + OBJECT_NAME(c1.id) AS "Table"
  , c1.name AS                                        "IndexName"
  , c2.name AS                                        "ExactDuplicate"
 FROM   indexcols AS c1
    JOIN indexcols AS c2 ON c1.id = c2.id
        AND c1.indid < c2.indid
        AND c1.cols = c2.cols
        AND c1.inc = c2.inc END';
 SELECT *
 FROM   #duplicateIndex;
GO
