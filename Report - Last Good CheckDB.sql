--https://www.travisgan.com/2012/12/sql-database-last-known-good-dbcc.html

DECLARE @DB NVARCHAR(MAX)= NULL;
--SET @DB = N'YourDatabase'; --NULL to list all databases

DECLARE @Command NVARCHAR(MAX);
DECLARE @ExecCommand NVARCHAR(MAX);
CREATE TABLE #DBInfoTemp
(ParentObject VARCHAR(255),
 [Object]     VARCHAR(255),
 Field        VARCHAR(255),
 [Value]      VARCHAR(255)
);
CREATE TABLE #LastCkTemp
(DatabaseName      VARCHAR(255),
 LastKnownGoodDate VARCHAR(255)
);
IF @DB IS NULL
    BEGIN
        SET @Command = N'
  INSERT INTO #DBInfoTemp
  EXEC (''DBCC DBINFO([?]) WITH TABLERESULTS'');';
    END;
    ELSE
    BEGIN
        SET @Command = N'
  INSERT INTO #DBInfoTemp
  EXEC (''DBCC DBINFO(['+@DB+']) WITH TABLERESULTS'');';
    END;
SET @ExecCommand = @Command+N'
 INSERT INTO #LastCkTemp
 SELECT 
  MAX(CASE WHEN di.Field = ''dbi_dbname''
   THEN di.Value
   ELSE NULL
   END) AS DatabaseName    
  , MAX(CASE WHEN di.Field = ''dbi_dbccLastKnownGood''
     THEN di.Value
     ELSE NULL
     END) AS LastCheckDBDate
 FROM #DBInfoTemp di
 WHERE 
  di.Field = ''dbi_dbccLastKnownGood''
  OR di.Field = ''dbi_dbname'';
   
 TRUNCATE TABLE #DBInfoTemp;
 ';
IF @DB IS NULL
    BEGIN
        EXEC sp_MSforeachdb
             @ExecCommand;
    END;
    ELSE
    BEGIN
        EXEC (@ExecCommand);
    END;
SELECT ck.DatabaseName,
       ck.LastKnownGoodDate,
       DATEDIFF(DD, ck.LastKnownGoodDate, GETDATE()) DaysBefore
FROM #LastCkTemp ck
WHERE DatabaseName NOT LIKE 'tempdb';
GO
DROP TABLE #LastCkTemp, #DBInfoTemp;
