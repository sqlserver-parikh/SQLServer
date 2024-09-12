IF OBJECT_ID('tempdb..#usp_LastKnownGoodDBCC') IS NOT NULL
    DROP PROCEDURE #usp_LastKnownGoodDBCC;
GO

CREATE PROCEDURE #usp_LastKnownGoodDBCC
(
    @DbNames NVARCHAR(MAX) = '' -- NULL OR EMPTY: All DBs
)
AS
BEGIN
    DECLARE @Command NVARCHAR(MAX);
    DECLARE @ExecCommand NVARCHAR(MAX);
	IF @DbNames = ''
	SET @DbNames = NULL
    CREATE TABLE #DBInfoTemp
    (
        ParentObject VARCHAR(255),
        [Object]     VARCHAR(255),
        Field        VARCHAR(255),
        [Value]      VARCHAR(255)
    );

    CREATE TABLE #LastCkTemp
    (
        DatabaseName      VARCHAR(255),
        LastKnownGoodDate VARCHAR(255)
    );

    IF ( @DbNames IS NULL )
    BEGIN
        SET @Command = N'
        INSERT INTO #DBInfoTemp
        EXEC (''DBCC DBINFO([?]) WITH TABLERESULTS'');';
    END
    ELSE
    BEGIN
        SET @Command = N'
        INSERT INTO #DBInfoTemp
        EXEC (''DBCC DBINFO([' + REPLACE(@DbNames, '''', '''''') + ']) WITH TABLERESULTS'');';
    END

    SET @ExecCommand = @Command + N'
    INSERT INTO #LastCkTemp
    SELECT 
        MAX(CASE WHEN di.Field = ''dbi_dbname''
            THEN di.Value
            ELSE NULL
        END) AS DatabaseName,    
        MAX(CASE WHEN di.Field = ''dbi_dbccLastKnownGood''
            THEN di.Value
            ELSE NULL
        END) AS LastCheckDBDate
    FROM #DBInfoTemp di
    WHERE 
        di.Field = ''dbi_dbccLastKnownGood''
        OR di.Field = ''dbi_dbname'';
    
    TRUNCATE TABLE #DBInfoTemp;
    ';

    IF @DbNames IS NULL
    BEGIN
        EXEC sp_MSforeachdb @ExecCommand;
    END
    ELSE
    BEGIN
        EXEC (@ExecCommand);
    END

    SELECT ck.DatabaseName,
           ck.LastKnownGoodDate,
           DATEDIFF(DD, ck.LastKnownGoodDate, GETDATE()) AS DaysBefore
    FROM #LastCkTemp ck
    WHERE DatabaseName NOT LIKE 'tempdb'
    ORDER BY DATEDIFF(DD, ck.LastKnownGoodDate, GETDATE()) DESC, ck.DatabaseName;

    -- Drop the temporary tables
    DROP TABLE #LastCkTemp;
    DROP TABLE #DBInfoTemp;
END;
GO
EXEC #usp_LastKnownGoodDBCC
