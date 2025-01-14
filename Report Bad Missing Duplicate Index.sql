USE tempdb;
GO

CREATE OR ALTER PROCEDURE usp_IndexAnalysis
(
    @DatabaseName NVARCHAR(128) = 'test', -- Specify the database name
    @BADIndex BIT = 1,        -- Set to 1 to include analysis of bad non-clustered indexes
    @MissingIndex BIT = 0,    -- Set to 1 to include analysis of missing indexes
    @IndexFragmentation BIT = 0,  -- This will take a lot of time if DB is big with indexes
    @PageCount INT = 2500,
    @findDuplicateIndex BIT = 1  -- Set to 1 to include analysis of duplicate indexes
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);

    -- Analyze bad non-clustered indexes
    IF @BADIndex = 1
    BEGIN
        SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + ';
        SELECT ''' + @DatabaseName + ''' AS DBName, 
               SCHEMA_NAME(o.[schema_id]) AS [Schema Name], 
               OBJECT_NAME(s.[object_id]) AS [Table Name],
               i.name AS [Index Name], 
               i.index_id, 
               i.is_disabled, 
               i.is_hypothetical, 
               i.has_filter, 
               i.fill_factor,
               s.user_updates AS [Total Writes], 
               s.user_seeks + s.user_scans + s.user_lookups AS [Total Reads],
               s.user_updates - (s.user_seeks + s.user_scans + s.user_lookups) AS [Difference]
        FROM sys.dm_db_index_usage_stats AS s WITH (NOLOCK)
        INNER JOIN sys.indexes AS i WITH (NOLOCK) ON s.[object_id] = i.[object_id] AND i.index_id = s.index_id
        INNER JOIN sys.objects AS o WITH (NOLOCK) ON i.[object_id] = o.[object_id]
        WHERE OBJECTPROPERTY(s.[object_id],''IsUserTable'') = 1
        AND s.database_id = DB_ID(@DatabaseName)
        AND s.user_updates > (s.user_seeks + s.user_scans + s.user_lookups)
        AND i.index_id > 1 
        AND i.[type_desc] = N''NONCLUSTERED''
        AND i.is_primary_key = 0 
        AND i.is_unique_constraint = 0 
        AND i.is_unique = 0
        ORDER BY [Difference] DESC, [Total Writes] DESC, [Total Reads] ASC OPTION (RECOMPILE);';
        EXEC sp_executesql @sql, N'@DatabaseName NVARCHAR(128)', @DatabaseName;
    END

    -- Analyze missing indexes
    IF @MissingIndex = 1
    BEGIN
        SET @sql = N'SELECT ''' + @DatabaseName + ''' AS DBName, 
               migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * 
               (migs.user_seeks + migs.user_scans) AS improvement_measure, 
               ''CREATE INDEX [missing_index_'' + CONVERT(varchar, mig.index_group_handle) + ''_'' + CONVERT(varchar, mid.index_handle) + ''_'' + LEFT(PARSENAME(mid.statement, 1), 32) + '']''
               + '' ON '' + mid.statement 
               + '' ('' + ISNULL(mid.equality_columns, '''') 
               + CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN '','' ELSE '''' END 
               + ISNULL(mid.inequality_columns, '''') + '')'' 
               + ISNULL('' INCLUDE ('' + mid.included_columns + '')'', '''') AS create_index_statement, 
               migs.*, mid.database_id, mid.[object_id]
        FROM ' + QUOTENAME(@DatabaseName) + '.sys.dm_db_missing_index_groups mig
        INNER JOIN ' + QUOTENAME(@DatabaseName) + '.sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
        INNER JOIN ' + QUOTENAME(@DatabaseName) + '.sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
        WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10
        AND mid.database_id = DB_ID(@DatabaseName)
        ORDER BY migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC;';
        EXEC sp_executesql @sql, N'@DatabaseName NVARCHAR(128)', @DatabaseName;
    END

    -- Analyze index fragmentation
    IF @IndexFragmentation = 1
    BEGIN
        SET @sql = N'SELECT DB_NAME(ps.database_id) AS [Database Name], 
                            SCHEMA_NAME(o.[schema_id]) AS [Schema Name],
                            OBJECT_NAME(ps.OBJECT_ID) AS [Object Name], 
                            i.[name] AS [Index Name], 
                            ps.index_id, 
                            ps.index_type_desc, 
                            CAST(ps.avg_fragmentation_in_percent AS DECIMAL (15,3)) AS [Avg Fragmentation in Pct], 
                            ps.fragment_count, 
                            ps.page_count, 
                            i.fill_factor, 
                            i.has_filter, 
                            i.filter_definition, 
                            i.[allow_page_locks]
        FROM sys.dm_db_index_physical_stats(DB_ID(@DatabaseName), NULL, NULL, NULL , N''LIMITED'') AS ps
        INNER JOIN sys.indexes AS i WITH (NOLOCK) ON ps.[object_id] = i.[object_id] AND ps.index_id = i.index_id
        INNER JOIN sys.objects AS o WITH (NOLOCK) ON i.[object_id] = o.[object_id]
        WHERE ps.database_id = DB_ID(@DatabaseName)
        AND ps.page_count > @PageCount 
        ORDER BY ps.avg_fragmentation_in_percent DESC OPTION (RECOMPILE);';
        EXEC sp_executesql @sql, N'@DatabaseName NVARCHAR(128), @PageCount INT', @DatabaseName, @PageCount;
    END

    -- Analyze duplicate indexes
    IF @findDuplicateIndex = 1
    BEGIN
        -- Temporary table to list databases
        CREATE TABLE #dblist (
            dbname SYSNAME
        );

        IF (@DatabaseName = 'ALL')
        BEGIN
            INSERT INTO #dblist (dbname)
            SELECT name
            FROM sys.databases AS sd
            WHERE HAS_DBACCESS(sd.name) = 1
                  AND sd.is_read_only = 0
                  AND sd.state_desc = 'ONLINE'
                  AND sd.user_access_desc = 'MULTI_USER'
                  AND sd.is_in_standby = 0;
        END
        ELSE
        BEGIN
            INSERT INTO #dblist (dbname)
            SELECT name
            FROM sys.databases AS sd
            WHERE HAS_DBACCESS(sd.name) = 1
                  AND sd.is_read_only = 0
                  AND sd.state_desc = 'ONLINE'
                  AND sd.user_access_desc = 'MULTI_USER'
                  AND sd.is_in_standby = 0
                  AND name LIKE @DatabaseName;
        END

        -- Temporary table to store duplicate index information
        CREATE TABLE #duplicateIndex (
            DBName         SYSNAME,
            TableName      SYSNAME,
            IndexName      SYSNAME,
            ExactDuplicate SYSNAME
        );

        -- Dynamic SQL to analyze duplicate indexes
        SET @sql = N'
        IF EXISTS (SELECT TOP (1) 1 FROM #dblist WHERE dbname = ''?'')
        BEGIN
            USE [?];
            WITH indexcols AS (
                SELECT object_id AS id, index_id AS indid, name,
                    (
                        SELECT CASE keyno WHEN 0 THEN NULL ELSE colid END AS [data()]
                        FROM sys.sysindexkeys AS k
                        WHERE k.id = i.object_id AND k.indid = i.index_id
                        ORDER BY keyno, colid
                        FOR XML PATH('''')
                    ) AS cols,
                    (
                        SELECT CASE keyno WHEN 0 THEN colid ELSE NULL END AS [data()]
                        FROM sys.sysindexkeys AS k
                        WHERE k.id = i.object_id AND k.indid = i.index_id
                        ORDER BY colid
                        FOR XML PATH('''')
                    ) AS inc
                FROM sys.indexes AS i
            )
            SELECT DB_NAME(DB_ID()) AS DBName,
                   OBJECT_SCHEMA_NAME(c1.id) + '' '' + OBJECT_NAME(c1.id) AS TableName,
                   c1.name AS IndexName,
                   c2.name AS ExactDuplicate
            FROM indexcols AS c1
            JOIN indexcols AS c2 ON c1.id = c2.id
                                 AND c1.indid < c2.indid
                                 AND c1.cols = c2.cols
                                 AND c1.inc = c2.inc;
        END';

        INSERT INTO #duplicateIndex
        EXEC sp_MSforeachdb @sql;

        SELECT *
        FROM #duplicateIndex;

        DROP TABLE #dblist;
        DROP TABLE #duplicateIndex;
    END
END;
GO
