ALTER PROCEDURE spTableGrowthDetail
(
    @LowRowsRetentionDays INT = 4,
    @AllRowsRetentionDays int = 15,
    @LowRowCount int = 10000,
    @ResultOnly bit = 0,
    @StoreTableName sysname = 'tblTableGrowthData' --Table will be created in msdb if not exists.
)

--All tables with record > LowRowCount will be retained for AllRowsRetentionDays
--All tables with record < LowRowCount will be retained for LowRowsRetentionDays
AS
SET NOCOUNT ON;
CREATE TABLE #indexsize
(
    dbname SYSNAME,
    tablename VARCHAR(128),
    indexname VARCHAR(128),
    indexid INT,
    indexsize INT,
    data_space_id int
);
create table #partitionScheme
(
    DBName sysname,
    SchemeName sysname,
    data_space_id int,
    type varchar(10),
    type_desc varchar(128),
    is_default int,
    is_system int,
    function_id int
)

insert into #partitionScheme
exec sp_MSForeachdb 'use [?];
select "?",* from sys.partition_schemes '

INSERT INTO #indexsize
EXEC sp_MSforeachdb 'SELECT "?",
OBJECT_NAME(i.OBJECT_ID,db_id("?")) AS TableName, i.name AS IndexName, i.index_id AS IndexID,
8 * SUM(a.used_pages)/1024 AS Indexsize,i.data_space_id
FROM [?].sys.indexes AS i
JOIN [?].sys.partitions AS p ON p.OBJECT_ID = i.OBJECT_ID AND p.index_id = i.index_id 
JOIN [?].sys.allocation_units AS a ON a.container_id = p.partition_id GROUP BY i.OBJECT_ID,i.index_id,i.name, i.data_space_id ORDER BY 4 desc';
SELECT i1.DBName,
       i1.TableName,
       (
           SELECT SUBSTRING(
                  (
                      SELECT ' ,' + indexname + ':' + CONVERT(VARCHAR(15), indexsize)
                      FROM #indexsize AS i2
                      WHERE i2.dbname = i1.dbname
                            AND i2.tablename = i1.tablename
                      FOR XML PATH('')
                  ),
                  3,
                  8000
                           )
       ) AS IndexName,
       CASE
           WHEN
           (
               SELECT SUBSTRING(
                      (
                          SELECT ' ,' + indexname + ':' + CONVERT(VARCHAR(15), indexsize)
                          FROM #indexsize AS i2
                          WHERE i2.dbname = i1.dbname
                                AND i2.tablename = i1.tablename
                          FOR XML PATH('')
                      ),
                      3,
                      8000
                               )
           ) = '' THEN
               '0'
           ELSE
               COUNT(*) OVER (PARTITION BY DBName, TableName)
       END AS IndexCount,
       SUM(indexsize) OVER (PARTITION BY DBName, TableName) AS TotalIndexSize,
       i1.data_space_id
INTO #temps
FROM #indexsize AS i1
ORDER BY i1.dbname,
         i1.tablename,
         i1.indexname;
CREATE TABLE #tableReport
(
    partition_id BIGINT,
    object_id INT,
    index_id BIGINT,
    partition_number INT,
    hobt_id BIGINT,
    rows BIGINT,
    filestream_filegroup_id SMALLINT,
    data_compression TINYINT,
    data_compression_desc NVARCHAR(60),
    [DatabaseName] [VARCHAR](128),
    SchemaName [SYSNAME] NOT NULL,
    [TableName] [NVARCHAR](128) NOT NULL,
    TableType NVARCHAR(128),
    CTEnabled VARCHAR(20),
    IsSchemaPublished VARCHAR(60),
    IsTablePublished VARCHAR(60),
    IsReplicated VARCHAR(60),
    IsTrackedbyCDC VARCHAR(60),
    TotalColumns VARCHAR(60),
    TableCreateDate [DATETIME] NOT NULL,
    TableModifyDate [DATETIME] NOT NULL,
    RowsCount [BIGINT] NULL,
    TotalSize [BIGINT] NULL,
    DataSize [BIGINT] NULL,
    IndexSize [BIGINT] NULL,
    UnusedSize [BIGINT] NULL
);
INSERT INTO #tableReport
(
    partition_id,
    object_id,
    index_id,
    partition_number,
    hobt_id,
    rows,
    filestream_filegroup_id,
    data_compression,
    data_compression_desc,
    [DatabaseName],
    SchemaName,
    [TableName],
    TableType,
    CTEnabled,
    IsSchemaPublished,
    IsTablePublished,
    IsReplicated,
    IsTrackedbyCDC,
    TotalColumns,
    TableCreateDate,
    TableModifyDate,
    RowsCount,
    TotalSize,
    DataSize,
    IndexSize,
    UnusedSize
)
EXEC sp_msforeachdb 'SELECT SP.*, db_name(db_id("?")) DatabaseName, a3.name AS [schemaname], a2.name AS [tablename], a2.type, CASE WHEN ctt.object_id IS NULL THEN ''No''
when ctt.object_id is not null then ''Yes'' end CTEnable, ST.is_schema_published, ST.is_published, ST.is_replicated, ST.is_tracked_by_cdc, ST.max_column_id_used, a2.create_date, a2.modify_date, a1.rows as row_count, (a1.reserved + ISNULL(a4.reserved,0))* 8 AS reserved, a1.data * 8 AS data, (CASE WHEN (a1.used + ISNULL(a4.used,0)) > a1.data THEN (a1.used + ISNULL(a4.used,0)) - a1.data ELSE 0 END) * 8 AS index_size, (CASE WHEN (a1.reserved + ISNULL(a4.reserved,0)) > a1.used THEN (a1.reserved + ISNULL(a4.reserved,0)) - a1.used ELSE 0 END) * 8 AS unused FROM (SELECT ps.object_id, SUM ( CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END
) AS [rows],
SUM (ps.reserved_page_count) AS reserved, SUM ( CASE WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count) ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count) END
) AS data,
SUM (ps.used_page_count) AS used
FROM ?.sys.dm_db_partition_stats ps
GROUP BY ps.object_id) AS a1
LEFT OUTER JOIN
(SELECT
it.parent_id,
SUM(ps.reserved_page_count) AS reserved,
SUM(ps.used_page_count) AS used
FROM ?.sys.dm_db_partition_stats ps
INNER JOIN ?.sys.internal_tables it ON (it.object_id = ps.object_id) WHERE it.internal_type IN (202,204) GROUP BY it.parent_id) AS a4 ON (a4.parent_id = a1.object_id) INNER JOIN ?.sys.all_objects a2 ON ( a1.object_id = a2.object_id ) INNER JOIN ?.sys.schemas a3 ON (a2.schema_id = a3.schema_id) left JOIN ?.sys.tables ST on ST.object_id = a2.object_id left JOIN ?.sys.partitions SP on SP.object_id = ST.object_id left JOIN ?.sys.change_tracking_tables CTT on a2.object_id = CTT.object_id --WHERE db_name(db_id("?")) not in (''master'',''model'',''tempdb'',''msdb'')
';
SELECT DISTINCT
    [DatabaseName],
    SchemaName,
    n.TableName,
    TableType,
    CTEnabled,
    ISNULL(IsSchemaPublished, 'System Table') AS IsSchemaPublished,
    ISNULL(IsTablePublished, 'System Table') AS IsTablePublished,
    ISNULL(data_compression_desc, 'System Table') AS DataCompressionDescription,
    ISNULL(IsReplicated, 'System Table') AS IsReplicated,
    ISNULL(IsTrackedbyCDC, 'System Table') AS IsTrackedbyCDC,
    ISNULL(TotalColumns, 'System Table') AS TotalColumns,
    TableCreateDate,
    TableModifyDate,
    RowsCount,
    TotalSize,
    DataSize,
    n.IndexSize,
    UnusedSize,
    i.IndexName AS [IndexName:Size],
    i.IndexCount,
    case
        when (i.data_space_id) > 256 then
        (
            select ps.SchemeName
            from #partitionScheme ps
            where ps.data_space_id = i.data_space_id
                  and ps.DBName = n.DatabaseName
        )
        else
            FILEGROUP_NAME(i.data_space_id)
    end FGName,
    i.data_space_id,
    GETDATE() AS ReportRun
INTO ##Report
FROM #tableReport AS n
    LEFT JOIN #temps AS i
        ON n.DatabaseName = i.dbname
           AND n.TableName = i.tablename
WHERE DatabaseName NOT LIKE 'tempdb'
ORDER BY TotalSize DESC;

if @ResultOnly = 1
    select *
    from ##Report
else
begin
	DECLARE @SQL NVARCHAR(MAX);
	if exists (SELECT 1 FROM msdb.sys.tables WHERE name = @StoreTableName)
	begin
    SET @SQL = 'INSERT INTO msdb.[dbo].' + QUOTENAME(@StoreTableName) + ' 
    select *
    from ##Report
    DELETE msdb.dbo.' + QUOTENAME(@StoreTableName) + ' 
    WHERE ReportRun < DATEADD(DD, -' + CONVERT(VARCHAR(10), @AllRowsRetentionDays) +', GETDATE());
    DELETE msdb.dbo.' + QUOTENAME(@StoreTableName)+ ' 
    WHERE RowsCount < ' + CONVERT(VARCHAR(10),  @LowRowCount) + ' 
          AND ReportRun < DATEADD(DD, -' +CONVERT(VARCHAR(10),  @LowRowsRetentionDays) +' , GETDATE());'
	end else 
	SET @SQL = 'SELECT * INTO msdb.dbo.' + QUOTENAME(@StoreTableName) + ' FROM ##REPORT'
	EXEC sp_executesql @SQL
end
DROP TABLE #tableReport;
DROP TABLE #temps;
DROP TABLE #indexsize;
DROP TABLE #partitionScheme
DROP TABLE ##Report
GO
