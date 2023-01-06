--https://www.mssqltips.com/sqlservertip/5296/implementation-of-sliding-window-partitioning-in-sql-server-to-purge-data/
SELECT OBJECT_SCHEMA_NAME(pstats.object_id) AS SchemaName, 
       OBJECT_NAME(pstats.object_id) AS TableName,
       CASE
           WHEN P.data_compression_desc = 'NONE'
           THEN 'ALTER TABLE ' + OBJECT_SCHEMA_NAME(pstats.object_id) + '.' + OBJECT_NAME(pstats.object_id) + ' REBUILD PARTITION = ' + CONVERT(VARCHAR(5), pstats.partition_number) + ' WITH(DATA_COMPRESSION = PAGE )'
           ELSE 'Already compressed' + ' --  ALTER TABLE ' + OBJECT_SCHEMA_NAME(pstats.object_id) + '.' + OBJECT_NAME(pstats.object_id) + ' REBUILD PARTITION = ' + CONVERT(VARCHAR(5), pstats.partition_number) + ' WITH(DATA_COMPRESSION = PAGE )'
       END CompressionScript, 
       ps.name AS PartitionSchemeName, 
       ds.name AS PartitionFilegroupName, 
       pf.name AS PartitionFunctionName,
       CASE pf.boundary_value_on_right
           WHEN 0
           THEN 'Range Left'
           ELSE 'Range Right'
       END AS PartitionFunctionRange,
       CASE pf.boundary_value_on_right
           WHEN 0
           THEN 'Upper Boundary'
           ELSE 'Lower Boundary'
       END AS PartitionBoundary, 
       c.name AS PartitionKey,
       CASE
           WHEN pf.boundary_value_on_right = 0
           THEN c.name + ' > ' + CAST(ISNULL(LAG(prv.value) OVER(PARTITION BY pstats.object_id
                ORDER BY pstats.object_id, 
                         pstats.partition_number), 'Infinity') AS VARCHAR(100)) + ' and ' + c.name + ' <= ' + CAST(ISNULL(prv.value, 'Infinity') AS VARCHAR(100))
           ELSE c.name + ' >= ' + CAST(ISNULL(prv.value, 'Infinity') AS VARCHAR(100)) + ' and ' + c.name + ' < ' + CAST(ISNULL(LEAD(prv.value) OVER(PARTITION BY pstats.object_id
                ORDER BY pstats.object_id, 
                         pstats.partition_number), 'Infinity') AS VARCHAR(100))
       END AS PartitionRange, 
       'ALTER PARTITION FUNCTION ' + CONVERT(VARCHAR(40), pf.name) + '() MERGE RANGE(''' MergeScript1, 
       prv.value AS PartitionBoundaryValueMergeScript2, 
       ''')' MergeScript3, 
       pstats.partition_number AS PartitionNumber, 
       pstats.row_count AS PartitionRowCount, 
       p.data_compression_desc AS DataCompression
INTO #TEMP
FROM sys.dm_db_partition_stats AS pstats
     INNER JOIN sys.partitions AS p ON pstats.partition_id = p.partition_id
     INNER JOIN sys.destination_data_spaces AS dds ON pstats.partition_number = dds.destination_id
     INNER JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
     INNER JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
     INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
     INNER JOIN sys.indexes AS i ON pstats.object_id = i.object_id
                                    AND pstats.index_id = i.index_id
                                    AND dds.partition_scheme_id = i.data_space_id
                                    AND i.type <= 1

     /* Heap or Clustered Index */

     INNER JOIN sys.index_columns AS ic ON i.index_id = ic.index_id
                                           AND i.object_id = ic.object_id
                                           AND ic.partition_ordinal > 0
     INNER JOIN sys.columns AS c ON pstats.object_id = c.object_id
                                    AND ic.column_id = c.column_id
     LEFT JOIN sys.partition_range_values AS prv ON pf.function_id = prv.function_id
                                                    AND pstats.partition_number = (CASE pf.boundary_value_on_right
                                                                                       WHEN 0
                                                                                       THEN prv.boundary_id
                                                                                       ELSE(prv.boundary_id + 1)
                                                                                   END)
WHERE 1 = 1
--   AND data_compression_desc LIKE 'NONE'
--AND OBJECT_NAME(pstats.object_id) like '%StagingDetectorData%'
--AND pstats.row_count > 10000
ORDER BY 2, 
         pstats.partition_number; 
--GO
--SELECT DISTINCT 
--       s.name, 
--       t.name, 
--       i.name, 
--       'ALTER INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(S.name) + '.' + QUOTENAME(T.name) + ' REBUILD PARTITION = ' + CONVERT(VARCHAR(3), partition_number) + ' WITH (SORT_IN_TEMPDB = OFF, DATA_COMPRESSION = PAGE )', 
--       i.type, 
--       i.index_id, 
--       p.partition_number, 
--       p.rows, 
--       data_compression_desc
--FROM sys.tables t
--     LEFT JOIN sys.indexes i ON t.object_id = i.object_id
--     JOIN sys.schemas s ON t.schema_id = s.schema_id
--     LEFT JOIN sys.partitions p ON i.index_id = p.index_id
--                                   AND t.object_id = p.object_id
--WHERE t.type = 'U'
--      AND p.data_compression_desc = 'NONE'
--      AND partition_number > 1
--      AND P.rows < 100000
--ORDER BY p.rows ASC;

SELECT *
FROM #TEMP;
--where PartitionRowCount = 0 and PartitionBoundaryValue is not null

DROP TABLE #TEMP;
