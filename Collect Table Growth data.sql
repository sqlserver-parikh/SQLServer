USE [DBATasks]
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblTableGrowthDetail]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblTableGrowthDetail](
	[DatabaseName] [varchar](128) NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[TableName] [nvarchar](128) NOT NULL,
	[TableType] [nvarchar](128) NULL,
	[CTEnabled] [varchar](20) NULL,
	[IsSchemaPublished] [varchar](60) NOT NULL,
	[IsTablePublished] [varchar](60) NOT NULL,
	[DataCompressionDescription] [nvarchar](60) NOT NULL,
	[IsReplicated] [varchar](60) NOT NULL,
	[IsTrackedbyCDC] [varchar](60) NOT NULL,
	[TotalColumns] [varchar](60) NOT NULL,
	[TableCreateDate] [datetime] NOT NULL,
	[TableModifyDate] [datetime] NOT NULL,
	[RowsCount] [bigint] NULL,
	[TotalSizeKB] [bigint] NULL,
	[DataSizeKB] [bigint] NULL,
	[IndexSizeKB] [bigint] NULL,
	[UnusedSizeKB] [bigint] NULL,
	[IndexName:Size] [nvarchar](max) NULL,
	[IndexCount] [int] NULL,
	[ReportRun] [datetime] NOT NULL
) ON [PRIMARY] 
END
GO
USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spTableGrowthDetail]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spTableGrowthDetail]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spTableGrowthDetail]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spTableGrowthDetail] AS' 
END
GO


ALTER PROCEDURE [dbo].[spTableGrowthDetail](@retaindays INT = 15, @ResultOnly bit = 0)
AS
     CREATE TABLE #indexsize
     (dbname    SYSNAME,
      tablename VARCHAR(128),
      indexname VARCHAR(128),
      indexid   INT,
      indexsize INT
     );
     INSERT INTO #indexsize
     EXEC sp_MSforeachdb
'SELECT "?",
OBJECT_NAME(i.OBJECT_ID,db_id("?")) AS TableName, i.name AS IndexName, i.index_id AS IndexID,
8 * SUM(a.used_pages)/1024 AS Indexsize
FROM [?].sys.indexes AS i
JOIN [?].sys.partitions AS p ON p.OBJECT_ID = i.OBJECT_ID AND p.index_id = i.index_id JOIN [?].sys.allocation_units AS a ON a.container_id = p.partition_id GROUP BY i.OBJECT_ID,i.index_id,i.name ORDER BY 4 desc';
     SELECT i1.DBName,
            i1.TableName,
     (
         SELECT SUBSTRING(
                         (
                             SELECT ' ,'+indexname+':'+CONVERT(VARCHAR(15), indexsize)
                             FROM #indexsize AS i2
                             WHERE i2.dbname = i1.dbname
                                   AND i2.tablename = i1.tablename FOR XML PATH('')
                         ), 3, 8000)
     ) AS IndexName,
            CASE
                WHEN
     (
         SELECT SUBSTRING(
                         (
                             SELECT ' ,'+indexname+':'+CONVERT(VARCHAR(15), indexsize)
                             FROM #indexsize AS i2
                             WHERE i2.dbname = i1.dbname
                                   AND i2.tablename = i1.tablename FOR XML PATH('')
                         ), 3, 8000)
     ) = ''
                THEN '0'
                ELSE COUNT(*) OVER(PARTITION BY DBName,
                                                TableName)
            END AS IndexCount,
            SUM(indexsize) OVER(PARTITION BY DBName,
                                             TableName) AS TotalIndexSize
     INTO #temps
     FROM #indexsize AS i1
     ORDER BY i1.dbname,
              i1.tablename,
              i1.indexname;
     CREATE TABLE #tableReport
     (partition_id            BIGINT,
      object_id               INT,
      index_id                BIGINT,
      partition_number        INT,
      hobt_id                 BIGINT,
      rows                    BIGINT,
      filestream_filegroup_id SMALLINT,
      data_compression        TINYINT,
      data_compression_desc   NVARCHAR(60),
      [DatabaseName]          [VARCHAR](128),
      SchemaName              [SYSNAME] NOT NULL,
      [TableName]             [NVARCHAR](128) NOT NULL,
      TableType               NVARCHAR(128),
      CTEnabled               VARCHAR(20),
      IsSchemaPublished       VARCHAR(60),
      IsTablePublished        VARCHAR(60),
      IsReplicated            VARCHAR(60),
      IsTrackedbyCDC          VARCHAR(60),
      TotalColumns            VARCHAR(60),
      TableCreateDate         [DATETIME] NOT NULL,
      TableModifyDate         [DATETIME] NOT NULL,
      RowsCount               [BIGINT] NULL,
      TotalSize               [BIGINT] NULL,
      DataSize                [BIGINT] NULL,
      IndexSize               [BIGINT] NULL,
      UnusedSize              [BIGINT] NULL
     );
     INSERT INTO #tableReport
     (partition_id,
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
     EXEC sp_msforeachdb
'SELECT SP.*, db_name(db_id("?")) DatabaseName, a3.name AS [schemaname], a2.name AS [tablename], a2.type, CASE WHEN ctt.object_id IS NULL THEN ''No''
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
                   GETDATE() AS ReportRun
			INTO #Report
            FROM #tableReport AS n
                 LEFT JOIN #temps AS i ON n.DatabaseName = i.dbname
                                          AND n.TableName = i.tablename
            WHERE DatabaseName NOT LIKE 'tempdb'
            ORDER BY TotalSize DESC;

			if @ResultOnly = 1 
			select * from #Report
			else 
			begin 
			     INSERT INTO DBATasks.[dbo].[tblTableGrowthDetail]
				select * from #Report
     DELETE DBATasks.dbo.tblTableGrowthDetail
     WHERE ReportRun < DATEADD(DD, -@retaindays, GETDATE());
     DELETE DBATasks.dbo.tblTableGrowthDetail
     WHERE RowsCount < 10000
           AND ReportRun < DATEADD(DD, -4, GETDATE());
		   end
     DROP TABLE #tableReport;
     DROP TABLE #temps;
     DROP TABLE #indexsize;
GO
USE [msdb]
GO
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Collect Table Growth Data')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Collect Table Growth Data', @delete_unused_schedule=1
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Collect Table Growth Data')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Collect Table Growth Data', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Table Growth Data', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC DBATasks..spTableGrowthDetail',		
		@database_name=N'DBATasks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily 8PM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20161201, 
		@active_end_date=99991231, 
		@active_start_time=200000, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

----Rollback
--USE [DBATasks]
--GO
--IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblTableGrowthDetail]') AND type in (N'U'))
--DROP TABLE [dbo].[tblTableGrowthDetail]
--GO
--USE [msdb]
--GO
--IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Collect Table Growth Data')
--EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Collect Table Growth Data', @delete_unused_schedule=1
--GO
