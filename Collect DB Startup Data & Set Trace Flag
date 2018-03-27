USE [DBATasks]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblStartupDetail]') AND type in (N'U'))
DROP TABLE [dbo].[tblStartupDetail]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblStartupDetail]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblStartupDetail](
	[ServerName] [sql_variant] NULL,
	[SQLVersion] [sql_variant] NULL,
	[ServicePack] [sql_variant] NULL,
	[RunningNode] [sql_variant] NULL,
	[IPAddress] [varchar](20) NULL,
	[AllNodes] [nvarchar](max) NULL,
	[Edition] [sql_variant] NULL,
	[ErrorLogLocation] [sql_variant] NULL,
	[DBNames] [nvarchar](max) NULL,
	[DBCount] [int] NULL,
	[TotalDataSizeMB] [decimal](10, 2) NULL,
	[TotalLogSizeMB] [decimal](10, 2) NULL,
	[ServerCollation] [sql_variant] NULL,
	[MAXDOP] [sql_variant] NULL,
	[TotalMemoryMB] [numeric](26, 6) NULL,
	[MinMemory] [sql_variant] NULL,
	[MaxMemory] [sql_variant] NULL,
	[IsClustered] [varchar](3) NULL,
	[SQLStartTime] [datetime] NULL,
	[OSRebootTime] [datetime] NULL,
	[SQLInstallDate] [datetime] NULL, 
	[ReportRunTime] [datetime] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

USE [msdb]
GO

IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Trace - Startup Detail')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Trace - Startup Detail', @delete_unused_schedule=1
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Trace - Startup Detail')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Trace - Startup Detail', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Trace ON - Startup Detail', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-traceon-trace-flags-transact-sql
DBCC TRACEON(3226  --Suppress Successful backup message from errorlog.
, 3042 --For large database backup, it will grow as needed.
, 1117 --All files in filegroup will grow, mostly used for tempdb optimization but affects all dbs with multiple files.
, 1118 --Reduce SGAM contention, in 2016 is controlled by SET MIXED_PAGE_ALLOCATION option of ALTER DATABASE and has no effect.
, 4199 --Query Optimizer
, 2371 --Auto update statistics algorithm change, in 2016 it has no effect - enabled by default.
, -1);
--DBCC TRACESTATUS
GO

INSERT INTO DBATasks..tblStartupDetail
       SELECT SERVERPROPERTY(''ServerName'') ServerName,
              SERVERPROPERTY(N''ProductVersion'') SQLVersion,
              SERVERPROPERTY(''ProductLevel'') ServicePack,
              SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') RunningNode,
       (
           SELECT DISTINCT TOP (1) CONVERT(VARCHAR(20), local_net_address)
           FROM sys.dm_exec_connections
           WHERE local_net_address IS NOT NULL
       ) IPAddress,
              CASE
                  WHEN SERVERPROPERTY(''IsClustered'') = 1
                  THEN
       (
           SELECT SUBSTRING(
                           (
                               SELECT '' ,''+NodeName
                               FROM sys.dm_os_cluster_nodes FOR xml PATH('''')
                           ), 3, 8000)
       )
                  WHEN SERVERPROPERTY(''IsClustered'') = 0
                  THEN ''Not Clustered''
              END AllNodes,
              SERVERPROPERTY(N''Edition'') Edition,
              SERVERPROPERTY(''ErrorLogFileName'') ErrorLogLocation,
       (
           SELECT SUBSTRING(
                           (
                               SELECT '' ,''+QUOTENAME(name)
                               FROM sys.sysdatabases
                               WHERE dbid > 4 FOR XML PATH('''')
                           ), 3, 8000)
       ) DBNames,
       (
           SELECT COUNT(*)
           FROM sys.sysdatabases
           WHERE dbid > 4
       ) DBCount,
       (
           SELECT CAST(cntr_value / 1024.0 AS DECIMAL(10, 2))
           FROM sys.dm_os_performance_counters
           WHERE instance_name LIKE ''%_Total%''
                 AND counter_name LIKE ''Data File(s) Size (KB)%''
       ) TotalDataSizeMB,
       (
           SELECT CAST(cntr_value / 1024.0 AS DECIMAL(10, 2))
           FROM sys.dm_os_performance_counters
           WHERE instance_name LIKE ''%_Total%''
                 AND counter_name LIKE ''Log File(s) Size (KB)%''
       ) TotalLogSizeMB,
              SERVERPROPERTY(''Collation'') ServerCollation,
       (
           SELECT value_in_use
           FROM sys.configurations
           WHERE name LIKE ''max degree of parallelism''
       ) MAXDOP,
       (
           SELECT total_physical_memory_kb / 1024.0
           FROM sys.dm_os_sys_memory
       ) TotalMemoryMB,
       (
           SELECT value_in_use
           FROM sys.configurations
           WHERE name LIKE ''min server memory (MB)''
       ) MinMemory,
       (
           SELECT value_in_use
           FROM sys.configurations
           WHERE name LIKE ''max server memory (MB)''
       ) MaxMemory,
              CASE
                  WHEN SERVERPROPERTY(''IsClustered'') = 0
                  THEN ''No''
                  WHEN SERVERPROPERTY(''IsClustered'') = 1
                  THEN ''Yes''
              END IsClustered,
       (
           SELECT create_date
           FROM sys.databases
           WHERE name LIKE ''tempdb''
       ) SQLStartTime,
       (
           SELECT DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE())
           FROM sys.[dm_os_sys_info]
       ) OSRebootTime,
	  	  (
        SELECT create_date
        FROM sys.server_principals
        WHERE sid = 0x010100000000000512000000
	   ) SQLInstallDate,
        GETDATE() ReportRunTime;
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'SQL Restart', 
		@enabled=1, 
		@freq_type=64, 
		@freq_interval=0, 
		@freq_subday_type=0, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20171201, 
		@active_end_date=99991231, 
		@active_start_time=0, 
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
EXEC msdb..sp_start_job @job_name = 'DBA - Trace - Startup Detail' 
