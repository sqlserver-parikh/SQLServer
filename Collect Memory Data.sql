USE [DBATasks]
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblMemoryUsage]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblMemoryUsage](
	[PhysicalMemory(MB)] [bigint] NULL,
	[AvailableMemory(MB)] [bigint] NULL,
	[TotalPageFile(MB)] [bigint] NULL,
	[AvailablePageFile(MB)] [bigint] NULL,
	[SystemCache(MB)] [bigint] NULL,
	[SystemMemoryState] [nvarchar](256) NOT NULL,
	[RunTime] [datetime2](7) NOT NULL
) ON [PRIMARY]
END

USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spMemoryUsage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spMemoryUsage]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spMemoryUsage]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spMemoryUsage] AS' 
END
GO

ALTER PROCEDURE [dbo].[spMemoryUsage](@retaindays INT = 4)
AS
     INSERT INTO DBATasks..tblMemoryUsage
            SELECT total_physical_memory_kb / 1024 AS [PhysicalMemory(MB)],
                   available_physical_memory_kb / 1024 AS [AvailableMemory(MB)],
                   total_page_file_kb / 1024 AS [TotalPageFile(MB)],
                   available_page_file_kb / 1024 AS [AvailablePageFile(MB)],
                   system_cache_kb / 1024 AS [SystemCache(MB)],
                   system_memory_state_desc AS [SystemMemoryState],
                   GETDATE() RunTime
            FROM sys.dm_os_sys_memory WITH (NOLOCK) OPTION(RECOMPILE);
     DELETE DBATasks..tblMemoryUsage
     WHERE RunTime < DATEADD(DD, -@retaindays, GETDATE());
GO
USE [msdb]
GO
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Collect Memory Data')
EXEC msdb.dbo.sp_delete_job @job_name = N'DBA - Collect Memory Data', @delete_unused_schedule=1
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
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Collect Memory Data')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Collect Memory Data', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Memory Collection', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC DBATasks..spMemoryUsage', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 5 Min', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20171103, 
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
