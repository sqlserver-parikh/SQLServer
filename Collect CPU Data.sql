USE [DBATasks]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblCPUUsage]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblCPUUsage](
	[SQLCPUUsage] [int] NULL,
	[IdleProcess] [int] NULL,
	[RestCPUUsage] [int] NULL,
	[RunTime] [datetime] NULL
) ON [PRIMARY]
END
GO

USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spCPUUsage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spCPUUsage]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spCPUUsage]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spCPUUsage] AS' 
END
GO

ALTER PROCEDURE [dbo].[spCPUUsage](@retentiondays INT = 30)
AS
     SET NOCOUNT ON;
     SET QUOTED_IDENTIFIER ON;
     DECLARE @ts_now BIGINT=
     (
         SELECT cpu_ticks / (cpu_ticks / ms_ticks)
         FROM sys.dm_os_sys_info WITH (NOLOCK)
     );
     INSERT INTO DBATasks..tblCPUUsage
            SELECT SQLProcessUtilization AS SQLCPUUsage,
                   SystemIdle AS IdleProcess,
                   100 - SystemIdle - SQLProcessUtilization AS RestCPUUsage,
                   DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS RunTime
            FROM
            (
                SELECT record.value('(./Record/@id)[1]', 'int') AS record_id,
                       record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS [SystemIdle],
                       record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS [SQLProcessUtilization],
                       [timestamp]
                FROM
                (
                    SELECT [timestamp],
                           CONVERT(XML, record) AS [record]
                    FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                          AND record LIKE N'%<SystemHealth>%'
                ) AS x
            ) AS y
            WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > ISNULL(
                                                                               (
                                                                                   SELECT MAX(RunTime)
                                                                                   FROM DBATasks..tblCPUUsage
                                                                               ), DATEADD(MI, -256, GETDATE()));
     DELETE DBATasks..tblCPUUsage
     WHERE RunTime < DATEADD(DD, -@retentiondays, GETDATE());
GO



USE [msdb]
GO

IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Collect CPU Data')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Collect CPU Data', @delete_unused_schedule=1
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
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Collect CPU Data')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Collect CPU Data', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CPU Data Collector', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC DBATasks..spCPUUsage', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Hourly', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20171102, 
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
