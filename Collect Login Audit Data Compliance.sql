USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblAuditLogins]') AND type in (N'U'))
DROP TABLE [dbo].[tblAuditLogins]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblAuditLogins]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblAuditLogins](
	[EventName] [nvarchar](128) NULL,
	[subclass_name] [nvarchar](128) NULL,
	[DatabaseName] [nvarchar](256) NULL,
	[DatabaseID] [int] NULL,
	[NTDomainName] [nvarchar](256) NULL,
	[ApplicationName] [nvarchar](256) NULL,
	[LoginName] [nvarchar](256) NULL,
	[SPID] [int] NULL,
	[StartTime] [datetime] NULL,
	[RoleName] [nvarchar](256) NULL,
	[TargetUserName] [nvarchar](256) NULL,
	[TargetLoginName] [nvarchar](256) NULL,
	[SessionLoginName] [nvarchar](256) NULL
) ON [PRIMARY]
END
GO

USE [msdb]
GO

IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Audit Logins')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Audit Logins', @delete_unused_schedule=1
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
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Audit Logins')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Audit Logins', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Audit Logins', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
IF EXISTS
(
    SELECT 1
    FROM tblAuditLogins
)
    BEGIN
        INSERT INTO tblAuditLogins
               SELECT TE.name AS [EventName],
                      v.subclass_name,
                      T.DatabaseName,
                      t.DatabaseID,
                      t.NTDomainName,
                      t.ApplicationName,
                      t.LoginName,
                      t.SPID,
                      t.StartTime,
                      t.RoleName,
                      t.TargetUserName,
                      t.TargetLoginName,
                      t.SessionLoginName
               FROM sys.fn_trace_gettable
               (CONVERT(VARCHAR(150),
                       (
                           SELECT TOP 1 f.[value]
                           FROM sys.fn_trace_getinfo(NULL) f
                           WHERE f.property = 2
                       )), DEFAULT
               ) T
                    JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
                    JOIN sys.trace_subclass_values v ON v.trace_event_id = TE.trace_event_id
                                                        AND v.subclass_value = t.EventSubClass
               WHERE te.name IN(''Audit Addlogin Event'', ''Audit Add DB User Event'', ''Audit Add Member to DB Role Event'', ''Audit Add Login to Server Role Event'')
               AND StartTime > DATEADD(HH, -4, GETDATE())
               AND StartTime >
               (
                   SELECT MAX(StartTime)
                   FROM tblAuditLogins
               )
               ORDER BY StartTime DESC;
END;
    ELSE
    BEGIN
        INSERT INTO tblAuditLogins
        SELECT TE.name AS [EventName],
               v.subclass_name,
               T.DatabaseName,
               t.DatabaseID,
               t.NTDomainName,
               t.ApplicationName,
               t.LoginName,
               t.SPID,
               t.StartTime,
               t.RoleName,
               t.TargetUserName,
               t.TargetLoginName,
               t.SessionLoginName
        FROM sys.fn_trace_gettable
        (CONVERT(VARCHAR(150),
                (
                    SELECT TOP 1 f.[value]
                    FROM sys.fn_trace_getinfo(NULL) f
                    WHERE f.property = 2
                )), DEFAULT
        ) T
             JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
             JOIN sys.trace_subclass_values v ON v.trace_event_id = TE.trace_event_id
                                                 AND v.subclass_value = t.EventSubClass
        WHERE te.name IN(''Audit Addlogin Event'', ''Audit Add DB User Event'', ''Audit Add Member to DB Role Event'', ''Audit Add Login to Server Role Event'')
        ORDER BY StartTime DESC;
END;', 
		@database_name=N'DBATasks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every Hour', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20191118, 
		@active_end_date=99991231, 
		@active_start_time=400, 
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

