USE [master]
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblDeadlockEvents]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblDeadlockEvents](
 [AlertTime] [datetime] NULL,
 [DeadlockGraph] [xml] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO 

USE [msdb]
GO
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Save Deadlock Graph')
BEGIN
EXEC msdb.dbo.sp_delete_job @job_name = N'DBA - Save Deadlock Graph',  @delete_unused_schedule=1
END
BEGIN
EXEC msdb.dbo.sp_add_job @job_name=N'DBA - Save Deadlock Graph',
  @enabled=1,
  @notify_level_eventlog=2,
  @notify_level_email=0,
  @notify_level_netsend=0,
  @notify_level_page=0,
  @delete_level=0,
  @description=N'Job for responding to DEADLOCK_GRAPH events',
  @category_name=N'[Uncategorized (Local)]',
  @owner_login_name=N'sa'
EXEC  msdb.dbo.sp_add_jobstep @job_name=N'DBA - Save Deadlock Graph', @step_name=N'Insert graph into LogEvents',
  @step_id=1,
  @cmdexec_success_code=0,
  @on_success_action=1,
  @on_success_step_id=0,
  @on_fail_action=2,
  @on_fail_step_id=0,
  @retry_attempts=0,
  @retry_interval=0,
  @os_run_priority=0, @subsystem=N'TSQL',
  @command=N'INSERT INTO master..tblDeadlockEvents 
                (AlertTime, DeadlockGraph) 
                VALUES (getdate(), N''$(ESCAPE_SQUOTE(WMI(TextData)))'')
				GO
				DELETE master..tblDeadlockEvents
WHERE AlertTime < DATEADD(DD,-15,GETDATE());',
  @database_name=N'master',
  @flags=0
EXEC msdb.dbo.sp_add_jobserver @job_name=N'DBA - Save Deadlock Graph', @server_name = @@SERVERNAME
End
-- Add an alert that responds to all DEADLOCK_GRAPH events for 
-- the default instance. To monitor deadlocks for a different instance, 
-- change MSSQLSERVER to the name of the instance. 
use msdb
DECLARE @wminamespace varchar(max)
if ISNULL(SERVERPROPERTY('InstanceName'),'Default') = 'Default'
set @wminamespace = '\\.\root\Microsoft\SqlServer\ServerEvents\MSSQLSERVER'
else
set  @wminamespace = '\\.\root\Microsoft\SqlServer\ServerEvents\' + CONVERT(VARCHAR(128),SERVERPROPERTY('InstanceName'))

IF  EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Respond to DEADLOCK')
EXEC msdb.dbo.sp_delete_alert @name=N'Respond to DEADLOCK'

EXEC msdb.dbo.sp_add_alert @name=N'Respond to DEADLOCK',
  @message_id=0,
  @severity=0,
  @enabled=1,
  @delay_between_responses=0,
  @include_event_description_in=5,
  @category_name=N'[Uncategorized]',
  @wmi_namespace=@wminamespace,
  @wmi_query=N'SELECT * FROM DEADLOCK_GRAPH',
  @job_name=N'DBA - Save Deadlock Graph'
GO