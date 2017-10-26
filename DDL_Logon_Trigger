USE master;
GO
IF OBJECT_ID('AuditEvents', 'U') IS  NULL
BEGIN
CREATE TABLE dbo.AuditEvents
(EventDate    DATETIME NOT NULL
                       DEFAULT CURRENT_TIMESTAMP,
 EventType    NVARCHAR(64),
 EventDDL     NVARCHAR(MAX),
 EventXML     XML,
 DatabaseName NVARCHAR(255),
 SchemaName   NVARCHAR(255),
 ObjectName   NVARCHAR(255),
 HostName     NVARCHAR(64),
 IPAddress    NVARCHAR(32),
 ProgramName  NVARCHAR(255),
 LoginName    NVARCHAR(255)
);

CREATE NONCLUSTERED INDEX [IX_AuditEvents_EventDate] ON [dbo].[AuditEvents]
(
	[EventDate] ASC
) ON [PRIMARY]

END 
USE master;
GO
IF EXISTS (SELECT 1 FROM sys.server_triggers WHERE name = 'DDLTrigger')
DROP TRIGGER [DDLTrigger] ON ALL SERVER
GO
CREATE TRIGGER DDLTrigger
ON ALL SERVER WITH EXECUTE AS 'sa'
FOR DDL_SERVER_LEVEL_EVENTS, DDL_DATABASE_LEVEL_EVENTS
AS
     BEGIN
		 SET ANSI_NULLS ON; 
		 SET ANSI_PADDING ON; 
		 SET QUOTED_IDENTIFIER ON; 
		 SET CONCAT_NULL_YIELDS_NULL ON;
         SET NOCOUNT ON;
         DECLARE @EventData XML= EVENTDATA();
         DECLARE @ip NVARCHAR(32)=
                 (
                 SELECT top (1) client_net_address
                 FROM sys.dm_exec_connections
                        WHERE session_id = @@SPID
                 );

         INSERT INTO master.dbo.AuditEvents
         (EventType,
          EventDDL,
          EventXML,
          DatabaseName,
          SchemaName,
          ObjectName,
          HostName,
          IPAddress,
          ProgramName,
          LoginName
         )
                SELECT @EventData.value('(/EVENT_INSTANCE/EventType)[1]', 'NVARCHAR(64)'),
                       @EventData.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'NVARCHAR(MAX)'),
                       @EventData,
                       DB_NAME(),
                       @EventData.value('(/EVENT_INSTANCE/SchemaName)[1]', 'NVARCHAR(255)'),
                       @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'NVARCHAR(255)'),
                       HOST_NAME(),
                       @ip,
                       PROGRAM_NAME(),
                       ORIGINAL_LOGIN();
     END;
GO
USE [master];
GO
IF  EXISTS (SELECT 1 FROM sys.server_triggers WHERE name = 'AuditLogins')
DROP TRIGGER [AuditLogins] ON ALL SERVER
GO
CREATE TRIGGER AuditLogins
ON ALL SERVER WITH EXECUTE AS 'sa'
FOR LOGON
AS
		 SET ANSI_NULLS ON; 
		 SET ANSI_PADDING ON; 
		 SET QUOTED_IDENTIFIER ON; 
		 SET CONCAT_NULL_YIELDS_NULL ON;
     BEGIN
         IF PROGRAM_NAME() LIKE 'Microsoft SQL Server Management Studio'
            OR PROGRAM_NAME() LIKE 'Microsoft SQL Server Management Studio - Query'
             BEGIN
                 DECLARE @EventData XML= EVENTDATA();
                 DECLARE @ip VARCHAR(32)=
                         (
                         SELECT TOP (1) client_net_address
                         FROM sys.dm_exec_connections
                                WHERE session_id = @@SPID
                         );
					
                 INSERT INTO master.dbo.AuditEvents
                 (EventType,
                  EventDDL,
                  EventXML,
                  DatabaseName,
                  SchemaName,
                  ObjectName,
                  HostName,
                  IPAddress,
                  ProgramName,
                  LoginName
                 )
                        SELECT @EventData.value('(/EVENT_INSTANCE/EventType)[1]', 'NVARCHAR(100)'),
                               @EventData.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'NVARCHAR(MAX)'),
                               @EventData,
                               DB_NAME(),
                               @EventData.value('(/EVENT_INSTANCE/SchemaName)[1]', 'NVARCHAR(255)'),
                               @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'NVARCHAR(255)'),
                               HOST_NAME(),
                               @ip,
                               PROGRAM_NAME(),
                               ORIGINAL_LOGIN();
             END;
     END;
GO
ENABLE TRIGGER AuditLogins ON ALL SERVER;
GO

USE [msdb]
GO
IF EXISTS (SELECT 1 FROM msdb..sysjobs WHERE name LIKE N'DBA_AuditEvents_Export_Cleanup')
BEGIN
EXEC msdb.dbo.sp_delete_job @job_name = N'DBA_AuditEvents_Export_Cleanup',  @delete_unused_schedule=1
END
GO

USE [msdb]
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
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA_AuditEvents_Export_Cleanup')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA_AuditEvents_Export_Cleanup', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'To clean up all audit events older than 1 year.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
/****** Object:  Step [Audit_Export]    Script Date: 10/20/2017 1:36:55 PM ******/
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Audit_Export', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'PowerShell', 
		@command=N'$Folder = invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database master -Query "SELECT REPLACE(CONVERT(VARCHAR(MAX),SERVERPROPERTY(''ErrorLogFileName'')),''ERRORLOG'','''') + ''AuditEvents_'' +  CONVERT(varchar(10),GETDATE(),112) + ''.csv'' AS FileName,
REPLACE(CONVERT(VARCHAR(MAX),SERVERPROPERTY(''ErrorLogFileName'')),''ERRORLOG'','''') AS RemoveFile"

invoke-sqlcmd -ServerInstance $(ESCAPE_SQUOTE(SRVR))  -Database master -Query "

SELECT *
  FROM [master].[dbo].[AuditEvents] WITH (NOLOCK)
  WHERE EventDate > DATEADD(DD,-1,GETDATE())

" | export-csv -Path ($Folder.FileName)  -NoTypeInformation

$limit = (Get-Date).AddDays(-5)

# Delete files older than the $limit.
Get-ChildItem -Path $Folder.RemoveFile -Recurse -include *.csv -Force | Where-Object { !$_.PSIsContainer -and $_.CreationTime -lt $limit } | Remove-Item -Force

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Cleanup_AuditEvents]    Script Date: 10/20/2017 1:36:55 PM ******/
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 2)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Cleanup_AuditEvents', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE master..AuditEvents
WHERE EventDate < DATEADD(DD,-5,GETDATE())', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily_11PM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20170615, 
		@active_end_date=99991231, 
		@active_start_time=230000, 
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



----ROLLBACK Audit Triggers and Job
--USE [master]
--GO
--DROP TRIGGER [AuditLogins] ON ALL SERVER
--GO
--USE [master]
--GO
--DROP TRIGGER [DDLTrigger] ON ALL SERVER
--GO
----DROP TABLE master..AuditEvents
--USE [msdb]
--GO
--EXEC msdb.dbo.sp_delete_job @job_name = N'DBA_AuditEvents_Export_Cleanup',  @delete_unused_schedule=1
--GO

----https://technet.microsoft.com/en-us/library/ms186456(v=sql.90).aspx

