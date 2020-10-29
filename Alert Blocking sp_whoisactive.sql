--Below script will create SQLAgent job, scheduled to run every 15 min to alert if there is lead blocker blocking more than 10 sessions
--Change email DL at line number 52 -- DECLARE @emailDL varchar(max) = ''SQLDBA@company.com'' 
--Prereq
--Please make sure you have database mail configured on server
--You have DBATasks database, sp_whoisactive procedure and collecting data in table tblWhoISActive. --Look up Collect sp_whoisactive on this git repository.
USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spBlockingAlert]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spBlockingAlert]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spBlockingAlert]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spBlockingAlert] AS' 
END
GO

ALTER PROCEDURE [dbo].[spBlockingAlert]
(@timecheckmin          INT          = 15,
 @emailDL               VARCHAR(MAX),
 @blocked_session_count INT          = 2
)
AS
     BEGIN
	   SELECT @emailDL = email_address FROM msdb..sysoperators
	   WHERE name LIKE 'SQLDBATeam'
         DECLARE @emailProfile VARCHAR(128);
         SELECT @emailProfile = name
         FROM msdb..sysmail_profile
         WHERE profile_id = 1;
         IF EXISTS
(
    SELECT 1
    FROM dbo.tblWhoIsActive WITH (nolock)
    WHERE 1 = 1
      --AND database_name LIKE 'DBName'
	 --AND CAST(sql_text AS VARCHAR(MAX)) LIKE '%QueryText%'
      --AND CAST(sql_command AS VARCHAR(MAX)) LIKE '%QueryText%'
          --AND (blocking_session_id IS NOT NULL
          --     OR blocked_session_count > 0)
          AND blocked_session_count > @blocked_session_count
	 --AND host_name LIKE 'ServerName'
	 --AND percent_complete > 20
	 --AND login_name LIKE 'LoginName'
	 --AND program_name LIKE '%ProgramName'
	 --and query_plan is not null
	 --and session_id = 55
          AND collection_time > DATEADD(mi, -@timecheckmin, GETDATE())
	 --AND collection_time > '2018-10-19 07:12:01.110'
	 --AND collection_time < '2018-10-19 07:29:01.110'
)
--ORDER BY tempdb_current DESC
             BEGIN
                 PRINT 'YES';
                 DECLARE @tableHTML VARCHAR(MAX);
                 SET @tableHTML = N'<table border="1">'+N'<tr>
<th>CollectTime</th>
<th>dd hh:mm:ss.mss</th>
<th>SQLCommand</th>
<th>SessionID</th>
<th>LoginName</th>
<th>BlockedBy</th>
<th>TotalBlocked</th>
<th>HostName</th>
<th>DBName</th>
<th>ProgramName</th>
<th>StartTime</th>
</tr>'+CAST(
(
    SELECT td = collection_time,
           '',
           td = [dd hh:mm:ss.mss],
           '',
           td = CASE
                    WHEN LEFT(CAST(sql_command AS VARCHAR(MAX)), 50) IS NULL
                    THEN ''
                    ELSE LEFT(CAST(sql_command AS VARCHAR(MAX)), 50)
                END,
           '',
           td = session_id,
           '',
           td = login_name,
           '',
           td = CASE
                    WHEN [blocking_session_id] IS NULL
                    THEN 'LeadBlocker'
                    ELSE CONVERT(VARCHAR(4), blocking_session_id)
                END,
           '',
           td = blocked_session_count,
           '',
           td = host_name,
           '',
           td = database_name,
           '',
           td = program_name,
           '',
           td = start_time
    FROM dbo.tblWhoIsActive WITH (nolock)
    WHERE 1 = 1
      --AND database_name LIKE 'DBName'
	 --AND CAST(sql_text AS VARCHAR(MAX)) LIKE '%QueryText%'
      --AND CAST(sql_command AS VARCHAR(MAX)) LIKE '%QueryText%'
          AND (blocking_session_id IS NOT NULL
               OR blocked_session_count > 0)
	 --AND host_name LIKE 'ServerName'
	 --AND percent_complete > 20
	 --AND login_name LIKE 'LoginName'
	 --AND program_name LIKE '%ProgramName'
	 --and query_plan is not null
	 --and session_id = 55
          AND collection_time > DATEADD(mi, -@timecheckmin, GETDATE())
	 --AND collection_time > '2018-10-19 07:12:01.110'
	 --AND collection_time < '2018-10-19 07:29:01.110'
    ORDER BY 1 DESC,
             blocked_session_count DESC,
             2 DESC FOR XML PATH('tr'), TYPE
) AS NVARCHAR(MAX))+N'</table>';
                 DECLARE @subject VARCHAR(228)= 'Heavy Blocking ON  '+CONVERT(VARCHAR(128), @@SERVERNAME);
                 EXEC msdb.dbo.sp_send_dbmail
                      @recipients = @emailDL,
                      @profile_name = @emailProfile,
                      @subject = @subject,
                      @body = @tableHTML,
                      @body_format = 'HTML';
             END;
     END;
GO



USE [msdb]
GO

IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Alert Blocking Detail')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Alert Blocking Detail', @delete_unused_schedule=1
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
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Alert Blocking Detail')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Alert Blocking Detail', 
		@enabled=1, 
		@notify_level_eventlog=2, 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Blocking Alert Detail', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [spBlockingAlert]', 
		@database_name=N'DBATasks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 15 min', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=15, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20181219, 
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

