--http://www.patrickkeisler.com/2013/12/collecting-historical-wait-statistics_10.html
USE DBATasks;
GO
IF OBJECT_ID('dbo.tblWaitStatsData') IS NULL
BEGIN
    CREATE TABLE dbo.tblWaitStatsData
    (
         SqlServerStartTime DATETIME NOT NULL
        ,CollectionTime DATETIME NOT NULL
        ,TimeDiff_ss INT NOT NULL
        ,WaitType NVARCHAR(60) NOT NULL
        ,WaitingTasksCountCumulative BIGINT NOT NULL
        ,WaitingTasksCountDiff INT NOT NULL
        ,WaitTimeCumulative_ms BIGINT NOT NULL
        ,WaitTimeDiff_ms INT NOT NULL
        ,MaxWaitTime_ms BIGINT NOT NULL
        ,SignalWaitTimeCumulative_ms BIGINT NOT NULL
        ,SignalWaitTimeDiff_ms INT NOT NULL
        ,CONSTRAINT PK_tblWaitStatsData PRIMARY KEY CLUSTERED (CollectionTime, WaitType)
    )
END
GO
USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spWaitStatsData]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spWaitStatsData]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spWaitStatsData]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spWaitStatsData] AS' 
END
GO

ALTER PROCEDURE [dbo].[spWaitStatsData](@retaindays INT = 1)
AS
     SET NOCOUNT ON;
     DECLARE @CurrentSqlServerStartTime DATETIME, @PreviousSqlServerStartTime DATETIME, @PreviousCollectionTime DATETIME;
     SELECT @CurrentSqlServerStartTime = sqlserver_start_time
     FROM sys.dm_os_sys_info;

-- Get the last collection time
     SELECT @PreviousSqlServerStartTime = MAX(SqlServerStartTime),
            @PreviousCollectionTime = MAX(CollectionTime)
     FROM DBATasks.dbo.tblWaitStatsData;
     IF @CurrentSqlServerStartTime <> ISNULL(@PreviousSqlServerStartTime, 0)
         BEGIN
    -- Insert starter values if SQL Server has been recently restarted
             INSERT INTO DBATasks.dbo.tblWaitStatsData
                    SELECT @CurrentSqlServerStartTime,
                           GETDATE(),
                           DATEDIFF(SS, @CurrentSqlServerStartTime, GETDATE()),
                           wait_type,
                           waiting_tasks_count,
                           0,
                           wait_time_ms,
                           0,
                           max_wait_time_ms,
                           signal_wait_time_ms,
                           0
                    FROM sys.dm_os_wait_stats;
     END;
         ELSE
         BEGIN
    -- Get the current wait stats
             WITH CurrentWaitStats
                  AS (
                  SELECT GETDATE() AS 'CollectionTime',
                         *
                  FROM sys.dm_os_wait_stats)
    -- Insert the diff values into the history table
                  INSERT INTO DBATasks.dbo.tblWaitStatsData
                         SELECT @CurrentSqlServerStartTime,
                                cws.CollectionTime,
                                DATEDIFF(SS, @PreviousCollectionTime, cws.CollectionTime),
                                cws.wait_type,
                                cws.waiting_tasks_count,
                                cws.waiting_tasks_count - hist.WaitingTasksCountCumulative,
                                cws.wait_time_ms,
                                cws.wait_time_ms - hist.WaitTimeCumulative_ms,
                                cws.max_wait_time_ms,
                                cws.signal_wait_time_ms,
                                cws.signal_wait_time_ms - hist.SignalWaitTimeCumulative_ms
                         FROM CurrentWaitStats cws
                              INNER JOIN DBATasks.dbo.tblWaitStatsData hist ON cws.wait_type = hist.WaitType
                                                                               AND hist.CollectionTime = @PreviousCollectionTime;
     END;
     DELETE [DBATasks].[dbo].[tblWaitStatsData]
     WHERE WaitType NOT IN(N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE', N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX', N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'ONDEMAND_TASK_QUEUE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PREEMPTIVE_OS_AUTHENTICATIONOPS', N'PREEMPTIVE_OS_CREATEFILE', N'PREEMPTIVE_OS_GENERICOPS', N'PREEMPTIVE_OS_LIBRARYOPS', N'PREEMPTIVE_OS_QUERYREGISTRY', N'PREEMPTIVE_HADR_LEASE_MECHANISM', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE', N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT')
     AND WaitingTasksCountCumulative = 0;
     DELETE [DBATasks].[dbo].[tblWaitStatsData]
     WHERE CollectionTime < DATEADD(DD, -@retaindays, GETDATE());
GO
USE [msdb]
GO
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Collect WaitStats Data')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Collect WaitStats Data', @delete_unused_schedule=1
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
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Collect WaitStats Data')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Collect WaitStats Data', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
/****** Object:  Step [WaitStats Data]    Script Date: 11/2/2017 10:10:34 PM ******/
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'WaitStats Data', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC DBATasks..spWaitStatsData', 
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
		@active_start_date=20171102
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO
