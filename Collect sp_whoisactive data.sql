
USE msdb;
DECLARE @destination_table VARCHAR(4000);
SET @destination_table = 'tblWhoIsActive';
DECLARE @schema VARCHAR(4000);
EXEC sp_WhoIsActive
     @get_transaction_info = 1,
     @get_plans = 1,
     @return_schema = 1,
     @get_full_inner_text = 1,
     @get_outer_command = 1,
     @find_block_leaders = 1,
     @schema = @schema OUTPUT;
SET @schema = REPLACE(@schema, '<table_name>', @destination_table);
PRINT @schema;
IF NOT EXISTS
(
    SELECT 1
    FROM sys.tables
    WHERE name LIKE 'tblWhoIsActive'
)
    EXEC (@schema);
GO
USE [msdb];
GO

IF EXISTS
(
    SELECT 1
    FROM msdb..sysjobs
    WHERE name LIKE 'DBA - Collect spWhoIsActive Data'
)
    EXEC msdb.dbo.sp_delete_job
         @job_name = N'DBA - Collect spWhoIsActive Data';--, @delete_unused_schedule=1
GO

BEGIN TRANSACTION;
DECLARE @ReturnCode INT;
SELECT @ReturnCode = 0;

IF NOT EXISTS
(
    SELECT name
    FROM msdb.dbo.syscategories
    WHERE name = N'Database Maintenance'
          AND category_class = 1
)
    BEGIN
        EXEC @ReturnCode = msdb.dbo.sp_add_category
             @class = N'JOB',
             @type = N'LOCAL',
             @name = N'Database Maintenance';
        IF(@@ERROR <> 0
           OR @ReturnCode <> 0)
            GOTO QuitWithRollback;
    END;
DECLARE @jobId BINARY(16);
EXEC @ReturnCode = msdb.dbo.sp_add_job
     @job_name = N'DBA - Collect spWhoIsActive Data',
     @enabled = 1,
     @notify_level_eventlog = 0,
     @notify_level_email = 0,
     @notify_level_netsend = 0,
     @notify_level_page = 0,
     @delete_level = 0,
     @description = N'No description available.',
     @category_name = N'Database Maintenance',
     @owner_login_name = N'sa',
     @job_id = @jobId OUTPUT;
IF(@@ERROR <> 0
   OR @ReturnCode <> 0)
    GOTO QuitWithRollback;

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
     @job_id = @jobId,
     @step_name = N'ActivePRocesses',
     @step_id = 1,
     @cmdexec_success_code = 0,
     @on_success_action = 1,
     @on_success_step_id = 0,
     @on_fail_action = 2,
     @on_fail_step_id = 0,
     @retry_attempts = 0,
     @retry_interval = 0,
     @os_run_priority = 0,
     @subsystem = N'TSQL',
     @command = N'DECLARE
    @destination_table VARCHAR(4000) ,
    @msg NVARCHAR(1000) ;
SET @destination_table = ''tblWhoIsActive'' ;
DECLARE @numberOfRuns INT ;
SET @numberOfRuns = 5 ;
WHILE @numberOfRuns > 0
    BEGIN;
        EXEC dbo.sp_WhoIsActive @get_transaction_info = 1, @get_plans = 1,@get_full_inner_text = 1,
@get_outer_command = 1,
@find_block_leaders = 1,
            @destination_table = @destination_table ;
        SET @numberOfRuns = @numberOfRuns - 1 ;
        IF @numberOfRuns > 0
            BEGIN
                SET @msg = CONVERT(CHAR(19), GETDATE(), 121) + '': '' +
                 ''Logged info. Waiting...''
                RAISERROR(@msg,0,0) WITH nowait ;
                WAITFOR DELAY ''00:01:00''
            END
        ELSE
            BEGIN
                SET @msg = CONVERT(CHAR(19), GETDATE(), 121) + '': '' + ''Done.''
                RAISERROR(@msg,0,0) WITH nowait ;
            END
    END ;
GO

DELETE FROM tblWhoIsActive
WHERE collection_time < DATEADD(HH, -24, GETDATE())
      AND CONVERT(INT, RTRIM(LTRIM((REPLACE(tempdb_current, '','', ''''))))) < 400000
      AND (blocking_session_id IS NULL
           AND blocked_session_count = 0);
GO
DELETE FROM tblWhoIsActive
WHERE collection_time < DATEADD(DD, -15, GETDATE())
      AND CONVERT(INT, RTRIM(LTRIM((REPLACE(tempdb_current, '','', ''''))))) < 400000
      AND (blocking_session_id IS NOT NULL
           AND blocked_session_count = 0);
GO
DELETE FROM tblWhoIsActive
WHERE collection_time < DATEADD(DD, -30, GETDATE());', 
     @database_name = N'msdb',
     @flags = 0;
IF(@@ERROR <> 0
   OR @ReturnCode <> 0)
    GOTO QuitWithRollback;
EXEC @ReturnCode = msdb.dbo.sp_update_job
     @job_id = @jobId,
     @start_step_id = 1;
IF(@@ERROR <> 0
   OR @ReturnCode <> 0)
    GOTO QuitWithRollback;
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule
     @job_id = @jobId,
     @name = N'Every 5min',
     @enabled = 1,
     @freq_type = 4,
     @freq_interval = 1,
     @freq_subday_type = 4,
     @freq_subday_interval = 5,
     @freq_relative_interval = 0,
     @freq_recurrence_factor = 0,
     @active_start_date = 20160829,
     @active_end_date = 99991231,
     @active_start_time = 0,
     @active_end_time = 235959;
IF(@@ERROR <> 0
   OR @ReturnCode <> 0)
    GOTO QuitWithRollback;
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver
     @job_id = @jobId,
     @server_name = N'(local)';
IF(@@ERROR <> 0
   OR @ReturnCode <> 0)
    GOTO QuitWithRollback;
COMMIT TRANSACTION;
GOTO EndSave;
QuitWithRollback:
IF(@@TRANCOUNT > 0)
    ROLLBACK TRANSACTION;
EndSave:
GO
USE msdb;
GO
sp_start_job
 'DBA - Collect spWhoIsActive Data';
GO
USE msdb;
DECLARE @destination_table NVARCHAR(2000), @dSQL NVARCHAR(4000);
SET @destination_table = 'tblWhoIsActive';
SET @dSQL = N'SELECT top 1000 collection_time, * FROM dbo.'+QUOTENAME(@destination_table)+N' with (nolock)
  where  1= 1
 -- and database_name    like ''DBName'' 
 --and cast(sql_text as varchar(max)) like ''%-- select * from%'' 
 and collection_time > dateadd(hh,-9,getdate()) order by 1 desc';
PRINT @dSQL;
EXEC sp_executesql
     @dSQL;



/*

dbo.sp_WhoIsActive

	--If 1, gets the full stored procedure or running batch, when available
	--If 0, gets only the actual statement that is currently running in the batch or procedure
	@get_full_inner_text  = 1,
	
	--Get associated query plans for running tasks, if available
	--If @get_plans = 1, gets the plan based on the request's statement offset
	--If @get_plans = 2, gets the entire plan based on the request's plan_handle
	@get_plans  = 2,
	
	--Get the associated outer ad hoc query or stored procedure call, if available
	@get_outer_command  = 1,

	--Enables pulling transaction log write info and transaction duration
	@get_transaction_info  = 0,

	--Get information on active tasks, based on three interest levels
	--Level 0 does not pull any task-related information
	--Level 1 is a lightweight mode that pulls the top non-CXPACKET wait, giving preference to blockers
	--Level 2 pulls all available task-based metrics, including: 
	--number of active tasks, current wait stats, physical I/O, context switches, and blocker information
	@get_task_info  = 2,

	--Gets associated locks for each request, aggregated in an XML format
	@get_locks  = 1,

	--Get additional non-performance-related information about the session or request
	--text_size, language, date_format, date_first, quoted_identifier, arithabort, ansi_null_dflt_on, 
	--ansi_defaults, ansi_warnings, ansi_padding, ansi_nulls, concat_null_yields_null, 
	--transaction_isolation_level, lock_timeout, deadlock_priority, row_count, command_type
	--
	--If a SQL Agent job is running, an subnode called agent_info will be populated with some or all of
	--the following: job_id, job_name, step_id, step_name, msdb_query_error (in the event of an error)
	--
	--If @get_task_info is set to 2 and a lock wait is detected, a subnode called block_info will be
	--populated with some or all of the following: lock_type, database_name, object_id, file_id, hobt_id, 
	--applock_hash, metadata_resource, metadata_class_id, object_name, schema_name
	@get_additional_info  = 1,
	
	--Walk the blocking chain and count the number of 
	--total SPIDs blocked all the way down by a given session
	--Also enables task_info Level 1, if @get_task_info is set to 0
	@find_block_leaders  = 1,
	
	--Pull deltas on various metrics
	--Interval in seconds to wait before doing the second data pull
	@delta_interval  = 60

*/
