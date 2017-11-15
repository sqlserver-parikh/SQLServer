USE [DBATasks];
IF NOT EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[tblDiskData]')
          AND type IN(N'U')
)
CREATE TABLE [dbo].[tblDiskData]
([DriveLetter] [VARCHAR](512) NULL,
 [BlockSize]   [VARCHAR](100) NOT NULL,
 [CapacityMB]    [VARCHAR](100) NULL,
 [FreeSpaceMB]   [VARCHAR](100) NULL,
 [FreePct]     [VARCHAR](100) NULL,
 [RunDate]     [DATETIME2](7) NULL
                              DEFAULT(GETDATE())
)
ON [PRIMARY];
GO
IF EXISTS
(
    SELECT *
    FROM sys.views
    WHERE object_id = OBJECT_ID(N'[dbo].[vwDiskData]')
)
    DROP VIEW [dbo].[vwDiskData];
GO
CREATE VIEW [dbo].[vwDiskData]
AS
     SELECT [DriveLetter],
            [BlockSize],
            [CapacityMB],
            [FreeSpaceMB],
            [FreePct]
     FROM [DBATasks].[dbo].[tblDiskData];
GO
USE [msdb];
GO
IF EXISTS
(
    SELECT job_id
    FROM msdb.dbo.sysjobs_view
    WHERE name = N'DBA - Collect Disk Space Data'
)
    EXEC msdb.dbo.sp_delete_job
         @job_name = N'DBA - Collect Disk Space Data',
         @delete_unused_schedule = 1;
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
SELECT @jobId = job_id
FROM msdb.dbo.sysjobs
WHERE(name = N'DBA - Collect Disk Space Data');
IF(@jobId IS NULL)
    BEGIN
        EXEC @ReturnCode = msdb.dbo.sp_add_job
             @job_name = N'DBA - Collect Disk Space Data',
             @enabled = 1,
             @notify_level_eventlog = 0,
             @notify_level_email = 0,
             @notify_level_netsend = 0,
             @notify_level_page = 0,
             @delete_level = 0,
             @description = N'This job will collect hourly disk space detail',
             @category_name = N'Database Maintenance',
             @owner_login_name = N'sa',
             @job_id = @jobId OUTPUT;
        IF(@@ERROR <> 0
           OR @ReturnCode <> 0)
            GOTO QuitWithRollback;
END;

/****** Object:  Step [PS-DiskSpaceReport]    Script Date: 10/27/2017 5:09:51 PM ******/

IF NOT EXISTS
(
    SELECT *
    FROM msdb.dbo.sysjobsteps
    WHERE job_id = @jobId
          AND step_id = 1
)
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
         @job_id = @jobId,
         @step_name = N'PS-DiskSpaceReport',
         @step_id = 1,
         @cmdexec_success_code = 0,
         @on_success_action = 3,
         @on_success_step_id = 0,
         @on_fail_action = 2,
         @on_fail_step_id = 0,
         @retry_attempts = 5,
         @retry_interval = 5,
         @os_run_priority = 0,
         @subsystem = N'PowerShell',
         @command = N'Get-WmiObject win32_volume  | Where-Object { ($_.drivetype -eq 3)}  | select name, BlockSize, @{Name="Capacity(GB)";expression={[math]::round(($_.Capacity/ 1048576),2)}}, @{Name="FreeSpace(GB)";expression={[math]::round(($_.FreeSpace / 1048576),2)}},@{Name="Free(%)";expression={[math]::round(((($_.FreeSpace / 1048576)/($_.Capacity / 1048576)) * 100),0)}} |Export-Csv "C:\temp\DiskSpaceReport.csv" -NoTypeInformation -Delimiter ","
',
         @database_name = N'master',
         @flags = 0;
IF(@@ERROR <> 0
   OR @ReturnCode <> 0)
    GOTO QuitWithRollback;

IF NOT EXISTS
(
    SELECT *
    FROM msdb.dbo.sysjobsteps
    WHERE job_id = @jobId
          AND step_id = 2
)
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
         @job_id = @jobId,
         @step_name = N'InsertData',
         @step_id = 2,
         @cmdexec_success_code = 0,
         @on_success_action = 1,
         @on_success_step_id = 0,
         @on_fail_action = 2,
         @on_fail_step_id = 0,
         @retry_attempts = 0,
         @retry_interval = 0,
         @os_run_priority = 0,
         @subsystem = N'TSQL',
         @command = N'WAITFOR DELAY ''00:00:10''; 
	    BULK INSERT vwDiskData FROM "c:\temp\DiskSpaceReport.csv" WITH(FIELDTERMINATOR = ''","'', FIRSTROW = 2, ROWTERMINATOR = ''"\n'');
UPDATE DBATasks.dbo.tblDiskData
  SET
      DriveLetter = REPLACE(DriveLetter, ''"'', '''')
WHERE RunDate > DATEADD(MI, -10, GETDATE());
GO
DELETE DBATasks.dbo.tblDiskData
WHERE DriveLetter LIKE ''\\%''
GO
DELETE DBATasks.dbo.tblDiskData
WHERE RunDate < DATEADD(YY,-2,GETDATE())
',
         @database_name = N'DBATasks',
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
     @name = N'Daily',
     @enabled = 1,
     @freq_type = 4,
     @freq_interval = 1,
     @freq_subday_type = 1,
     @freq_subday_interval = 1,
     @freq_relative_interval = 0,
     @freq_recurrence_factor = 0,
     @active_start_date = 20161116,
     @active_end_date = 99991231,
     @active_start_time = 230000;
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
EXEC msdb..sp_start_job
     @job_name = 'DBA - Collect Disk Space Data';
