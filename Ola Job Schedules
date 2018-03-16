--https://ola.hallengren.com/scripts/MaintenanceSolution.sql 

IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'DatabaseBackup - SYSTEM_DATABASES - FULL'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'DatabaseBackup - SYSTEM_DATABASES - FULL',
         @name = N'Daily 10PM',
         @enabled = 1,
         @freq_type = 4,
         @freq_interval = 1,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 220000,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'CommandLog Cleanup'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'CommandLog Cleanup',
         @name = N'Weekly - Sunday - 10PM',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 1,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 220000,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'DatabaseBackup - USER_DATABASES - FULL'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'DatabaseBackup - USER_DATABASES - FULL',
         @name = N'Daily 10:15PM',
         @enabled = 1,
         @freq_type = 4,
         @freq_interval = 1,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 221500,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'DatabaseBackup - USER_DATABASES - LOG'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'DatabaseBackup - USER_DATABASES - LOG',
         @name = N'Every 15 min',
         @enabled = 1,
         @freq_type = 4,
         @freq_interval = 1,
         @freq_subday_type = 4,
         @freq_subday_interval = 15,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 0,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'DatabaseIntegrityCheck - SYSTEM_DATABASES'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'DatabaseIntegrityCheck - SYSTEM_DATABASES',
         @name = N'Weekly - Sunday - 22:30',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 1,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 223000,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'DatabaseIntegrityCheck - USER_DATABASES'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'DatabaseIntegrityCheck - USER_DATABASES',
         @name = N'Weekly - Sunday - 22:45',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 1,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 224500,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'IndexOptimize - USER_DATABASES'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'IndexOptimize - USER_DATABASES',
         @name = N'Weekly - Saturday - 23:00',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 64,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 230000,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'Output File Cleanup'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'Output File Cleanup',
         @name = N'Weekly - Saturday - 21:00',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 64,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 210000,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'sp_delete_backuphistory'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'sp_delete_backuphistory',
         @name = N'Weekly - Saturday - 21:15',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 64,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 211500,
         @active_end_time = 235959;
IF NOT EXISTS
(
    SELECT 1
    FROM [msdb].[dbo].[sysjobschedules] sjs
         INNER JOIN msdb..sysjobs sj ON sjs.job_id = sj.job_id
    WHERE sj.name LIKE 'sp_purge_jobhistory'
)
    EXEC msdb.dbo.sp_add_jobschedule
         @job_name = N'sp_purge_jobhistory',
         @name = N'Weekly - Saturday - 21:30',
         @enabled = 1,
         @freq_type = 8,
         @freq_interval = 64,
         @freq_subday_type = 1,
         @freq_subday_interval = 0,
         @freq_relative_interval = 0,
         @freq_recurrence_factor = 1,
         @active_start_date = 20160606,
         @active_end_date = 99991231,
         @active_start_time = 213000,
         @active_end_time = 235959;
