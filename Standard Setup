USE [master];
GO
--Change error log retention from 6 to 26
EXEC xp_instance_regwrite
     N'HKEY_LOCAL_MACHINE',
     N'Software\Microsoft\MSSQLServer\MSSQLServer',
     N'NumErrorLogs',
     REG_DWORD,
     26;
GO
--Keep MSDB job history to 20k rows and max 500 rows per job
USE [msdb];
GO
EXEC msdb.dbo.sp_set_sqlagent_properties
     @jobhistory_max_rows = 20000,
     @jobhistory_max_rows_per_job = 500;
GO
IF NOT EXISTS
(
    SELECT *
    FROM sys.configurations
    WHERE name LIKE 'backup comp%'
          AND value_in_use = 1
)
    BEGIN
        EXEC sp_configure
             'backup compression default',
             1;
        RECONFIGURE WITH OVERRIDE;
END;
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_set_sqlagent_properties 
		@alert_replace_runtime_tokens=1
GO
