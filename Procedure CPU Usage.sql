USE TEMPDB
GO
CREATE OR ALTER PROCEDURE [dbo].[usp_CPUUsage]
(@LogToTable BIT = 0, @retentiondays INT = 30)
AS
     SET NOCOUNT ON;
     SET QUOTED_IDENTIFIER ON;
	 IF @LogToTable = 1
	 BEGIN
	 IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblCPUUsage]') AND type in (N'U'))
		BEGIN
		CREATE TABLE [dbo].[tblCPUUsage](
			[SQLCPUUsage] [int] NULL,
			[IdleProcess] [int] NULL,
			[RestCPUUsage] [int] NULL,
			[RunTime] [datetime] NULL
		) ON [PRIMARY]
		END
	END
     DECLARE @ts_now BIGINT=
     (
         SELECT cpu_ticks / (cpu_ticks / ms_ticks)
         FROM sys.dm_os_sys_info WITH (NOLOCK)
     );

	 IF @LogToTable = 1
	 BEGIN
     INSERT INTO tblCPUUsage
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
                                                                                   FROM tblCPUUsage
                                                                               ), DATEADD(MI, -256, GETDATE()));
     DELETE tblCPUUsage
     WHERE RunTime < DATEADD(DD, -@retentiondays, GETDATE());
	 END
	 ELSE 
	 BEGIN 
	 SELECT TOP(256) SQLProcessUtilization AS [SQL Server Process CPU Utilization], 
               SystemIdle AS [System Idle Process], 
               100 - SystemIdle - SQLProcessUtilization AS [Other Process CPU Utilization], 
               DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS [Event Time] 
FROM (SELECT record.value('(./Record/@id)[1]', 'int') AS record_id, 
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') 
			AS [SystemIdle], 
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') 
			AS [SQLProcessUtilization], [timestamp] 
	  FROM (SELECT [timestamp], CONVERT(xml, record) AS [record] 
			FROM sys.dm_os_ring_buffers WITH (NOLOCK)
			WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
			AND record LIKE N'%<SystemHealth>%') AS x) AS y 
ORDER BY record_id DESC OPTION (RECOMPILE);
	 END
GO

EXEC [usp_CPUUsage]
GO 
