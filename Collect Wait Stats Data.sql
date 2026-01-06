USE tempdb
GO

IF OBJECT_ID('dbo.usp_WaitStatsData') IS NULL
    EXEC('CREATE PROCEDURE dbo.usp_WaitStatsData AS SET NOCOUNT ON;');
GO

ALTER PROCEDURE [dbo].[usp_WaitStatsData]
(
    @retaindays INT = 14,
    @LogToTable BIT = 1,           -- 1 = Create table/Save/Clean, 0 = Immediate Snapshot
    @LiveDelaySeconds INT = 10     -- Duration for the live snapshot (only used if @LogToTable = 0)
)
AS
BEGIN
    SET NOCOUNT ON;

    ---------------------------------------------------------------------------
    -- MODE 1: PERSISTENT LOGGING (For Automation/SQL Agent)
    ---------------------------------------------------------------------------
    IF @LogToTable = 1
    BEGIN
        -- Table creation ONLY happens if LogToTable is enabled
        IF OBJECT_ID('dbo.tblWaitStatsData') IS NULL
        BEGIN
            CREATE TABLE dbo.tblWaitStatsData
            (
                 SqlServerStartTime DATETIME NOT NULL
                ,CollectionTime DATETIME NOT NULL
                ,WaitType NVARCHAR(60) NOT NULL
                ,WaitTimeDiff_ms BIGINT NOT NULL
                ,WaitTasksDiff BIGINT NOT NULL
                ,SignalWaitDiff_ms BIGINT NOT NULL
                ,WaitTimeCumulative_ms BIGINT NOT NULL
                ,WaitingTasksCountCumulative BIGINT NOT NULL
                ,SignalWaitTimeCumulative_ms BIGINT NOT NULL
                ,CONSTRAINT PK_tblWaitStatsData PRIMARY KEY CLUSTERED (CollectionTime, WaitType)
            );
        END

        DECLARE @CurrentStartTime DATETIME = (SELECT sqlserver_start_time FROM sys.dm_os_sys_info);
        DECLARE @PrevStartTime DATETIME, @PrevCollTime DATETIME;

        SELECT TOP 1 @PrevStartTime = SqlServerStartTime, @PrevCollTime = CollectionTime
        FROM dbo.tblWaitStatsData ORDER BY CollectionTime DESC;

        -- Check for SQL Restart or first run
        IF @CurrentStartTime <> ISNULL(@PrevStartTime, 0)
        BEGIN
            INSERT INTO dbo.tblWaitStatsData (SqlServerStartTime, CollectionTime, WaitType, WaitTimeDiff_ms, WaitTasksDiff, SignalWaitDiff_ms, WaitTimeCumulative_ms, WaitingTasksCountCumulative, SignalWaitTimeCumulative_ms)
            SELECT @CurrentStartTime, GETDATE(), wait_type, 0, 0, 0, wait_time_ms, waiting_tasks_count, signal_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE [wait_type] NOT IN (N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE', N'CXCONSUMER', N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX', N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'MEMORY_ALLOCATION_EXT', N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_DRAIN_WORKER', N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST', N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK', N'PREEMPTIVE_XE_GETTARGETSTATE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_ASYNC_QUEUE', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE', N'REDO_THREAD_PENDING_WORK', N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SOS_WORK_DISPATCHER', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_RECOVERY', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT');
        END
        ELSE
        BEGIN
            INSERT INTO dbo.tblWaitStatsData (SqlServerStartTime, CollectionTime, WaitType, WaitTimeDiff_ms, WaitTasksDiff, SignalWaitDiff_ms, WaitTimeCumulative_ms, WaitingTasksCountCumulative, SignalWaitTimeCumulative_ms)
            SELECT @CurrentStartTime, GETDATE(), s.wait_type,
                   s.wait_time_ms - h.WaitTimeCumulative_ms,
                   s.waiting_tasks_count - h.WaitingTasksCountCumulative,
                   s.signal_wait_time_ms - h.SignalWaitTimeCumulative_ms,
                   s.wait_time_ms, s.waiting_tasks_count, s.signal_wait_time_ms
            FROM sys.dm_os_wait_stats s
            INNER JOIN dbo.tblWaitStatsData h ON s.wait_type = h.WaitType AND h.CollectionTime = @PrevCollTime
            WHERE (s.wait_time_ms - h.WaitTimeCumulative_ms) > 100 
              AND s.wait_type NOT IN (N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE', N'CXCONSUMER', N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX', N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'MEMORY_ALLOCATION_EXT', N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_DRAIN_WORKER', N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST', N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK', N'PREEMPTIVE_XE_GETTARGETSTATE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_ASYNC_QUEUE', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE', N'REDO_THREAD_PENDING_WORK', N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SOS_WORK_DISPATCHER', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_RECOVERY', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT');
        END

        DELETE FROM dbo.tblWaitStatsData WHERE CollectionTime < DATEADD(DAY, -@retaindays, GETDATE());
    END

    ---------------------------------------------------------------------------
    -- MODE 2: LIVE SNAPSHOT (Using Dynamic Delay)
    ---------------------------------------------------------------------------
    ELSE 
    BEGIN
        -- Convert integer seconds to HH:MM:SS format for WAITFOR
        DECLARE @DelayFormat CHAR(8);
        SET @DelayFormat = CONVERT(CHAR(8), DATEADD(second, @LiveDelaySeconds, '00:00:00'), 108);

        SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
        INTO #S1 FROM sys.dm_os_wait_stats;

        -- Dynamic Pause based on parameter
        WAITFOR DELAY @DelayFormat; 

        SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
        INTO #S2 FROM sys.dm_os_wait_stats;

        WITH RawDiff AS (
            SELECT s2.wait_type, 
                   s2.wait_time_ms - s1.wait_time_ms AS W_ms,
                   s2.signal_wait_time_ms - s1.signal_wait_time_ms AS S_ms,
                   s2.waiting_tasks_count - s1.waiting_tasks_count AS W_count
            FROM #S2 s2 JOIN #S1 s1 ON s2.wait_type = s1.wait_type
            WHERE s2.wait_time_ms - s1.wait_time_ms > 0
              AND s2.wait_type NOT IN (N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE', N'CXCONSUMER', N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX', N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'MEMORY_ALLOCATION_EXT', N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_DRAIN_WORKER', N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST', N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK', N'PREEMPTIVE_XE_GETTARGETSTATE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_ASYNC_QUEUE', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE', N'REDO_THREAD_PENDING_WORK', N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SOS_WORK_DISPATCHER', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_RECOVERY', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT')
        )
        SELECT wait_type,
               CAST(W_ms / 1000.0 AS DECIMAL(16,2)) AS Wait_S,
               CAST((W_ms - S_ms) / 1000.0 AS DECIMAL(16,2)) AS Resource_S,
               CAST(S_ms / 1000.0 AS DECIMAL(16,2)) AS Signal_S,
               W_count AS WaitCount,
               CAST(100.0 * W_ms / SUM(W_ms) OVER() AS DECIMAL(5,2)) AS [Percentage],
               CAST('https://www.sqlskills.com/help/waits/' + wait_type AS XML) AS [HelpURL]
        FROM RawDiff
        ORDER BY W_ms DESC;
    END
END
GO



CREATE OR ALTER VIEW dbo.vw_WaitStatsHistory
AS
WITH UniqueSnapshots AS (
    -- Step 1: Identify all unique collection times and find their previous sibling
    SELECT 
        CollectionTime AS Sample_End,
        LAG(CollectionTime) OVER (ORDER BY CollectionTime) AS Sample_Start
    FROM (SELECT DISTINCT CollectionTime FROM dbo.tblWaitStatsData) AS T
),
IntervalCalculation AS (
    -- Step 2: Calculate the duration in HH:MM:SS
    SELECT 
        Sample_Start,
        Sample_End,
        DATEDIFF(SECOND, Sample_Start, Sample_End) AS SecondsDiff,
        -- Format seconds to HH:MM:SS
        CONVERT(varchar, DATEADD(second, DATEDIFF(SECOND, Sample_Start, Sample_End), 0), 108) AS Interval_HHMMSS
    FROM UniqueSnapshots
),
AggregatedData AS (
    -- Step 3: Join the interval logic back to the actual wait data
    SELECT 
        i.Sample_Start,
        i.Sample_End,
        i.Interval_HHMMSS,
        w.WaitType,
        w.WaitTimeDiff_ms,
        w.SignalWaitDiff_ms,
        (w.WaitTimeDiff_ms - w.SignalWaitDiff_ms) AS ResourceWaitDiff_ms,
        w.WaitTasksDiff,
        -- Percentage of total waits during THIS specific snapshot
        100.0 * w.WaitTimeDiff_ms / NULLIF(SUM(w.WaitTimeDiff_ms) OVER(PARTITION BY w.CollectionTime), 0) AS Percentage
    FROM dbo.tblWaitStatsData w
    INNER JOIN IntervalCalculation i ON w.CollectionTime = i.Sample_End
    WHERE w.WaitTimeDiff_ms > 0
)
SELECT 
    Sample_Start,
    Sample_End,
    Interval_HHMMSS,
    WaitType,
    CAST(WaitTimeDiff_ms / 1000.0 AS DECIMAL(16,2)) AS Wait_S,
    CAST(ResourceWaitDiff_ms / 1000.0 AS DECIMAL(16,2)) AS Resource_S,
    CAST(SignalWaitDiff_ms / 1000.0 AS DECIMAL(16,2)) AS Signal_S,
    WaitTasksDiff AS WaitCount,
    CAST(Percentage AS DECIMAL(5,2)) AS [Percentage],
    'https://www.sqlskills.com/help/waits/' + WaitType AS [HelpURL]
FROM AggregatedData;
GO


SELECT 
   *
FROM vw_WaitStatsHistory
ORDER BY Sample_End DESC, Wait_S DESC;
