/*============================================================================
  usp_PerformanceTroubleshoot
  Purpose : One-shot, comprehensive "high CPU / slow server" diagnostic proc.
            Consolidates the DMV logic already used across this repo's
            individual scripts (CPU ring buffer, wait stats, blocking chain,
            WhoIsActive-style active requests, top costly queries, missing
            indexes, index fragmentation, tempdb contention, memory, IO
            stalls) into a single, parameterized, section-by-section report.
            Optionally pulls Query Store data when a @DatabaseName is passed
            and Query Store is enabled for that database.

  Notes   : - Created in tempdb to match the existing usp_CPUUsage /
              usp_SQLInformation / usp_IndexAnalysis convention in this repo.
              Objects in tempdb survive until the next SQL Server restart;
              move it to a persistent admin DB (e.g. DBATasks) if you need
              it to survive restarts.
            - Every section is wrapped in TRY/CATCH so one failing DMV
              (permissions, edition, version) never blocks the rest of the
              report.
            - Nothing here writes data or changes server state - it is
              strictly read-only / diagnostic.
            - Live query-plan retrieval (dm_exec_query_plan against
              currently running requests) is guarded behind
              @IncludeLiveQueryPlans because generating plan XML for
              in-flight requests adds extra CPU while the server is already
              under pressure. Cached-plan retrieval for the historical
              "top queries" section is left on by default since it reads
              the plan cache, not currently executing requests.

  Usage   :
      -- Quick pass, no query store, no plans, no fragmentation
      EXEC tempdb..usp_PerformanceTroubleshoot;

      -- Full pass scoped to a database, including Query Store + fragmentation
      EXEC tempdb..usp_PerformanceTroubleshoot
           @DatabaseName = 'MyAppDB',
           @IncludeIndexFragment = 1,
           @IncludeLiveQueryPlans = 1;
============================================================================*/
USE [tempdb];
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_PerformanceTroubleshoot]
(
    @DatabaseName           SYSNAME       = NULL,   -- Scope Query Store / missing-index / top-query / IO checks to this DB. NULL = all databases (where applicable)
    @TopN                   INT           = 20,      -- Row cap for ranked result sets
    @MinutesBack            INT           = 15,      -- Reserved lookback window for time-bounded checks
    @QueryStoreTimeRange    NVARCHAR(10)  = '1H',     -- Query Store lookback: suffix Mi=minutes, H=hours, D=days, M=months (e.g. '30Mi','6H','1D','1M')
    @IncludeServerInfo      BIT           = 1,
    @IncludeCPUHistory      BIT           = 1,
    @IncludeSchedulerHealth BIT           = 1,
    @IncludeWaitStats       BIT           = 1,
    @IncludeActiveRequests  BIT           = 1,
    @IncludeLiveQueryPlans  BIT           = 0,       -- Fetch live plan XML for currently running requests (adds overhead)
    @IncludeBlocking        BIT           = 1,
    @IncludeTopQueries      BIT           = 1,
    @IncludeMissingIndexes  BIT           = 1,
    @IncludeIndexFragment   BIT           = 0,       -- Expensive; opt-in, requires @DatabaseName
    @IncludeTempdbHealth    BIT           = 1,
    @IncludeMemoryHealth    BIT           = 1,
    @IncludeIOStats         BIT           = 1,
    @IncludeQueryStore      BIT           = 1        -- Only runs if @DatabaseName is passed AND Query Store is ON for that DB
)
AS
BEGIN
    SET NOCOUNT ON;
    SET QUOTED_IDENTIFIER ON;
    SET DEADLOCK_PRIORITY LOW;  -- this proc should never be the reason something else deadlocks

    DECLARE @sql          NVARCHAR(MAX);
    DECLARE @ts_now       BIGINT;
    DECLARE @QSState      NVARCHAR(60);
    DECLARE @QSStartTime  DATETIME;
    DECLARE @QSStartTimeStr NVARCHAR(30);

    ----------------------------------------------------------------------
    -- 0. Run header
    ----------------------------------------------------------------------
    SELECT '=== usp_PerformanceTroubleshoot @ ' + CONVERT(VARCHAR(23), GETDATE(), 121) + ' ===' AS RunInfo,
           @@SERVERNAME       AS ServerName,
           ISNULL(@DatabaseName, '<all databases>') AS DatabaseScope,
           @TopN              AS TopN;

    ----------------------------------------------------------------------
    -- 1. Server / hardware / configuration snapshot
    ----------------------------------------------------------------------
    IF @IncludeServerInfo = 1
    BEGIN
        BEGIN TRY
            SELECT '1. SERVER INFO' AS Section;
            SELECT
                @@VERSION                                                       AS SQLVersion,
                CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128))                AS Edition,
                CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(128))           AS ProductLevel,
                CAST(SERVERPROPERTY('EngineEdition') AS INT)                    AS EngineEdition,
                si.cpu_count                                                    AS LogicalCPUs,
                si.hyperthread_ratio                                            AS HyperthreadRatio,
                si.cpu_count / NULLIF(si.hyperthread_ratio, 0)                  AS PhysicalCPUsApprox,
                si.physical_memory_kb / 1024                                    AS PhysicalMemoryMB,
                si.committed_kb / 1024                                          AS SQLCommittedMemoryMB,
                si.committed_target_kb / 1024                                   AS SQLCommittedTargetMB,
                si.sqlserver_start_time                                        AS SQLStartTime,
                DATEDIFF(HOUR, si.sqlserver_start_time, GETDATE())              AS HoursUp,
                si.scheduler_count                                              AS SchedulerCount,
                si.max_workers_count                                            AS MaxWorkers,
                (SELECT COUNT(*) FROM sys.dm_exec_sessions)                     AS CurrentSessions,
                (SELECT COUNT(*) FROM sys.dm_exec_requests)                     AS CurrentRequests,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'max degree of parallelism')        AS MAXDOP,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'cost threshold for parallelism')   AS CostThresholdParallelism,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'max server memory (MB)')           AS MaxServerMemoryMB,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'min server memory (MB)')           AS MinServerMemoryMB,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'optimize for ad hoc workloads')    AS OptimizeForAdhoc
            FROM sys.dm_os_sys_info si;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS ServerInfoError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 2. CPU utilization history (SystemHealth ring buffer, last ~256 samples)
    ----------------------------------------------------------------------
    IF @IncludeCPUHistory = 1
    BEGIN
        BEGIN TRY
            SELECT '2. CPU UTILIZATION HISTORY (ring buffer)' AS Section;

            SET @ts_now = (SELECT cpu_ticks / (cpu_ticks / ms_ticks) FROM sys.dm_os_sys_info WITH (NOLOCK));

            SELECT TOP (256)
                   SQLProcessUtilization                                          AS SQLServerCPUUtilPct,
                   SystemIdle                                                     AS SystemIdlePct,
                   100 - SystemIdle - SQLProcessUtilization                       AS OtherProcessCPUUtilPct,
                   DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE())          AS EventTime
            FROM
            (
                SELECT record.value('(./Record/@id)[1]', 'int')                                                       AS record_id,
                       record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')             AS SystemIdle,
                       record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')     AS SQLProcessUtilization,
                       [timestamp]
                FROM
                (
                    SELECT [timestamp], CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                          AND record LIKE N'%<SystemHealth>%'
                ) AS x
            ) AS y
            ORDER BY record_id DESC
            OPTION (RECOMPILE);
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS CPUHistoryError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 3. Scheduler / signal-wait health (CPU pressure indicators)
    ----------------------------------------------------------------------
    IF @IncludeSchedulerHealth = 1
    BEGIN
        BEGIN TRY
            SELECT '3a. SIGNAL WAIT % (>25% = CPU pressure, threads waiting for a CPU quantum)' AS Section;
            SELECT
                CAST(100.0 * SUM(signal_wait_time_ms) / NULLIF(SUM(wait_time_ms), 0) AS DECIMAL(5, 2))                        AS SignalWaitPct,
                CAST(100.0 * (SUM(wait_time_ms) - SUM(signal_wait_time_ms)) / NULLIF(SUM(wait_time_ms), 0) AS DECIMAL(5, 2))  AS ResourceWaitPct
            FROM sys.dm_os_wait_stats;

            SELECT '3b. SCHEDULERS (runnable_tasks_count > 0 sustained = CPU pressure / not enough cores)' AS Section;
            SELECT
                scheduler_id,
                cpu_id,
                status,
                is_online,
                current_tasks_count,
                runnable_tasks_count,
                current_workers_count,
                active_workers_count,
                work_queue_count,
                pending_disk_io_count,
                context_switches_count,
                yield_count
            FROM sys.dm_os_schedulers
            WHERE scheduler_id < 1048576  -- exclude hidden/DAC/internal schedulers
            ORDER BY runnable_tasks_count DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS SchedulerHealthError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 4. Wait statistics (top N, benign/idle waits filtered out)
    ----------------------------------------------------------------------
    IF @IncludeWaitStats = 1
    BEGIN
        BEGIN TRY
            SELECT '4. TOP WAIT STATS (since last restart / stats clear)' AS Section;
            SELECT TOP (@TopN)
                wait_type,
                waiting_tasks_count,
                wait_time_ms,
                max_wait_time_ms,
                signal_wait_time_ms,
                wait_time_ms - signal_wait_time_ms                              AS resource_wait_time_ms,
                CAST(100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER (), 0) AS DECIMAL(5, 2)) AS pct_of_total_wait_time
            FROM sys.dm_os_wait_stats
            WHERE waiting_tasks_count > 0
                  AND wait_type NOT IN(
                      N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER',
                      N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
                      N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD',
                      N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC', N'FSAGENT',
                      N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
                      N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE',
                      N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE',
                      N'ONDEMAND_TASK_QUEUE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PREEMPTIVE_OS_AUTHENTICATIONOPS',
                      N'PREEMPTIVE_OS_CREATEFILE', N'PREEMPTIVE_OS_GENERICOPS', N'PREEMPTIVE_OS_LIBRARYOPS', N'PREEMPTIVE_OS_QUERYREGISTRY',
                      N'PREEMPTIVE_HADR_LEASE_MECHANISM', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS',
                      N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE',
                      N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH',
                      N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED',
                      N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT',
                      N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES',
                      N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
                      N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT')
            ORDER BY wait_time_ms DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS WaitStatsError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 5. Currently active requests (WhoIsActive-lite)
    ----------------------------------------------------------------------
    IF @IncludeActiveRequests = 1
    BEGIN
        BEGIN TRY
            SELECT '5. ACTIVE REQUESTS' AS Section;

            IF @IncludeLiveQueryPlans = 1
            BEGIN
                SELECT
                    r.session_id,
                    r.status,
                    r.command,
                    r.blocking_session_id,
                    r.wait_type,
                    r.wait_time,
                    r.wait_resource,
                    r.cpu_time,
                    r.total_elapsed_time,
                    r.reads,
                    r.writes,
                    r.logical_reads,
                    r.granted_query_memory * 8                    AS granted_memory_kb,
                    r.open_transaction_count,
                    r.percent_complete,
                    DB_NAME(r.database_id)                        AS database_name,
                    s.login_name,
                    s.host_name,
                    s.program_name,
                    r.start_time,
                    est.text                                      AS sql_text,
                    qp.query_plan
                FROM sys.dm_exec_requests r
                    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) est
                    OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) qp
                WHERE r.session_id <> @@SPID
                      AND s.is_user_process = 1
                ORDER BY r.cpu_time DESC;
            END
            ELSE
            BEGIN
                SELECT
                    r.session_id,
                    r.status,
                    r.command,
                    r.blocking_session_id,
                    r.wait_type,
                    r.wait_time,
                    r.wait_resource,
                    r.cpu_time,
                    r.total_elapsed_time,
                    r.reads,
                    r.writes,
                    r.logical_reads,
                    r.granted_query_memory * 8                    AS granted_memory_kb,
                    r.open_transaction_count,
                    r.percent_complete,
                    DB_NAME(r.database_id)                        AS database_name,
                    s.login_name,
                    s.host_name,
                    s.program_name,
                    r.start_time,
                    est.text                                      AS sql_text
                FROM sys.dm_exec_requests r
                    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) est
                WHERE r.session_id <> @@SPID
                      AND s.is_user_process = 1
                ORDER BY r.cpu_time DESC;
            END;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS ActiveRequestsError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 6. Blocking chain
    ----------------------------------------------------------------------
    IF @IncludeBlocking = 1
    BEGIN
        BEGIN TRY
            SELECT '6. BLOCKING CHAIN' AS Section;

            IF OBJECT_ID('tempdb..#BlockingSessions') IS NOT NULL
                DROP TABLE #BlockingSessions;

            SELECT
                r.session_id,
                r.blocking_session_id,
                r.wait_type,
                r.wait_time,
                r.wait_resource,
                r.status,
                r.command,
                DB_NAME(r.database_id) AS database_name,
                est.text               AS sql_text
            INTO #BlockingSessions
            FROM sys.dm_exec_requests r
                CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) est
            WHERE r.blocking_session_id <> 0;

            IF EXISTS (SELECT 1 FROM #BlockingSessions)
            BEGIN
                SELECT '6a. HEAD BLOCKERS (blocking others, may be idle/not currently executing)' AS Section;
                SELECT DISTINCT
                    bs.blocking_session_id                                       AS head_blocker_session_id,
                    s.login_name,
                    s.host_name,
                    s.program_name,
                    s.status,
                    COALESCE(est.text, '<no active request - likely idle in an open transaction>') AS head_blocker_last_sql,
                    (SELECT COUNT(*) FROM #BlockingSessions b2 WHERE b2.blocking_session_id = bs.blocking_session_id) AS sessions_blocked
                FROM #BlockingSessions bs
                    LEFT JOIN sys.dm_exec_sessions s ON s.session_id = bs.blocking_session_id
                    LEFT JOIN sys.dm_exec_connections c ON c.session_id = bs.blocking_session_id
                    OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) est
                ORDER BY sessions_blocked DESC;

                SELECT '6b. FULL BLOCKING DETAIL' AS Section;
                SELECT * FROM #BlockingSessions ORDER BY blocking_session_id;
            END
            ELSE
            BEGIN
                SELECT 'No blocking detected at time of run.' AS BlockingStatus;
            END;

            DROP TABLE #BlockingSessions;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS BlockingError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 7. Top resource-consuming queries (plan cache, cumulative since cache/plan creation)
    ----------------------------------------------------------------------
    IF @IncludeTopQueries = 1
    BEGIN
        BEGIN TRY
            SELECT '7a. TOP QUERIES BY TOTAL CPU TIME' AS Section;
            ;WITH TopByCPU AS
            (
                SELECT TOP (@TopN)
                       qs.sql_handle, qs.plan_handle, qs.execution_count, qs.total_worker_time, qs.total_elapsed_time,
                       qs.total_logical_reads, qs.total_physical_reads, qs.total_logical_writes,
                       qs.creation_time, qs.last_execution_time, qs.statement_start_offset, qs.statement_end_offset
                FROM sys.dm_exec_query_stats qs
                WHERE (@DatabaseName IS NULL OR EXISTS (SELECT 1 FROM sys.dm_exec_sql_text(qs.sql_handle) t WHERE t.dbid = DB_ID(@DatabaseName)))
                ORDER BY qs.total_worker_time DESC
            )
            SELECT
                DB_NAME(qt.dbid)                                                     AS database_name,
                t.execution_count,
                t.total_worker_time / 1000                                           AS total_cpu_ms,
                (t.total_worker_time / NULLIF(t.execution_count, 0)) / 1000           AS avg_cpu_ms,
                t.total_elapsed_time / 1000                                          AS total_duration_ms,
                (t.total_elapsed_time / NULLIF(t.execution_count, 0)) / 1000          AS avg_duration_ms,
                t.total_logical_reads,
                t.total_logical_reads / NULLIF(t.execution_count, 0)                  AS avg_logical_reads,
                t.total_physical_reads,
                t.total_logical_writes,
                t.creation_time,
                t.last_execution_time,
                SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1,
                    ((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END
                      - t.statement_start_offset) / 2) + 1)                          AS statement_text,
                qp.query_plan
            FROM TopByCPU t
                CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) qt
                OUTER APPLY sys.dm_exec_query_plan(t.plan_handle) qp
            ORDER BY t.total_worker_time DESC;

            SELECT '7b. TOP QUERIES BY TOTAL LOGICAL READS (I/O)' AS Section;
            ;WITH TopByReads AS
            (
                SELECT TOP (@TopN)
                       qs.sql_handle, qs.plan_handle, qs.execution_count, qs.total_worker_time, qs.total_elapsed_time,
                       qs.total_logical_reads, qs.total_physical_reads, qs.total_logical_writes,
                       qs.creation_time, qs.last_execution_time, qs.statement_start_offset, qs.statement_end_offset
                FROM sys.dm_exec_query_stats qs
                WHERE (@DatabaseName IS NULL OR EXISTS (SELECT 1 FROM sys.dm_exec_sql_text(qs.sql_handle) t WHERE t.dbid = DB_ID(@DatabaseName)))
                ORDER BY qs.total_logical_reads DESC
            )
            SELECT
                DB_NAME(qt.dbid)                                                     AS database_name,
                t.execution_count,
                t.total_logical_reads,
                t.total_logical_reads / NULLIF(t.execution_count, 0)                  AS avg_logical_reads,
                t.total_worker_time / 1000                                           AS total_cpu_ms,
                t.total_elapsed_time / 1000                                          AS total_duration_ms,
                t.total_physical_reads,
                t.last_execution_time,
                SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1,
                    ((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END
                      - t.statement_start_offset) / 2) + 1)                          AS statement_text
            FROM TopByReads t
                CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) qt
            ORDER BY t.total_logical_reads DESC;

            SELECT '7c. TOP QUERIES BY EXECUTION COUNT (chatty / parameter-sniffing candidates)' AS Section;
            ;WITH TopByExec AS
            (
                SELECT TOP (@TopN)
                       qs.sql_handle, qs.execution_count, qs.total_worker_time, qs.total_elapsed_time,
                       qs.last_execution_time, qs.statement_start_offset, qs.statement_end_offset
                FROM sys.dm_exec_query_stats qs
                WHERE (@DatabaseName IS NULL OR EXISTS (SELECT 1 FROM sys.dm_exec_sql_text(qs.sql_handle) t WHERE t.dbid = DB_ID(@DatabaseName)))
                ORDER BY qs.execution_count DESC
            )
            SELECT
                DB_NAME(qt.dbid)                                                     AS database_name,
                t.execution_count,
                t.total_worker_time / 1000                                           AS total_cpu_ms,
                (t.total_worker_time / NULLIF(t.execution_count, 0)) / 1000           AS avg_cpu_ms,
                t.last_execution_time,
                SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1,
                    ((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END
                      - t.statement_start_offset) / 2) + 1)                          AS statement_text
            FROM TopByExec t
                CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) qt
            ORDER BY t.execution_count DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS TopQueriesError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 8. Missing indexes (server-wide DMVs, filtered by @DatabaseName if provided)
    ----------------------------------------------------------------------
    IF @IncludeMissingIndexes = 1
    BEGIN
        BEGIN TRY
            SELECT '8. MISSING INDEXES' AS Section;
            SELECT TOP (@TopN)
                DB_NAME(mid.database_id)                                                                          AS database_name,
                migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans)   AS improvement_measure,
                OBJECT_NAME(mid.object_id, mid.database_id)                                                       AS table_name,
                mid.equality_columns,
                mid.inequality_columns,
                mid.included_columns,
                migs.user_seeks,
                migs.user_scans,
                migs.avg_total_user_cost,
                migs.avg_user_impact,
                migs.last_user_seek
            FROM sys.dm_db_missing_index_group_stats migs
                INNER JOIN sys.dm_db_missing_index_groups mig ON migs.group_handle = mig.index_group_handle
                INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
            WHERE (@DatabaseName IS NULL OR mid.database_id = DB_ID(@DatabaseName))
            ORDER BY improvement_measure DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS MissingIndexesError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 9. Index fragmentation (requires @DatabaseName - needs sys.indexes in that DB's context)
    ----------------------------------------------------------------------
    IF @IncludeIndexFragment = 1 AND @DatabaseName IS NOT NULL
    BEGIN
        BEGIN TRY
            SELECT '9. INDEX FRAGMENTATION (' + @DatabaseName + ')' AS Section;

            SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
            SELECT TOP (' + CAST(@TopN AS NVARCHAR(10)) + N')
                DB_NAME()                                                        AS database_name,
                OBJECT_SCHEMA_NAME(ps.object_id) + N''.'' + OBJECT_NAME(ps.object_id) AS table_name,
                i.name                                                           AS index_name,
                ps.index_type_desc,
                CAST(ps.avg_fragmentation_in_percent AS DECIMAL(5,2))            AS avg_fragmentation_pct,
                ps.fragment_count,
                ps.page_count
            FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, N''LIMITED'') ps
                INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
            WHERE ps.page_count > 500
                  AND ps.avg_fragmentation_in_percent > 10
            ORDER BY ps.avg_fragmentation_in_percent DESC;';

            EXEC (@sql);
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS IndexFragmentationError;
        END CATCH
    END
    ELSE IF @IncludeIndexFragment = 1 AND @DatabaseName IS NULL
    BEGIN
        SELECT '9. INDEX FRAGMENTATION skipped - pass @DatabaseName to run this section.' AS IndexFragmentationStatus;
    END;

    ----------------------------------------------------------------------
    -- 10. Tempdb health / contention
    ----------------------------------------------------------------------
    IF @IncludeTempdbHealth = 1
    BEGIN
        BEGIN TRY
            SELECT '10a. TEMPDB SPACE USAGE' AS Section;
            SELECT
                SUM(user_object_reserved_page_count) * 8 / 1024      AS UserObjectsMB,
                SUM(internal_object_reserved_page_count) * 8 / 1024  AS InternalObjectsMB,
                SUM(version_store_reserved_page_count) * 8 / 1024    AS VersionStoreMB,
                SUM(unallocated_extent_page_count) * 8 / 1024        AS FreeSpaceMB,
                SUM(mixed_extent_page_count) * 8 / 1024              AS MixedExtentMB
            FROM sys.dm_db_file_space_usage;

            SELECT '10b. TOP TEMPDB CONSUMERS BY SESSION' AS Section;
            SELECT TOP (@TopN)
                session_id,
                request_id,
                SUM(internal_objects_alloc_page_count) * 8 / 1024   AS InternalObjMB,
                SUM(user_objects_alloc_page_count) * 8 / 1024       AS UserObjMB
            FROM sys.dm_db_task_space_usage
            GROUP BY session_id, request_id
            HAVING SUM(internal_objects_alloc_page_count) + SUM(user_objects_alloc_page_count) > 0
            ORDER BY SUM(internal_objects_alloc_page_count) + SUM(user_objects_alloc_page_count) DESC;

            SELECT '10c. TEMPDB ALLOCATION-PAGE CONTENTION (PAGELATCH waits on tempdb pages)' AS Section;
            SELECT
                wt.session_id,
                wt.wait_duration_ms,
                wt.wait_type,
                wt.resource_description,
                er.status,
                er.command,
                est.text AS sql_text
            FROM sys.dm_os_waiting_tasks wt
                LEFT JOIN sys.dm_exec_requests er ON wt.session_id = er.session_id
                OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) est
            WHERE wt.wait_type LIKE 'PAGELATCH%'
                  AND wt.resource_description LIKE '2:%'
            ORDER BY wt.wait_duration_ms DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS TempdbHealthError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 11. Memory health
    ----------------------------------------------------------------------
    IF @IncludeMemoryHealth = 1
    BEGIN
        BEGIN TRY
            SELECT '11a. SQL SERVER PROCESS MEMORY' AS Section;
            SELECT
                physical_memory_in_use_kb / 1024   AS SQLMemUsedMB,
                large_page_allocations_kb / 1024    AS LargePageMB,
                locked_page_allocations_kb / 1024   AS LockedPageMB,
                total_virtual_address_space_kb / 1024 AS TotalVASMB,
                process_physical_memory_low,
                process_virtual_memory_low
            FROM sys.dm_os_process_memory;

            SELECT '11b. OS MEMORY STATE' AS Section;
            SELECT
                total_physical_memory_kb / 1024      AS TotalPhysicalMemoryMB,
                available_physical_memory_kb / 1024  AS AvailablePhysicalMemoryMB,
                system_memory_state_desc
            FROM sys.dm_os_sys_memory;

            SELECT '11c. BUFFER POOL USAGE BY DATABASE' AS Section;
            SELECT TOP (@TopN)
                DB_NAME(database_id)      AS database_name,
                COUNT(*) * 8 / 1024        AS BufferedMB
            FROM sys.dm_os_buffer_descriptors
            GROUP BY database_id
            ORDER BY BufferedMB DESC;

            SELECT '11d. ACTIVE / PENDING MEMORY GRANTS' AS Section;
            SELECT TOP (@TopN)
                session_id,
                request_time,
                grant_time,
                requested_memory_kb,
                granted_memory_kb,
                ideal_memory_kb,
                used_memory_kb,
                max_used_memory_kb,
                query_cost,
                timeout_sec,
                wait_order,
                is_next_candidate
            FROM sys.dm_exec_query_memory_grants
            ORDER BY requested_memory_kb DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS MemoryHealthError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 12. IO stalls by database file
    ----------------------------------------------------------------------
    IF @IncludeIOStats = 1
    BEGIN
        BEGIN TRY
            SELECT '12. IO STALLS BY DATABASE FILE' AS Section;
            SELECT TOP (@TopN)
                DB_NAME(vfs.database_id)                                          AS database_name,
                mf.type_desc,
                mf.physical_name,
                vfs.num_of_reads,
                vfs.num_of_writes,
                vfs.io_stall_read_ms,
                vfs.io_stall_write_ms,
                (vfs.io_stall_read_ms + vfs.io_stall_write_ms)                    AS io_stall_total_ms,
                CAST(vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) AS DECIMAL(10, 2))   AS avg_read_stall_ms,
                CAST(vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) AS DECIMAL(10, 2)) AS avg_write_stall_ms,
                vfs.size_on_disk_bytes / 1024 / 1024                             AS size_on_disk_mb
            FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
                INNER JOIN sys.master_files mf ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
            WHERE (@DatabaseName IS NULL OR vfs.database_id = DB_ID(@DatabaseName))
            ORDER BY io_stall_total_ms DESC;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS IOStatsError;
        END CATCH
    END;

    ----------------------------------------------------------------------
    -- 13. Query Store (only when @DatabaseName is passed and QS is ON there)
    ----------------------------------------------------------------------
    IF @IncludeQueryStore = 1 AND @DatabaseName IS NOT NULL
    BEGIN
        BEGIN TRY
            SET @sql = N'SELECT @state = actual_state_desc FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_query_store_options;';
            EXEC sp_executesql @sql, N'@state NVARCHAR(60) OUTPUT', @state = @QSState OUTPUT;

            IF @QSState IS NULL OR @QSState = 'OFF'
            BEGIN
                SELECT 'Query Store is OFF (or database not found / accessible) for ' + @DatabaseName AS QueryStoreStatus;
            END
            ELSE
            BEGIN
                SET @QSStartTime =
                    CASE
                        WHEN @QueryStoreTimeRange LIKE '%Mi' THEN DATEADD(MINUTE, -CAST(LEFT(@QueryStoreTimeRange, LEN(@QueryStoreTimeRange) - 2) AS INT), GETUTCDATE())
                        WHEN @QueryStoreTimeRange LIKE '%H'  THEN DATEADD(HOUR,   -CAST(LEFT(@QueryStoreTimeRange, LEN(@QueryStoreTimeRange) - 1) AS INT), GETUTCDATE())
                        WHEN @QueryStoreTimeRange LIKE '%D'  THEN DATEADD(DAY,    -CAST(LEFT(@QueryStoreTimeRange, LEN(@QueryStoreTimeRange) - 1) AS INT), GETUTCDATE())
                        WHEN @QueryStoreTimeRange LIKE '%M'  THEN DATEADD(MONTH,  -CAST(LEFT(@QueryStoreTimeRange, LEN(@QueryStoreTimeRange) - 1) AS INT), GETUTCDATE())
                        ELSE DATEADD(HOUR, -1, GETUTCDATE())
                    END;
                SET @QSStartTimeStr = CONVERT(NVARCHAR(30), @QSStartTime, 121);

                SELECT '13. QUERY STORE - TOP RESOURCE CONSUMING QUERIES (' + @DatabaseName + ', state=' + @QSState
                       + ', since ' + @QSStartTimeStr + ' UTC)' AS Section;

                SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
                SELECT TOP (' + CAST(@TopN AS NVARCHAR(10)) + N')
                    q.query_id,
                    OBJECT_SCHEMA_NAME(q.object_id) + N''.'' + OBJECT_NAME(q.object_id)  AS object_name,
                    qt.query_sql_text,
                    SUM(rs.count_executions)                                             AS total_executions,
                    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS DECIMAL(18,2))  AS total_cpu_ms,
                    CAST(AVG(rs.avg_cpu_time) / 1000.0 AS DECIMAL(18,2))                  AS avg_cpu_ms,
                    CAST(SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS DECIMAL(18,2))  AS total_duration_ms,
                    CAST(AVG(rs.avg_duration) / 1000.0 AS DECIMAL(18,2))                  AS avg_duration_ms,
                    CAST(AVG(rs.avg_logical_io_reads) AS DECIMAL(18,2))                   AS avg_logical_reads,
                    CAST(AVG(rs.avg_rowcount) AS DECIMAL(18,2))                           AS avg_row_count,
                    MAX(rs.last_execution_time)                                           AS last_execution_time
                FROM sys.query_store_query q
                    INNER JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
                    INNER JOIN sys.query_store_plan p ON q.query_id = p.query_id
                    INNER JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
                    INNER JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
                WHERE rsi.start_time >= ''' + @QSStartTimeStr + N'''
                GROUP BY q.query_id, q.object_id, qt.query_sql_text
                ORDER BY total_cpu_ms DESC;';

                EXEC (@sql);
            END;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS QueryStoreError;
        END CATCH
    END
    ELSE IF @IncludeQueryStore = 1 AND @DatabaseName IS NULL
    BEGIN
        SELECT '13. QUERY STORE skipped - pass @DatabaseName to run this section.' AS QueryStoreStatus;
    END;

    SELECT '=== usp_PerformanceTroubleshoot complete ===' AS RunInfo;
END;
GO
