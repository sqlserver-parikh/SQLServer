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
            Also supports a "procedure deep-dive" mode: pass @ProcedureName
            (+ @DatabaseName) to get a focused report on ONE stored procedure -
            plan cache stats, statement-level CPU/IO breakdown, Query Store
            history for that object, missing-index hints mined directly out
            of its cached plan XML (with ready-to-run CREATE INDEX DDL),
            every table/index it touches, index inventory + fragmentation +
            usage stats + row counts for those tables, FKs, triggers, and
            statistics freshness - the same checklist a DBA would work
            through by hand when asked "why is this proc slow?".

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

      -- Deep-dive a single procedure: plan cache, Query Store, missing
      -- index hints, impacted tables/indexes, FKs, triggers, stats freshness
      EXEC tempdb..usp_PerformanceTroubleshoot
           @DatabaseName = 'MyAppDB',
           @ProcedureName = 'dbo.usp_GetOrders',
           @IncludeServerInfo = 0, @IncludeCPUHistory = 0, @IncludeSchedulerHealth = 0,
           @IncludeWaitStats = 0, @IncludeActiveRequests = 0, @IncludeBlocking = 0,
           @IncludeTopQueries = 0, @IncludeMissingIndexes = 0, @IncludeTempdbHealth = 0,
           @IncludeMemoryHealth = 0, @IncludeIOStats = 0;   -- isolate section 14 only
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
    @IncludeQueryStore      BIT           = 1,       -- Only runs if @DatabaseName is passed AND Query Store is ON for that DB
    @ProcedureName          SYSNAME       = NULL,    -- Deep-dive a single procedure (requires @DatabaseName); schema-qualify if not dbo, e.g. 'sales.usp_GetOrders'
    @IncludeProcedureAnalysis BIT         = 1        -- Master switch for the section 14 procedure deep-dive; only runs when @ProcedureName is supplied
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

    -- Section 14 (procedure deep-dive) working variables
    DECLARE @ProcDbId        INT;
    DECLARE @ProcObjectId    INT;
    DECLARE @ProcFullName    NVARCHAR(400);
    DECLARE @ProcQSState     NVARCHAR(60);
    DECLARE @TableFilter     NVARCHAR(MAX);
    DECLARE @DbCursorName    SYSNAME;
    DECLARE @ProcPlans       TABLE (plan_handle VARBINARY(64), query_plan XML);
    DECLARE @ImpactedTables  TABLE (database_name SYSNAME, schema_name SYSNAME, table_name SYSNAME, index_name SYSNAME NULL, physical_op NVARCHAR(60) NULL);

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

    ----------------------------------------------------------------------
    -- 14. PROCEDURE DEEP-DIVE ANALYSIS (only when @ProcedureName is passed)
    --     Everything a DBA would manually check for "why is this proc slow":
    --     plan cache stats, statement-level hotspots, Query Store history,
    --     missing-index hints mined from the plan XML, impacted tables,
    --     index inventory/fragmentation/usage, row counts, FKs, triggers,
    --     and statistics freshness.
    ----------------------------------------------------------------------
    IF @ProcedureName IS NOT NULL AND @IncludeProcedureAnalysis = 1
    BEGIN
        IF @DatabaseName IS NULL
        BEGIN
            SELECT '14. PROCEDURE DEEP-DIVE ANALYSIS' AS Section,
                   'Pass @DatabaseName along with @ProcedureName so the procedure can be resolved.' AS Message;
        END
        ELSE
        BEGIN
            SET @ProcDbId = DB_ID(@DatabaseName);
            IF @ProcDbId IS NULL
            BEGIN
                SELECT '14. PROCEDURE DEEP-DIVE ANALYSIS' AS Section,
                       'Database ''' + @DatabaseName + ''' not found.' AS Message;
            END
            ELSE
            BEGIN
                SET @ProcFullName = QUOTENAME(@DatabaseName) + N'.' +
                    CASE WHEN CHARINDEX('.', @ProcedureName) > 0 THEN @ProcedureName ELSE N'dbo.' + @ProcedureName END;
                SET @ProcObjectId = OBJECT_ID(@ProcFullName);

                IF @ProcObjectId IS NULL
                BEGIN
                    SELECT '14. PROCEDURE DEEP-DIVE ANALYSIS' AS Section,
                           'Could not resolve procedure ''' + @ProcedureName + ''' in database ''' + @DatabaseName + '''. Schema-qualify it if not dbo (e.g. ''sales.usp_GetOrders'').' AS Message;
                END
                ELSE
                BEGIN
                    SELECT '14. PROCEDURE DEEP-DIVE ANALYSIS: ' + @ProcFullName AS Section;

                    ------------------------------------------------------------------
                    -- 14a. Plan-cache overview: executions, CPU/IO totals, plan stability
                    ------------------------------------------------------------------
                    BEGIN TRY
                        SELECT '14a. PROCEDURE OVERVIEW & PLAN CACHE STATS' AS Section;
                        SELECT
                            @ProcFullName                                                      AS procedure_name,
                            COUNT(DISTINCT ps.plan_handle)                                      AS cached_plan_count,
                            COUNT(DISTINCT ps.query_plan_hash)                                   AS distinct_plan_shapes,
                            SUM(ps.execution_count)                                              AS total_executions,
                            MIN(ps.cached_time)                                                  AS oldest_plan_cached_time,
                            MAX(ps.last_execution_time)                                          AS last_execution_time,
                            CAST(SUM(ps.total_worker_time) / 1000.0 AS DECIMAL(18, 2))           AS total_cpu_ms,
                            CAST(SUM(ps.total_worker_time) / 1000.0 / NULLIF(SUM(ps.execution_count), 0) AS DECIMAL(18, 2)) AS avg_cpu_ms_per_exec,
                            CAST(MAX(ps.max_worker_time) / 1000.0 AS DECIMAL(18, 2))             AS worst_single_exec_cpu_ms,
                            CAST(SUM(ps.total_elapsed_time) / 1000.0 AS DECIMAL(18, 2))          AS total_duration_ms,
                            CAST(SUM(ps.total_elapsed_time) / 1000.0 / NULLIF(SUM(ps.execution_count), 0) AS DECIMAL(18, 2)) AS avg_duration_ms_per_exec,
                            SUM(ps.total_logical_reads)                                          AS total_logical_reads,
                            SUM(ps.total_logical_writes)                                         AS total_logical_writes,
                            SUM(ps.total_physical_reads)                                          AS total_physical_reads,
                            MAX(ps.plan_generation_num)                                          AS max_recompile_count,
                            CASE WHEN COUNT(DISTINCT ps.query_plan_hash) > 1
                                 THEN 'MULTIPLE PLAN SHAPES CACHED - possible parameter sniffing / plan instability'
                                 ELSE 'Single plan shape' END                                     AS plan_stability_flag
                        FROM sys.dm_exec_procedure_stats ps
                        WHERE ps.object_id = @ProcObjectId AND ps.database_id = @ProcDbId;

                        IF NOT EXISTS (SELECT 1 FROM sys.dm_exec_procedure_stats WHERE object_id = @ProcObjectId AND database_id = @ProcDbId)
                            SELECT 'No cached plan found for this procedure right now (plan may have been evicted, or it has never run since last recompile/restart).' AS Note;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS ProcOverviewError;
                    END CATCH;

                    ------------------------------------------------------------------
                    -- 14b. Statement-level breakdown - which statement inside the proc is hot
                    ------------------------------------------------------------------
                    BEGIN TRY
                        SELECT '14b. STATEMENT-LEVEL BREAKDOWN (which statement inside the proc is expensive)' AS Section;
                        SELECT TOP (@TopN)
                            qs.plan_handle,
                            SUBSTRING(st.text,
                                (qs.statement_start_offset / 2) + 1,
                                ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END - qs.statement_start_offset) / 2) + 1
                            )                                                                    AS statement_text,
                            qs.execution_count,
                            CAST(qs.total_worker_time / 1000.0 AS DECIMAL(18, 2))                AS total_cpu_ms,
                            CAST(qs.total_worker_time / 1000.0 / NULLIF(qs.execution_count, 0) AS DECIMAL(18, 2)) AS avg_cpu_ms,
                            CAST(qs.total_elapsed_time / 1000.0 AS DECIMAL(18, 2))                AS total_duration_ms,
                            qs.total_logical_reads,
                            qs.total_logical_reads / NULLIF(qs.execution_count, 0)                 AS avg_logical_reads,
                            qs.last_execution_time
                        FROM sys.dm_exec_procedure_stats ps
                            INNER JOIN sys.dm_exec_query_stats qs ON qs.plan_handle = ps.plan_handle
                            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                        WHERE ps.object_id = @ProcObjectId AND ps.database_id = @ProcDbId
                        ORDER BY qs.total_worker_time DESC;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS StatementBreakdownError;
                    END CATCH;

                    ------------------------------------------------------------------
                    -- 14c. Query Store history for this procedure (if QS is enabled)
                    ------------------------------------------------------------------
                    IF @IncludeQueryStore = 1
                    BEGIN
                        BEGIN TRY
                            SET @sql = N'SELECT @state = actual_state_desc FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_query_store_options;';
                            EXEC sp_executesql @sql, N'@state NVARCHAR(60) OUTPUT', @state = @ProcQSState OUTPUT;

                            IF @ProcQSState IS NULL OR @ProcQSState = 'OFF'
                            BEGIN
                                SELECT '14c. QUERY STORE for this procedure is OFF (or inaccessible) in ' + @DatabaseName AS QueryStoreStatus;
                            END
                            ELSE
                            BEGIN
                                SELECT '14c. QUERY STORE HISTORY FOR THIS PROCEDURE (' + @DatabaseName + ', last ' + @QueryStoreTimeRange + ')' AS Section;

                                SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
                                SELECT
                                    q.query_id,
                                    p.plan_id,
                                    qt.query_sql_text,
                                    SUM(rs.count_executions)                                             AS total_executions,
                                    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS DECIMAL(18,2))  AS total_cpu_ms,
                                    CAST(AVG(rs.avg_cpu_time) / 1000.0 AS DECIMAL(18,2))                  AS avg_cpu_ms,
                                    CAST(MAX(rs.max_duration) / 1000.0 AS DECIMAL(18,2))                  AS worst_duration_ms,
                                    CAST(AVG(rs.avg_duration) / 1000.0 AS DECIMAL(18,2))                  AS avg_duration_ms,
                                    CAST(MAX(rs.max_duration) / NULLIF(AVG(rs.avg_duration), 0) AS DECIMAL(18,2)) AS worst_vs_avg_duration_ratio,
                                    CAST(AVG(rs.avg_logical_io_reads) AS DECIMAL(18,2))                   AS avg_logical_reads,
                                    MAX(rs.last_execution_time)                                           AS last_execution_time
                                FROM sys.query_store_query q
                                    INNER JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
                                    INNER JOIN sys.query_store_plan p ON q.query_id = p.query_id
                                    INNER JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
                                WHERE q.object_id = ' + CAST(@ProcObjectId AS NVARCHAR(20)) + N'
                                GROUP BY q.query_id, p.plan_id, qt.query_sql_text
                                ORDER BY total_cpu_ms DESC;';
                                EXEC (@sql);

                                SELECT '14c-note. A worst_vs_avg_duration_ratio much greater than 1 across many executions suggests PARAMETER SNIFFING (same plan, wildly different runtimes per parameter value).' AS Note;
                            END;
                        END TRY
                        BEGIN CATCH
                            SELECT ERROR_MESSAGE() AS ProcQueryStoreError;
                        END CATCH
                    END;

                    ------------------------------------------------------------------
                    -- 14d/14e. Fetch this procedure's cached plan XML once, reuse for
                    --          missing-index mining and impacted-table discovery
                    ------------------------------------------------------------------
                    BEGIN TRY
                        INSERT INTO @ProcPlans (plan_handle, query_plan)
                        SELECT DISTINCT ps.plan_handle, qp.query_plan
                        FROM sys.dm_exec_procedure_stats ps
                            CROSS APPLY sys.dm_exec_query_plan(ps.plan_handle) qp
                        WHERE ps.object_id = @ProcObjectId AND ps.database_id = @ProcDbId
                            AND qp.query_plan IS NOT NULL;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS PlanFetchError;
                    END CATCH;

                    BEGIN TRY
                        IF EXISTS (SELECT 1 FROM @ProcPlans)
                        BEGIN
                            SELECT '14d. MISSING INDEX HINTS FROM THIS PROCEDURE''S PLAN(S)' AS Section;

                            ;WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
                            SELECT
                                pp.plan_handle,
                                mig.value('(@Impact)[1]', 'float')                        AS impact_pct,
                                mi.value('(@Database)[1]', 'nvarchar(128)')               AS database_name,
                                mi.value('(@Schema)[1]', 'nvarchar(128)')                 AS schema_name,
                                mi.value('(@Table)[1]', 'nvarchar(128)')                  AS table_name,
                                eq.cols   AS equality_columns,
                                ineq.cols AS inequality_columns,
                                inc.cols  AS included_columns,
                                N'CREATE NONCLUSTERED INDEX [IX_' +
                                    REPLACE(REPLACE(mi.value('(@Table)[1]', 'nvarchar(128)'), '[', ''), ']', '') +
                                    N'_Missing] ON ' + mi.value('(@Schema)[1]', 'nvarchar(128)') + N'.' + mi.value('(@Table)[1]', 'nvarchar(128)') +
                                    N' (' + ISNULL(eq.cols, '') +
                                    CASE WHEN eq.cols IS NOT NULL AND ineq.cols IS NOT NULL THEN N', ' ELSE N'' END +
                                    ISNULL(ineq.cols, '') + N')' +
                                    CASE WHEN inc.cols IS NOT NULL THEN N' INCLUDE (' + inc.cols + N')' ELSE N'' END AS suggested_create_index_ddl
                            FROM @ProcPlans pp
                                CROSS APPLY pp.query_plan.nodes('//MissingIndexGroup') AS t1(mig)
                                CROSS APPLY mig.nodes('MissingIndex') AS t2(mi)
                                OUTER APPLY (
                                    SELECT STUFF((SELECT ',' + c.value('(@Name)[1]', 'nvarchar(128)')
                                                  FROM mi.nodes('ColumnGroup[@Usage="EQUALITY"]/Column') AS cc(c)
                                                  FOR XML PATH('')), 1, 1, '') AS cols) eq
                                OUTER APPLY (
                                    SELECT STUFF((SELECT ',' + c.value('(@Name)[1]', 'nvarchar(128)')
                                                  FROM mi.nodes('ColumnGroup[@Usage="INEQUALITY"]/Column') AS cc(c)
                                                  FOR XML PATH('')), 1, 1, '') AS cols) ineq
                                OUTER APPLY (
                                    SELECT STUFF((SELECT ',' + c.value('(@Name)[1]', 'nvarchar(128)')
                                                  FROM mi.nodes('ColumnGroup[@Usage="INCLUDE"]/Column') AS cc(c)
                                                  FOR XML PATH('')), 1, 1, '') AS cols) inc
                            ORDER BY impact_pct DESC;
                        END
                        ELSE
                        BEGIN
                            SELECT '14d. MISSING INDEX HINTS' AS Section,
                                   'No cached plan XML available - run the procedure once, then re-run this analysis.' AS Message;
                        END;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS MissingIndexXmlError;
                    END CATCH;

                    BEGIN TRY
                        ;WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
                        INSERT INTO @ImpactedTables (database_name, schema_name, table_name, index_name, physical_op)
                        SELECT DISTINCT
                            REPLACE(REPLACE(obj.value('(@Database)[1]', 'nvarchar(128)'), '[', ''), ']', ''),
                            REPLACE(REPLACE(obj.value('(@Schema)[1]', 'nvarchar(128)'), '[', ''), ']', ''),
                            REPLACE(REPLACE(obj.value('(@Table)[1]', 'nvarchar(128)'), '[', ''), ']', ''),
                            REPLACE(REPLACE(obj.value('(@Index)[1]', 'nvarchar(128)'), '[', ''), ']', ''),
                            obj.value('(../../@PhysicalOp)[1]', 'nvarchar(60)')
                        FROM @ProcPlans pp
                            CROSS APPLY pp.query_plan.nodes('//Object') AS t(obj)
                        WHERE obj.value('(@Table)[1]', 'nvarchar(128)') IS NOT NULL;

                        SELECT '14e. TABLES / OBJECTS ACCESSED BY THIS PROCEDURE (from cached plan)' AS Section;
                        SELECT
                            database_name, schema_name, table_name,
                            COUNT(DISTINCT index_name)                        AS distinct_indexes_touched,
                            STRING_AGG(DISTINCT index_name, ', ')             AS indexes_touched,
                            STRING_AGG(DISTINCT physical_op, ', ')            AS access_operations
                        FROM @ImpactedTables
                        GROUP BY database_name, schema_name, table_name
                        ORDER BY database_name, schema_name, table_name;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS ImpactedTablesError;
                    END CATCH;

                    ------------------------------------------------------------------
                    -- 14f. Engine-tracked missing indexes on the impacted tables
                    --      (independent, server-wide cross-check vs. the plan-XML hints above)
                    ------------------------------------------------------------------
                    BEGIN TRY
                        IF EXISTS (SELECT 1 FROM @ImpactedTables)
                        BEGIN
                            SELECT '14f. ENGINE-TRACKED MISSING INDEXES ON IMPACTED TABLES (dm_db_missing_index_*, all queries - not just this proc)' AS Section;
                            SELECT DISTINCT
                                it.database_name, it.schema_name, it.table_name,
                                migs.avg_total_user_cost, migs.avg_user_impact,
                                migs.user_seeks, migs.user_scans,
                                mid.equality_columns, mid.inequality_columns, mid.included_columns,
                                CAST((migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) AS DECIMAL(18, 2)) AS improvement_measure
                            FROM @ImpactedTables it
                                CROSS APPLY (SELECT OBJECT_ID(QUOTENAME(it.database_name) + N'.' + QUOTENAME(it.schema_name) + N'.' + QUOTENAME(it.table_name)) AS obj_id) r
                                INNER JOIN sys.dm_db_missing_index_details mid ON mid.object_id = r.obj_id AND mid.database_id = DB_ID(it.database_name)
                                INNER JOIN sys.dm_db_missing_index_groups mig ON mig.index_handle = mid.index_handle
                                INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
                            ORDER BY improvement_measure DESC;
                        END;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS EngineMissingIndexError;
                    END CATCH;

                    ------------------------------------------------------------------
                    -- 14g-14k. Per-table deep dive: index inventory, fragmentation,
                    --          usage stats, row counts, FKs, triggers, stats freshness.
                    --          Looped per distinct database referenced by the plan
                    --          (normally just @DatabaseName) via dynamic SQL, since
                    --          sys.indexes/sys.stats/sys.triggers are catalog views
                    --          scoped to the current database context.
                    ------------------------------------------------------------------
                    IF EXISTS (SELECT 1 FROM @ImpactedTables)
                    BEGIN
                        DECLARE proc_db_cursor CURSOR LOCAL FAST_FORWARD FOR
                            SELECT DISTINCT database_name FROM @ImpactedTables WHERE database_name IS NOT NULL AND DB_ID(database_name) IS NOT NULL;
                        OPEN proc_db_cursor;
                        FETCH NEXT FROM proc_db_cursor INTO @DbCursorName;
                        WHILE @@FETCH_STATUS = 0
                        BEGIN
                            BEGIN TRY
                                SET @TableFilter = NULL;
                                SELECT @TableFilter = ISNULL(@TableFilter + N' UNION ALL ', N'') +
                                    N'SELECT ' + QUOTENAME(schema_name, '''') + N' AS sch, ' + QUOTENAME(table_name, '''') + N' AS tbl'
                                FROM (SELECT DISTINCT schema_name, table_name FROM @ImpactedTables WHERE database_name = @DbCursorName) x;

                                IF @TableFilter IS NOT NULL
                                BEGIN
                                    SET @sql = N'
USE ' + QUOTENAME(@DbCursorName) + N';

SELECT ''14g. INDEX INVENTORY - FRAGMENTATION - USAGE STATS (db: ' + @DbCursorName + N')'' AS Section;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    i.name AS index_name, i.index_id, i.type_desc,
    i.is_unique, i.is_primary_key, i.is_unique_constraint, i.fill_factor,
    STUFF((SELECT '','' + c.name + CASE WHEN ic.is_descending_key = 1 THEN '' DESC'' ELSE '''' END
           FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
           ORDER BY ic.key_ordinal FOR XML PATH('''')), 1, 1, '''') AS key_columns,
    STUFF((SELECT '','' + c.name
           FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
           FOR XML PATH('''')), 1, 1, '''') AS included_columns,
    ips.avg_fragmentation_in_percent, ips.page_count,
    ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates,
    ius.last_user_seek, ius.last_user_scan, ius.last_user_update
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.indexes i ON i.object_id = tbl.object_id AND i.type > 0
    OUTER APPLY (SELECT TOP 1 avg_fragmentation_in_percent, page_count FROM sys.dm_db_index_physical_stats(DB_ID(), i.object_id, i.index_id, NULL, ''LIMITED'')) ips
    LEFT JOIN sys.dm_db_index_usage_stats ius ON ius.database_id = DB_ID() AND ius.object_id = i.object_id AND ius.index_id = i.index_id
ORDER BY tgt.sch, tgt.tbl, i.index_id;

SELECT ''14h. ROW COUNTS / TABLE AGE (db: ' + @DbCursorName + N')'' AS Section;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    SUM(p.rows) AS row_count_approx,
    tbl.create_date, tbl.modify_date
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.partitions p ON p.object_id = tbl.object_id AND p.index_id IN (0, 1)
GROUP BY tgt.sch, tgt.tbl, tbl.create_date, tbl.modify_date;

SELECT ''14i. FOREIGN KEYS TOUCHING IMPACTED TABLES (db: ' + @DbCursorName + N')'' AS Section;
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) + ''.'' + OBJECT_NAME(fk.parent_object_id)         AS parent_table,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) + ''.'' + OBJECT_NAME(fk.referenced_object_id)  AS referenced_table,
    fk.name AS fk_name, fk.is_disabled, fk.is_not_trusted
FROM sys.foreign_keys fk
WHERE fk.parent_object_id IN (SELECT tbl.object_id FROM (' + @TableFilter + N') tgt JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch)
   OR fk.referenced_object_id IN (SELECT tbl.object_id FROM (' + @TableFilter + N') tgt JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch);

SELECT ''14j. TRIGGERS ON IMPACTED TABLES (db: ' + @DbCursorName + N')'' AS Section;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    tr.name AS trigger_name, tr.is_disabled, tr.is_instead_of_trigger
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.triggers tr ON tr.parent_id = tbl.object_id;

SELECT ''14k. STATISTICS FRESHNESS ON IMPACTED TABLES (db: ' + @DbCursorName + N')'' AS Section;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    s.name AS stats_name,
    sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.stats s ON s.object_id = tbl.object_id
    OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
ORDER BY sp.modification_counter DESC;
';
                                    EXEC (@sql);
                                END;
                            END TRY
                            BEGIN CATCH
                                SELECT ERROR_MESSAGE() AS PerTableDeepDiveError, @DbCursorName AS FailedDatabase;
                            END CATCH;

                            FETCH NEXT FROM proc_db_cursor INTO @DbCursorName;
                        END;
                        CLOSE proc_db_cursor;
                        DEALLOCATE proc_db_cursor;
                    END;
                END;
            END;
        END;
    END;

    SELECT '=== usp_PerformanceTroubleshoot complete ===' AS RunInfo;
END;
GO
