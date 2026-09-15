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
            of its cached plan XML (candidates, not automatic CREATE INDEX DDL),
            tables/indexes visible in those plans, index inventory + optional fragmentation +
            usage stats + row counts for those tables, FKs, triggers, and
            statistics freshness - the same checklist a DBA would work
            through by hand when asked "why is this proc slow?".

  Notes   : - Created in tempdb to match the existing usp_CPUUsage /
              usp_SQLInformation / usp_IndexAnalysis convention in this repo.
              Objects in tempdb survive until the next SQL Server restart;
              move it to a persistent admin DB (e.g. DBATasks) if you need
              it to survive restarts.
            - Collection errors are returned by section. TRY/CATCH does not
              catch same-scope compilation failures, connection loss or a
              client cancellation. Unsupported platforms are rejected.
            - Targets SQL Server 2016 SP1+ / Managed Instance, not Azure SQL
              Database. Requires VIEW SERVER STATE (SQL Server 2022+:
              VIEW SERVER PERFORMANCE STATE for performance DMVs), plus
              appropriate access/metadata permissions in target databases.
            - Does not modify application data/configuration or clear DMVs.
              Uses tempdb work tables and consumes CPU, IO and a worker.
              Do not run inside an application transaction or with
              IMPLICIT_TRANSACTIONS enabled. WAITFOR retains a worker;
              do not launch many simultaneous samples.
            - dm_exec_query_plan returns cached compiled plans, NOT live
              operator runtime statistics. Both request and top-query plan
              retrieval are opt-in. A procedure deep-dive still mines XML.
            - No universal latency, fragmentation or wait-percentage cutoff
              proves a bottleneck. Correlate interval data with workload.
            - @WaitForDelay NULL/blank = cumulative counters. Otherwise,
              sample once on each side of one WAITFOR DELAY, before running
              report queries. IO/selected throughput rates use actual
              source-specific elapsed time, not the requested delay.
              Waits are instance-wide even when a database is specified.
              Wait time accumulates across workers and is not CPU percent;
              waits spanning a boundary can distort short samples.
              Cache, missing-index, index-usage and procedure totals remain
              cumulative; requests/schedulers/memory/tempdb are snapshots.
            - Sampling is not atomic across DMVs. Detectable counter
              decreases invalidate deltas; a reset followed by counters
              overtaking the baseline cannot always be detected. Do not
              clear counters, restart/offline databases or replace files
              during a sample. No IO = NULL latency, not zero latency.
            - Query Store includes overlapping aggregation intervals; it
              cannot provide exact execution-level lookback boundaries.

  Usage   :
      -- Quick pass, no query store, no plans, no fragmentation
      EXEC tempdb..usp_PerformanceTroubleshoot;

      -- Ten-second interval: IO latency/IOPS/MBps, waits, throughput
      EXEC tempdb..usp_PerformanceTroubleshoot @WaitForDelay = '00:00:10';

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
           @IncludeMemoryHealth = 0, @IncludeIOStats = 0,
           @IncludePerformanceCounters = 0;   -- isolate section 14 only
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
    @MinutesBack            INT           = 30,      -- CPU ring-buffer lookback in minutes
    @QueryStoreTimeRange    NVARCHAR(10)  = '1H',     -- Query Store lookback: suffix Mi=minutes, H=hours, D=days, M=months (e.g. '30Mi','6H','1D','1M')
    @IncludeServerInfo      BIT           = 1,
    @IncludeCPUHistory      BIT           = 1,
    @IncludeSchedulerHealth BIT           = 1,
    @IncludeWaitStats       BIT           = 1,
    @IncludeActiveRequests  BIT           = 1,
    @IncludeLiveQueryPlans  BIT           = 0,       -- Legacy name: compiled cached XML for current requests, NOT live runtime stats
    @IncludeBlocking        BIT           = 1,
    @IncludeTopQueries      BIT           = 1,
    @IncludeMissingIndexes  BIT           = 1,
    @IncludeIndexFragment   BIT           = 0,       -- Expensive; opt-in, requires @DatabaseName
    @IncludeTempdbHealth    BIT           = 1,
    @IncludeMemoryHealth    BIT           = 1,
    @IncludeIOStats         BIT           = 1,
    @IncludeQueryStore      BIT           = 1,       -- Only runs if @DatabaseName is passed AND Query Store is ON for that DB
    @ProcedureName          NVARCHAR(517) = NULL,    -- One/two-part procedure name; defaults to dbo
    @IncludeProcedureAnalysis BIT         = 1,       -- Only runs when @ProcedureName is supplied
    @WaitForDelay           VARCHAR(32)   = NULL,    -- NULL/blank = no wait; strict hh:mm:ss, 1 second to 23:59:59
    @IncludePerformanceCounters BIT       = 1,       -- Selected cumulative counters; rates only with sampling
    @IncludeCachedQueryPlans BIT          = 0,       -- Top-query compiled XML; procedure deep-dive still mines XML
    @IncludeBufferPoolScan  BIT           = 0        -- Potentially large dm_os_buffer_descriptors scan
)
AS
BEGIN
    SET NOCOUNT ON;
    SET QUOTED_IDENTIFIER ON;
    SET DEADLOCK_PRIORITY LOW;

    DECLARE @sql          NVARCHAR(MAX);
    DECLARE @ts_now       BIGINT;
    DECLARE @QSState      NVARCHAR(60);
    DECLARE @QSStartTime  DATETIMEOFFSET(7);
    DECLARE @QSEndTime    DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), '+00:00');
    DECLARE @QSStartTimeStr NVARCHAR(30);

    -- Section 14 (procedure deep-dive) working variables
    DECLARE @ProcDbId        INT;
    DECLARE @ProcObjectId    INT;
    DECLARE @ProcFullName    NVARCHAR(776);
    DECLARE @ProcQSState     NVARCHAR(60);
    DECLARE @TableFilter     NVARCHAR(MAX);
    DECLARE @DbCursorName    SYSNAME;
    DECLARE @ProcPlans       TABLE (plan_handle VARBINARY(64), query_plan XML);
    DECLARE @ImpactedTables TABLE
    (
        database_name SYSNAME COLLATE Latin1_General_100_BIN2,
        schema_name SYSNAME COLLATE Latin1_General_100_BIN2,
        table_name SYSNAME COLLATE Latin1_General_100_BIN2,
        index_name SYSNAME COLLATE Latin1_General_100_BIN2 NULL,
        physical_op NVARCHAR(60) NULL
    );

    SET @DatabaseName = NULLIF(LTRIM(RTRIM(@DatabaseName)), N'');
    SET @ProcedureName = NULLIF(LTRIM(RTRIM(@ProcedureName)), N'');
    SET @WaitForDelay = NULLIF(LTRIM(RTRIM(@WaitForDelay)), '');
    DECLARE @DatabaseId INT = CASE WHEN @DatabaseName IS NOT NULL THEN DB_ID(@DatabaseName) END;
    DECLARE @Delay TIME(0) = TRY_CONVERT(TIME(0), @WaitForDelay);
    DECLARE @Sample BIT = CASE WHEN @WaitForDelay IS NULL THEN 0 ELSE 1 END;
    DECLARE @QSRange NVARCHAR(10) = UPPER(LTRIM(RTRIM(@QueryStoreTimeRange)));
    DECLARE @QSUnit VARCHAR(2), @QSAmount INT;

    IF CONVERT(INT, SERVERPROPERTY('ProductMajorVersion')) < 13
       OR CONVERT(INT, SERVERPROPERTY('EngineEdition')) NOT IN (2, 3, 4, 8)
        THROW 50000, 'This procedure targets SQL Server 2016 SP1+ and Azure SQL Managed Instance.', 1;
    IF @@TRANCOUNT > 0
        THROW 50000, 'Run this diagnostic outside an explicit transaction.', 1;
    IF (@@OPTIONS & 2) = 2
        THROW 50000, 'Run this diagnostic with IMPLICIT_TRANSACTIONS OFF.', 1;
    IF @TopN IS NULL OR @TopN NOT BETWEEN 1 AND 1000
        THROW 50000, '@TopN must be between 1 and 1000.', 1;
    IF @MinutesBack IS NULL OR @MinutesBack NOT BETWEEN 1 AND 10080
        THROW 50000, '@MinutesBack must be between 1 and 10080.', 1;
    IF @DatabaseName IS NOT NULL
       AND (@DatabaseId IS NULL OR ISNULL(HAS_DBACCESS(@DatabaseName), 0) <> 1
            OR NOT EXISTS (SELECT 1 FROM sys.databases WHERE database_id = @DatabaseId AND state = 0))
        THROW 50000, '@DatabaseName must identify an accessible, online database.', 1;
    IF @ProcedureName IS NOT NULL AND @DatabaseName IS NULL
        THROW 50000, '@ProcedureName requires @DatabaseName.', 1;
    IF @Sample = 1 AND
       (LEN(@WaitForDelay) <> 8
        OR @WaitForDelay COLLATE Latin1_General_100_BIN2 NOT LIKE '[0-2][0-9]:[0-5][0-9]:[0-5][0-9]'
        OR @Delay IS NULL OR @Delay = CONVERT(TIME(0), '00:00:00'))
        THROW 50000, '@WaitForDelay must be hh:mm:ss from 00:00:01 through 23:59:59, or NULL/blank.', 1;

    IF @IncludeQueryStore = 1 AND @DatabaseName IS NOT NULL
    BEGIN
        SET @QSUnit = CASE WHEN RIGHT(@QSRange, 2) = N'MI' THEN 'MI' ELSE RIGHT(@QSRange, 1) END;
        SET @QSAmount = TRY_CONVERT(INT, LEFT(@QSRange, CASE WHEN LEN(@QSRange) > LEN(@QSUnit) THEN LEN(@QSRange) - LEN(@QSUnit) ELSE 0 END));
        IF @QSUnit IS NULL OR @QSUnit NOT IN ('MI', 'H', 'D', 'M') OR @QSAmount IS NULL OR @QSAmount < 1
            THROW 50000, '@QueryStoreTimeRange must be a positive integer followed by Mi, H, D or M.', 1;
        BEGIN TRY
            SET @QSStartTime = CASE @QSUnit
                WHEN 'MI' THEN DATEADD(MINUTE, -@QSAmount, @QSEndTime)
                WHEN 'H' THEN DATEADD(HOUR, -@QSAmount, @QSEndTime)
                WHEN 'D' THEN DATEADD(DAY, -@QSAmount, @QSEndTime)
                WHEN 'M' THEN DATEADD(MONTH, -@QSAmount, @QSEndTime) END;
        END TRY
        BEGIN CATCH
            THROW 50000, '@QueryStoreTimeRange exceeds the supported datetime range.', 1;
        END CATCH;
    END;

    -- Persist both unranked snapshots; TOP and idle-wait filtering come later.
    CREATE TABLE #WaitSamples
    (
        sample_no TINYINT NOT NULL, wait_type NVARCHAR(60) NOT NULL,
        waiting_tasks_count BIGINT NOT NULL, wait_time_ms BIGINT NOT NULL,
        max_wait_time_ms BIGINT NOT NULL, signal_wait_time_ms BIGINT NOT NULL,
        PRIMARY KEY (sample_no, wait_type)
    );
    CREATE TABLE #IOSamples
    (
        sample_no TINYINT NOT NULL, database_id INT NOT NULL, file_id INT NOT NULL,
        file_handle VARBINARY(8) NULL, file_guid UNIQUEIDENTIFIER NULL,
        database_name SYSNAME NULL, type_desc NVARCHAR(60) NULL, physical_name NVARCHAR(260) NULL,
        num_of_reads BIGINT NOT NULL, num_of_writes BIGINT NOT NULL,
        num_of_bytes_read BIGINT NOT NULL, num_of_bytes_written BIGINT NOT NULL,
        io_stall_read_ms BIGINT NOT NULL, io_stall_write_ms BIGINT NOT NULL,
        size_on_disk_bytes BIGINT NOT NULL,
        PRIMARY KEY (sample_no, database_id, file_id)
    );
    CREATE TABLE #CounterSamples
    (
        sample_no TINYINT NOT NULL, object_name NVARCHAR(128) NOT NULL,
        counter_name NVARCHAR(128) NOT NULL, instance_name NVARCHAR(128) NOT NULL,
        cntr_type INT NOT NULL, cntr_value BIGINT NOT NULL,
        PRIMARY KEY (sample_no, object_name, counter_name, instance_name)
    );
    CREATE TABLE #CaptureTimes
    (
        sample_no TINYINT NOT NULL, source_name VARCHAR(20) NOT NULL,
        capture_utc DATETIME2(7) NOT NULL, start_ticks BIGINT NOT NULL, end_ticks BIGINT NULL,
        succeeded BIT NOT NULL DEFAULT 0,
        PRIMARY KEY (sample_no, source_name)
    );
    DECLARE @Pass TINYINT = CASE WHEN @Sample = 1 THEN 1 ELSE 2 END;
    DECLARE @WaitSeconds DECIMAL(19,6), @IOSeconds DECIMAL(19,6), @CounterSeconds DECIMAL(19,6);
    WHILE @Pass <= 2
    BEGIN
        IF @IncludeWaitStats = 1 OR @IncludeSchedulerHealth = 1
        BEGIN
            BEGIN TRY
                INSERT #CaptureTimes (sample_no, source_name, capture_utc, start_ticks)
                    SELECT @Pass, 'Waits', SYSUTCDATETIME(), ms_ticks FROM sys.dm_os_sys_info;
                INSERT #WaitSamples
                    SELECT @Pass, wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms
                    FROM sys.dm_os_wait_stats;
                UPDATE #CaptureTimes SET succeeded = 1, end_ticks = (SELECT ms_ticks FROM sys.dm_os_sys_info)
                    WHERE sample_no = @Pass AND source_name = 'Waits';
            END TRY
            BEGIN CATCH
                DELETE FROM #WaitSamples WHERE sample_no = @Pass;
                SELECT 'Wait capture' AS FailedSection, @Pass AS SampleNumber, ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
            END CATCH;
        END;
        IF @IncludeIOStats = 1
        BEGIN
            BEGIN TRY
                INSERT #CaptureTimes (sample_no, source_name, capture_utc, start_ticks)
                    SELECT @Pass, 'IO', SYSUTCDATETIME(), ms_ticks FROM sys.dm_os_sys_info;
                INSERT #IOSamples
                    SELECT @Pass, v.database_id, v.file_id, v.file_handle, mf.file_guid,
                           DB_NAME(v.database_id), mf.type_desc, mf.physical_name,
                           v.num_of_reads, v.num_of_writes, v.num_of_bytes_read, v.num_of_bytes_written,
                           v.io_stall_read_ms, v.io_stall_write_ms, v.size_on_disk_bytes
                    FROM sys.dm_io_virtual_file_stats(@DatabaseId, NULL) v
                    LEFT JOIN sys.master_files mf ON mf.database_id = v.database_id AND mf.file_id = v.file_id;
                UPDATE #CaptureTimes SET succeeded = 1, end_ticks = (SELECT ms_ticks FROM sys.dm_os_sys_info)
                    WHERE sample_no = @Pass AND source_name = 'IO';
            END TRY
            BEGIN CATCH
                DELETE FROM #IOSamples WHERE sample_no = @Pass;
                SELECT 'IO capture' AS FailedSection, @Pass AS SampleNumber, ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
            END CATCH;
        END;
        IF @IncludePerformanceCounters = 1
        BEGIN
            BEGIN TRY
                INSERT #CaptureTimes (sample_no, source_name, capture_utc, start_ticks)
                    SELECT @Pass, 'Counters', SYSUTCDATETIME(), ms_ticks FROM sys.dm_os_sys_info;
                INSERT #CounterSamples
                    SELECT @Pass, object_name, counter_name, instance_name, cntr_type, cntr_value
                    FROM sys.dm_os_performance_counters
                    WHERE cntr_type IN (272696320, 272696576)
                      AND ((object_name LIKE N'%:SQL Statistics'
                            AND counter_name IN (N'Batch Requests/sec', N'SQL Compilations/sec', N'SQL Re-Compilations/sec'))
                        OR (object_name LIKE N'%:Buffer Manager'
                            AND counter_name IN (N'Page reads/sec', N'Page writes/sec', N'Lazy writes/sec'))
                        OR (object_name LIKE N'%:Databases'
                            AND counter_name IN (N'Transactions/sec', N'Log Bytes Flushed/sec')
                            AND ((@DatabaseName IS NULL AND instance_name = N'_Total') OR instance_name = @DatabaseName)));
                UPDATE #CaptureTimes SET succeeded = 1, end_ticks = (SELECT ms_ticks FROM sys.dm_os_sys_info)
                    WHERE sample_no = @Pass AND source_name = 'Counters';
            END TRY
            BEGIN CATCH
                DELETE FROM #CounterSamples WHERE sample_no = @Pass;
                SELECT 'Counter capture' AS FailedSection, @Pass AS SampleNumber, ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
            END CATCH;
        END;
        IF @Pass = 1
            WAITFOR DELAY @WaitForDelay;
        SET @Pass += 1;
    END;

    SELECT @WaitSeconds = CASE WHEN e.start_ticks > b.start_ticks THEN (e.start_ticks - b.start_ticks) / 1000.0 END
    FROM #CaptureTimes b JOIN #CaptureTimes e ON b.source_name = e.source_name
    WHERE b.sample_no = 1 AND e.sample_no = 2 AND b.source_name = 'Waits' AND b.succeeded = 1 AND e.succeeded = 1;
    SELECT @IOSeconds = CASE WHEN e.start_ticks > b.start_ticks THEN (e.start_ticks - b.start_ticks) / 1000.0 END
    FROM #CaptureTimes b JOIN #CaptureTimes e ON b.source_name = e.source_name
    WHERE b.sample_no = 1 AND e.sample_no = 2 AND b.source_name = 'IO' AND b.succeeded = 1 AND e.succeeded = 1;
    SELECT @CounterSeconds = CASE WHEN e.start_ticks > b.start_ticks THEN (e.start_ticks - b.start_ticks) / 1000.0 END
    FROM #CaptureTimes b JOIN #CaptureTimes e ON b.source_name = e.source_name
    WHERE b.sample_no = 1 AND e.sample_no = 2 AND b.source_name = 'Counters' AND b.succeeded = 1 AND e.succeeded = 1;

    DECLARE @WaitReset BIT = 0;
    IF @Sample = 1 AND EXISTS
    (
        SELECT 1 FROM #WaitSamples b LEFT JOIN #WaitSamples e ON e.sample_no = 2 AND b.wait_type = e.wait_type
        WHERE b.sample_no = 1 AND
            (e.wait_type IS NULL OR e.waiting_tasks_count < b.waiting_tasks_count
             OR e.wait_time_ms < b.wait_time_ms OR e.signal_wait_time_ms < b.signal_wait_time_ms
             OR e.max_wait_time_ms < b.max_wait_time_ms
             OR e.wait_time_ms - b.wait_time_ms < e.signal_wait_time_ms - b.signal_wait_time_ms)
    )
        SET @WaitReset = 1;

    SELECT e.wait_type,
        e.waiting_tasks_count - CASE WHEN @Sample = 1 THEN COALESCE(b.waiting_tasks_count, 0) ELSE 0 END AS waiting_tasks_count,
        e.wait_time_ms - CASE WHEN @Sample = 1 THEN COALESCE(b.wait_time_ms, 0) ELSE 0 END AS wait_time_ms,
        e.signal_wait_time_ms - CASE WHEN @Sample = 1 THEN COALESCE(b.signal_wait_time_ms, 0) ELSE 0 END AS signal_wait_time_ms,
        CASE WHEN @Sample = 0 THEN e.max_wait_time_ms END AS max_wait_time_ms
    INTO #EffectiveWaits
    FROM #WaitSamples e LEFT JOIN #WaitSamples b ON b.sample_no = 1 AND b.wait_type = e.wait_type
    WHERE e.sample_no = 2 AND (@Sample = 0 OR (@WaitReset = 0 AND @WaitSeconds IS NOT NULL));
    -- Filter only well-understood idle/background waits; retain EXECSYNC,
    -- PREEMPTIVE_OS_* and other potentially actionable waits.
    DELETE FROM #EffectiveWaits
    WHERE wait_type IN
    (
        N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER',
        N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT',
        N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD',
        N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'FT_IFTS_SCHEDULER_IDLE_WAIT',
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE',
        N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE',
        N'ONDEMAND_TASK_QUEUE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS',
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        N'REQUEST_FOR_DEADLOCK_SEARCH', N'SLEEP_BPOOL_FLUSH', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK',
        N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT'
    );

    ----------------------------------------------------------------------
    -- 0. Run header
    ----------------------------------------------------------------------
    SELECT '=== usp_PerformanceTroubleshoot @ ' + CONVERT(VARCHAR(23), GETDATE(), 121) + ' ===' AS RunInfo,
           @@SERVERNAME       AS ServerName,
           ISNULL(@DatabaseName, '<all databases>') AS DatabaseScope,
           @TopN              AS TopN,
           CASE WHEN @Sample = 1 THEN 'INTERVAL' ELSE 'CUMULATIVE' END AS CounterMode,
           @WaitForDelay AS RequestedDelay, @WaitSeconds AS WaitSampleSeconds,
           @IOSeconds AS IOSampleSeconds, @CounterSeconds AS CounterSampleSeconds,
           'Waits and SQL/Buffer Manager counters are instance-wide; other sections label their scope.' AS ScopeNote;
    SELECT sample_no, source_name, capture_utc, end_ticks - start_ticks AS capture_duration_ms, succeeded
    FROM #CaptureTimes ORDER BY sample_no, source_name;
    IF @Sample = 1 AND (@IncludeWaitStats = 1 OR @IncludeSchedulerHealth = 1)
       AND (@WaitReset = 1 OR @WaitSeconds IS NULL)
        SELECT 'Wait interval unavailable: capture failed, elapsed time invalid, or counters decreased/disappeared. No cumulative fallback.' AS Warning;

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

            SET @ts_now = (SELECT ms_ticks FROM sys.dm_os_sys_info);

            SELECT TOP (256)
                   SQLProcessUtilization                                          AS SQLServerCPUUtilPct,
                   SystemIdle                                                     AS SystemIdlePct,
                   100 - SystemIdle - SQLProcessUtilization                       AS OtherProcessCPUUtilPct,
                   DATEADD(SECOND, -CONVERT(INT, (@ts_now - [timestamp]) / 1000), GETDATE()) AS EventTime
            FROM
            (
                SELECT record.value('(./Record/@id)[1]', 'int')                                                       AS record_id,
                       record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')             AS SystemIdle,
                       record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')     AS SQLProcessUtilization,
                       [timestamp]
                FROM
                (
                    SELECT [timestamp], TRY_CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                          AND record LIKE N'%<SystemHealth>%'
                          AND [timestamp] BETWEEN @ts_now - CONVERT(BIGINT, @MinutesBack) * 60000 AND @ts_now
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
            SELECT '3a. SIGNAL WAIT SHARE (filtered waits; not a standalone CPU-pressure threshold)' AS Section;
            SELECT
                CAST(100.0 * SUM(CONVERT(DECIMAL(38,0), signal_wait_time_ms)) / NULLIF(SUM(CONVERT(DECIMAL(38,0), wait_time_ms)), 0) AS DECIMAL(9, 2)) AS SignalWaitPct,
                CASE WHEN @Sample = 1 THEN 'INTERVAL' ELSE 'CUMULATIVE' END AS CounterMode
            FROM #EffectiveWaits;

            SELECT '3b. VISIBLE ONLINE SCHEDULERS (point-in-time; repeated queues merit investigation)' AS Section;
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
            WHERE status = N'VISIBLE ONLINE'
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
            SELECT '4. TOP WAIT STATS (instance-wide, idle waits excluded)' AS Section,
                   CASE WHEN @Sample = 1 THEN 'INTERVAL' ELSE 'CUMULATIVE since restart/clear' END AS CounterMode;
            SELECT TOP (@TopN)
                wait_type,
                waiting_tasks_count,
                wait_time_ms,
                max_wait_time_ms,
                signal_wait_time_ms,
                wait_time_ms - signal_wait_time_ms                              AS resource_wait_time_ms,
                CAST(100.0 * wait_time_ms / NULLIF(SUM(CONVERT(DECIMAL(38,0), wait_time_ms)) OVER (), 0) AS DECIMAL(9, 2)) AS pct_of_filtered_wait_time,
                1.0 * wait_time_ms / NULLIF(waiting_tasks_count, 0) AS avg_wait_ms,
                @WaitSeconds AS sample_seconds,
                wait_time_ms / NULLIF(@WaitSeconds * 1000.0, 0) AS wait_seconds_per_elapsed_second
            FROM #EffectiveWaits
            WHERE wait_time_ms > 0
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
                SELECT TOP (@TopN)
                    r.session_id,
                    r.request_id,
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
                    CONVERT(BIGINT, r.granted_query_memory) * 8    AS granted_memory_kb,
                    r.open_transaction_count,
                    r.percent_complete,
                    DB_NAME(r.database_id)                        AS database_name,
                    s.login_name,
                    s.host_name,
                    s.program_name,
                    r.start_time,
                    est.text                                      AS sql_text,
                    SUBSTRING(est.text, r.statement_start_offset / 2 + 1,
                        (CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(est.text) ELSE r.statement_end_offset END
                         - r.statement_start_offset) / 2 + 1) AS current_statement,
                    qp.query_plan
                FROM sys.dm_exec_requests r
                    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) est
                    OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) qp
                WHERE r.session_id <> @@SPID
                      AND s.is_user_process = 1
                      AND (@DatabaseId IS NULL OR r.database_id = @DatabaseId)
                ORDER BY r.cpu_time DESC;
            END
            ELSE
            BEGIN
                SELECT TOP (@TopN)
                    r.session_id,
                    r.request_id,
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
                    CONVERT(BIGINT, r.granted_query_memory) * 8    AS granted_memory_kb,
                    r.open_transaction_count,
                    r.percent_complete,
                    DB_NAME(r.database_id)                        AS database_name,
                    s.login_name,
                    s.host_name,
                    s.program_name,
                    r.start_time,
                    est.text                                      AS sql_text,
                    SUBSTRING(est.text, r.statement_start_offset / 2 + 1,
                        (CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(est.text) ELSE r.statement_end_offset END
                         - r.statement_start_offset) / 2 + 1) AS current_statement
                FROM sys.dm_exec_requests r
                    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) est
                WHERE r.session_id <> @@SPID
                      AND s.is_user_process = 1
                      AND (@DatabaseId IS NULL OR r.database_id = @DatabaseId)
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
            SELECT '6. BLOCKING EDGES (instance-wide to preserve cross-database blockers; not capped)' AS Section;

            IF OBJECT_ID('tempdb..#BlockingSessions') IS NOT NULL
                DROP TABLE #BlockingSessions;

            SELECT
                r.session_id,
                r.request_id,
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
                OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) est
            WHERE r.blocking_session_id <> 0 AND r.session_id <> @@SPID;

            IF EXISTS (SELECT 1 FROM #BlockingSessions)
            BEGIN
                SELECT '6a. HEAD BLOCKERS (blocking others, may be idle/not currently executing)' AS Section;
                SELECT DISTINCT
                    bs.blocking_session_id                                       AS head_blocker_session_id,
                    s.login_name,
                    s.host_name,
                    s.program_name,
                    s.status,
                    s.open_transaction_count,
                    s.last_request_end_time,
                    est.text AS head_blocker_last_sql,
                    (SELECT COUNT(DISTINCT b2.session_id) FROM #BlockingSessions b2 WHERE b2.blocking_session_id = bs.blocking_session_id) AS directly_blocked_sessions
                FROM #BlockingSessions bs
                    LEFT JOIN sys.dm_exec_sessions s ON s.session_id = bs.blocking_session_id
                    LEFT JOIN sys.dm_exec_connections c ON c.session_id = bs.blocking_session_id
                    OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) est
                WHERE bs.blocking_session_id > 0
                  AND NOT EXISTS (SELECT 1 FROM #BlockingSessions parent WHERE parent.session_id = bs.blocking_session_id)
                ORDER BY directly_blocked_sessions DESC;

                SELECT '6b. FULL BLOCKING DETAIL (negative blocker IDs are engine special owners, not session IDs; see version-specific documentation)' AS Section;
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
            SELECT '7a. TOP QUERIES BY TOTAL CPU TIME (cumulative completed executions; database filter is compilation context, not all referenced databases)' AS Section;
            ;WITH TopByCPU AS
            (
                SELECT TOP (@TopN)
                       qs.sql_handle, qs.plan_handle, qs.execution_count, qs.total_worker_time, qs.total_elapsed_time,
                       qs.total_logical_reads, qs.total_physical_reads, qs.total_logical_writes,
                       qs.creation_time, qs.last_execution_time, qs.statement_start_offset, qs.statement_end_offset
                FROM sys.dm_exec_query_stats qs
                WHERE (@DatabaseId IS NULL OR EXISTS (SELECT 1 FROM sys.dm_exec_plan_attributes(qs.plan_handle) a WHERE a.attribute = N'dbid' AND CONVERT(INT, a.value) = @DatabaseId))
                ORDER BY qs.total_worker_time DESC
            )
            SELECT
                DB_NAME(COALESCE(qt.dbid, CONVERT(INT, pa.value)))                    AS compilation_database_name,
                t.execution_count,
                t.total_worker_time / 1000.0                                         AS total_cpu_ms,
                t.total_worker_time / 1000.0 / NULLIF(t.execution_count, 0)           AS avg_cpu_ms,
                t.total_elapsed_time / 1000.0                                        AS total_duration_ms,
                t.total_elapsed_time / 1000.0 / NULLIF(t.execution_count, 0)          AS avg_duration_ms,
                t.total_logical_reads,
                1.0 * t.total_logical_reads / NULLIF(t.execution_count, 0)            AS avg_logical_reads,
                t.total_physical_reads,
                t.total_logical_writes,
                t.creation_time,
                t.last_execution_time,
                SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1,
                    ((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END
                      - t.statement_start_offset) / 2) + 1)                          AS statement_text,
                qp.query_plan
            FROM TopByCPU t
                OUTER APPLY sys.dm_exec_sql_text(t.sql_handle) qt
                OUTER APPLY (SELECT value FROM sys.dm_exec_plan_attributes(t.plan_handle) WHERE attribute = N'dbid') pa
                OUTER APPLY sys.dm_exec_query_plan(CASE WHEN @IncludeCachedQueryPlans = 1 THEN t.plan_handle END) qp
            ORDER BY t.total_worker_time DESC;

            SELECT '7b. TOP QUERIES BY TOTAL LOGICAL READS (cumulative buffer accesses, not storage IO)' AS Section;
            ;WITH TopByReads AS
            (
                SELECT TOP (@TopN)
                       qs.sql_handle, qs.plan_handle, qs.execution_count, qs.total_worker_time, qs.total_elapsed_time,
                       qs.total_logical_reads, qs.total_physical_reads, qs.total_logical_writes,
                       qs.creation_time, qs.last_execution_time, qs.statement_start_offset, qs.statement_end_offset
                FROM sys.dm_exec_query_stats qs
                WHERE (@DatabaseId IS NULL OR EXISTS (SELECT 1 FROM sys.dm_exec_plan_attributes(qs.plan_handle) a WHERE a.attribute = N'dbid' AND CONVERT(INT, a.value) = @DatabaseId))
                ORDER BY qs.total_logical_reads DESC
            )
            SELECT
                DB_NAME(COALESCE(qt.dbid, CONVERT(INT, pa.value)))                    AS compilation_database_name,
                t.execution_count,
                t.total_logical_reads,
                1.0 * t.total_logical_reads / NULLIF(t.execution_count, 0)            AS avg_logical_reads,
                t.total_worker_time / 1000.0                                         AS total_cpu_ms,
                t.total_elapsed_time / 1000.0                                        AS total_duration_ms,
                t.total_physical_reads,
                t.last_execution_time,
                SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1,
                    ((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END
                      - t.statement_start_offset) / 2) + 1)                          AS statement_text
            FROM TopByReads t
                OUTER APPLY sys.dm_exec_sql_text(t.sql_handle) qt
                OUTER APPLY (SELECT value FROM sys.dm_exec_plan_attributes(t.plan_handle) WHERE attribute = N'dbid') pa
            ORDER BY t.total_logical_reads DESC;

            SELECT '7c. TOP QUERIES BY EXECUTION COUNT (cumulative; frequency does not establish parameter sensitivity)' AS Section;
            ;WITH TopByExec AS
            (
                SELECT TOP (@TopN)
                       qs.sql_handle, qs.plan_handle, qs.execution_count, qs.total_worker_time, qs.total_elapsed_time,
                       qs.last_execution_time, qs.statement_start_offset, qs.statement_end_offset
                FROM sys.dm_exec_query_stats qs
                WHERE (@DatabaseId IS NULL OR EXISTS (SELECT 1 FROM sys.dm_exec_plan_attributes(qs.plan_handle) a WHERE a.attribute = N'dbid' AND CONVERT(INT, a.value) = @DatabaseId))
                ORDER BY qs.execution_count DESC
            )
            SELECT
                DB_NAME(COALESCE(qt.dbid, CONVERT(INT, pa.value)))                    AS compilation_database_name,
                t.execution_count,
                t.total_worker_time / 1000.0                                         AS total_cpu_ms,
                t.total_worker_time / 1000.0 / NULLIF(t.execution_count, 0)           AS avg_cpu_ms,
                t.last_execution_time,
                SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1,
                    ((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END
                      - t.statement_start_offset) / 2) + 1)                          AS statement_text
            FROM TopByExec t
                OUTER APPLY sys.dm_exec_sql_text(t.sql_handle) qt
                OUTER APPLY (SELECT value FROM sys.dm_exec_plan_attributes(t.plan_handle) WHERE attribute = N'dbid') pa
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
            SELECT '8. MISSING INDEX CANDIDATES (cumulative optimizer estimates, not prescriptions; check overlap and write cost)' AS Section;
            SELECT TOP (@TopN)
                DB_NAME(mid.database_id)                                                                          AS database_name,
                migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans)   AS improvement_measure,
                OBJECT_NAME(mid.object_id, mid.database_id)                                                       AS table_name,
                OBJECT_SCHEMA_NAME(mid.object_id, mid.database_id)                                                AS schema_name,
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
       AND (@ProcedureName IS NULL OR ISNULL(@IncludeProcedureAnalysis, 0) = 0)
    BEGIN
        BEGIN TRY
            SELECT '9. INDEX FRAGMENTATION (' + @DatabaseName + ', opt-in LIMITED scan; thresholds shortlist evidence, not rebuild instructions)' AS Section;

            SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
            SELECT TOP (@n)
                DB_NAME()                                                        AS database_name,
                OBJECT_SCHEMA_NAME(ps.object_id) + N''.'' + OBJECT_NAME(ps.object_id) AS table_name,
                i.name                                                           AS index_name,
                ps.partition_number,
                ps.alloc_unit_type_desc,
                ps.index_type_desc,
                CAST(ps.avg_fragmentation_in_percent AS DECIMAL(5,2))            AS avg_fragmentation_pct,
                ps.fragment_count,
                ps.page_count
            FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, N''LIMITED'') ps
                INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
            WHERE ps.page_count > 500
                  AND ps.avg_fragmentation_in_percent > 10
            ORDER BY ps.avg_fragmentation_in_percent DESC;';

            EXEC sys.sp_executesql @sql, N'@n INT', @n = @TopN;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS IndexFragmentationError;
        END CATCH
    END
    ELSE IF @IncludeIndexFragment = 1 AND @DatabaseName IS NULL
    BEGIN
        SELECT '9. INDEX FRAGMENTATION skipped - pass @DatabaseName to run this section.' AS IndexFragmentationStatus;
    END;
    ELSE IF @IncludeIndexFragment = 1 AND @ProcedureName IS NOT NULL AND @IncludeProcedureAnalysis = 1
        SELECT '9. Database-wide fragmentation skipped; section 14 scans only objects found in the procedure plans.' AS IndexFragmentationStatus;

    ----------------------------------------------------------------------
    -- 10. Tempdb health / contention
    ----------------------------------------------------------------------
    IF @IncludeTempdbHealth = 1
    BEGIN
        BEGIN TRY
            SELECT '10a. TEMPDB SPACE USAGE' AS Section;
            SELECT
                SUM(user_object_reserved_page_count) / 128.0      AS UserObjectsMB,
                SUM(internal_object_reserved_page_count) / 128.0  AS InternalObjectsMB,
                SUM(version_store_reserved_page_count) / 128.0    AS VersionStoreMB,
                SUM(unallocated_extent_page_count) / 128.0        AS FreeSpaceMB,
                SUM(mixed_extent_page_count) / 128.0              AS MixedExtentMB
            FROM tempdb.sys.dm_db_file_space_usage;

            SELECT '10b. TEMPDB NET TASK ALLOCATIONS (point-in-time; excludes completed tasks)' AS Section;
            SELECT TOP (@TopN)
                session_id,
                request_id,
                SUM(internal_objects_alloc_page_count - internal_objects_dealloc_page_count) / 128.0 AS InternalObjMB,
                SUM(user_objects_alloc_page_count - user_objects_dealloc_page_count) / 128.0 AS UserObjMB
            FROM tempdb.sys.dm_db_task_space_usage
            WHERE session_id <> @@SPID
            GROUP BY session_id, request_id
            HAVING SUM(internal_objects_alloc_page_count - internal_objects_dealloc_page_count)
                 + SUM(user_objects_alloc_page_count - user_objects_dealloc_page_count) > 0
            ORDER BY SUM(internal_objects_alloc_page_count - internal_objects_dealloc_page_count)
                   + SUM(user_objects_alloc_page_count - user_objects_dealloc_page_count) DESC;

            SELECT '10b-session. TEMPDB NET SESSION ALLOCATIONS (completed tasks; deferred deallocations may still be pending)' AS Section;
            SELECT TOP (@TopN) session_id,
                (internal_objects_alloc_page_count - internal_objects_dealloc_page_count) / 128.0 AS InternalObjMB,
                (user_objects_alloc_page_count - user_objects_dealloc_page_count) / 128.0 AS UserObjMB
            FROM tempdb.sys.dm_db_session_space_usage
            WHERE session_id <> @@SPID
            ORDER BY (internal_objects_alloc_page_count - internal_objects_dealloc_page_count)
                   + (user_objects_alloc_page_count - user_objects_dealloc_page_count) DESC;

            SELECT '10c. TEMPDB PAGELATCH WAITS (not all tempdb pages are allocation pages)' AS Section;
            SELECT
                wt.session_id,
                wt.wait_duration_ms,
                wt.wait_type,
                wt.resource_description,
                er.status,
                er.command,
                est.text AS sql_text
            FROM sys.dm_os_waiting_tasks wt
                LEFT JOIN sys.dm_os_tasks task_info ON wt.waiting_task_address = task_info.task_address
                LEFT JOIN sys.dm_exec_requests er ON task_info.session_id = er.session_id AND task_info.request_id = er.request_id
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

            IF @IncludeBufferPoolScan = 1
            BEGIN
                SELECT '11c. BUFFER POOL USAGE BY DATABASE (opt-in scan)' AS Section;
                SELECT TOP (@TopN)
                    DB_NAME(database_id) AS database_name,
                    COUNT_BIG(*) / 128.0 AS BufferedMB
                FROM sys.dm_os_buffer_descriptors
                GROUP BY database_id
                ORDER BY BufferedMB DESC;
            END;

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
            WHERE session_id <> @@SPID
            ORDER BY CASE WHEN grant_time IS NULL THEN 0 ELSE 1 END, requested_memory_kb DESC;

            SELECT '11e. RESOURCE SEMAPHORES (point-in-time)' AS Section;
            SELECT pool_id, resource_semaphore_id, available_memory_kb, granted_memory_kb,
                   used_memory_kb, grantee_count, waiter_count, timeout_error_count
            FROM sys.dm_exec_query_resource_semaphores;
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
            SELECT '12. IO BY FILE (latency from observed completed IO; zero operations = NULL latency)' AS Section,
                   CASE WHEN @Sample = 1 THEN 'INTERVAL' ELSE 'CUMULATIVE since file counters initialized' END AS CounterMode;
            ;WITH Compared AS
            (
                SELECT e.*,
                    CASE
                        WHEN @Sample = 0 THEN 'CUMULATIVE'
                        WHEN @IOSeconds IS NULL THEN 'CAPTURE_UNAVAILABLE'
                        WHEN b.database_id IS NULL THEN 'NO_BASELINE'
                        WHEN e.file_handle <> b.file_handle OR e.file_handle IS NULL OR b.file_handle IS NULL
                          OR e.file_guid <> b.file_guid OR e.physical_name <> b.physical_name
                          OR (e.file_guid IS NULL AND b.file_guid IS NOT NULL)
                          OR (e.file_guid IS NOT NULL AND b.file_guid IS NULL) THEN 'FILE_IDENTITY_CHANGED'
                        WHEN e.num_of_reads < b.num_of_reads OR e.num_of_writes < b.num_of_writes
                          OR e.num_of_bytes_read < b.num_of_bytes_read OR e.num_of_bytes_written < b.num_of_bytes_written
                          OR e.io_stall_read_ms < b.io_stall_read_ms OR e.io_stall_write_ms < b.io_stall_write_ms THEN 'COUNTER_DECREASE'
                        ELSE 'VALID_INTERVAL'
                    END AS sample_status,
                    e.num_of_reads - CASE WHEN @Sample = 1 THEN b.num_of_reads ELSE 0 END AS reads,
                    e.num_of_writes - CASE WHEN @Sample = 1 THEN b.num_of_writes ELSE 0 END AS writes,
                    e.num_of_bytes_read - CASE WHEN @Sample = 1 THEN b.num_of_bytes_read ELSE 0 END AS read_bytes,
                    e.num_of_bytes_written - CASE WHEN @Sample = 1 THEN b.num_of_bytes_written ELSE 0 END AS write_bytes,
                    e.io_stall_read_ms - CASE WHEN @Sample = 1 THEN b.io_stall_read_ms ELSE 0 END AS read_stall_ms,
                    e.io_stall_write_ms - CASE WHEN @Sample = 1 THEN b.io_stall_write_ms ELSE 0 END AS write_stall_ms
                FROM #IOSamples e
                LEFT JOIN #IOSamples b ON b.sample_no = 1 AND b.database_id = e.database_id AND b.file_id = e.file_id
                WHERE e.sample_no = 2
            )
            SELECT database_id, file_id, database_name, type_desc, physical_name, sample_status, size_on_disk_bytes,
                   CASE WHEN sample_status IN ('CUMULATIVE', 'VALID_INTERVAL') THEN reads END AS num_of_reads,
                   CASE WHEN sample_status IN ('CUMULATIVE', 'VALID_INTERVAL') THEN writes END AS num_of_writes,
                   CASE WHEN sample_status IN ('CUMULATIVE', 'VALID_INTERVAL') THEN read_bytes END AS num_of_bytes_read,
                   CASE WHEN sample_status IN ('CUMULATIVE', 'VALID_INTERVAL') THEN write_bytes END AS num_of_bytes_written,
                   CASE WHEN sample_status IN ('CUMULATIVE', 'VALID_INTERVAL') THEN read_stall_ms END AS io_stall_read_ms,
                   CASE WHEN sample_status IN ('CUMULATIVE', 'VALID_INTERVAL') THEN write_stall_ms END AS io_stall_write_ms
            INTO #EffectiveIO
            FROM Compared;

            SELECT TOP (@TopN) database_id, file_id, database_name, type_desc, physical_name, sample_status,
                @IOSeconds AS sample_seconds,
                num_of_reads, num_of_writes, num_of_bytes_read, num_of_bytes_written,
                io_stall_read_ms, io_stall_write_ms,
                CONVERT(DECIMAL(38,0), io_stall_read_ms) + io_stall_write_ms AS io_stall_total_ms,
                CAST(1.0 * io_stall_read_ms / NULLIF(num_of_reads, 0) AS DECIMAL(19,3)) AS avg_read_stall_ms,
                CAST(1.0 * io_stall_write_ms / NULLIF(num_of_writes, 0) AS DECIMAL(19,3)) AS avg_write_stall_ms,
                CAST(num_of_reads / NULLIF(@IOSeconds, 0) AS DECIMAL(19,3)) AS read_iops,
                CAST(num_of_writes / NULLIF(@IOSeconds, 0) AS DECIMAL(19,3)) AS write_iops,
                CAST(num_of_bytes_read / 1048576.0 / NULLIF(@IOSeconds, 0) AS DECIMAL(19,3)) AS read_MB_per_sec,
                CAST(num_of_bytes_written / 1048576.0 / NULLIF(@IOSeconds, 0) AS DECIMAL(19,3)) AS write_MB_per_sec,
                CAST(num_of_bytes_read / 1024.0 / NULLIF(num_of_reads, 0) AS DECIMAL(19,3)) AS avg_read_KB,
                CAST(num_of_bytes_written / 1024.0 / NULLIF(num_of_writes, 0) AS DECIMAL(19,3)) AS avg_write_KB,
                size_on_disk_bytes / 1048576.0 AS size_on_disk_mb
            FROM #EffectiveIO
            ORDER BY io_stall_total_ms DESC, database_id, file_id;

            IF @Sample = 1
            BEGIN
                SELECT '12-status. Invalid/new/disappeared files (uncapped; derived metrics withheld)' AS Section;
                SELECT database_id, file_id, database_name, physical_name, sample_status
                FROM #EffectiveIO WHERE sample_status <> 'VALID_INTERVAL'
                UNION ALL
                SELECT b.database_id, b.file_id, b.database_name, b.physical_name,
                    CASE WHEN @IOSeconds IS NULL THEN 'CAPTURE_UNAVAILABLE' ELSE 'MISSING_AT_END' END
                FROM #IOSamples b
                WHERE b.sample_no = 1 AND NOT EXISTS
                    (SELECT 1 FROM #IOSamples e WHERE e.sample_no = 2 AND e.database_id = b.database_id AND e.file_id = b.file_id);
                IF @IOSeconds IS NULL
                    SELECT 'IO interval unavailable: one capture failed or elapsed time is invalid. No cumulative fallback.' AS Warning;
            END;
        END TRY
        BEGIN CATCH
            SELECT ERROR_MESSAGE() AS IOStatsError;
        END CATCH
    END;

    IF @IncludePerformanceCounters = 1
    BEGIN
        SELECT '12b. SELECTED PERFORMANCE COUNTERS (raw rate-counter values are cumulative, not already per second)' AS Section;
        ;WITH Compared AS
        (
            SELECT e.object_name, e.counter_name, e.instance_name, e.cntr_type, e.cntr_value,
                e.cntr_value - b.cntr_value AS delta_value,
                CASE WHEN @Sample = 0 THEN 'CUMULATIVE'
                     WHEN @CounterSeconds IS NULL THEN 'CAPTURE_UNAVAILABLE'
                     WHEN b.counter_name IS NULL THEN 'NO_BASELINE'
                     WHEN e.cntr_type <> b.cntr_type OR e.cntr_value < b.cntr_value THEN 'COUNTER_RESET_OR_TYPE_CHANGE'
                     ELSE 'VALID_INTERVAL' END AS sample_status
            FROM #CounterSamples e
            LEFT JOIN #CounterSamples b ON b.sample_no = 1 AND b.object_name = e.object_name
                AND b.counter_name = e.counter_name AND b.instance_name = e.instance_name
            WHERE e.sample_no = 2
        )
        SELECT object_name, counter_name, instance_name, cntr_type, sample_status,
            cntr_value AS cumulative_value_at_end,
            CASE WHEN sample_status = 'VALID_INTERVAL' THEN delta_value END AS interval_value,
            CASE WHEN sample_status = 'VALID_INTERVAL' THEN CAST(delta_value / NULLIF(@CounterSeconds, 0) AS DECIMAL(19,3)) END AS value_per_second,
            @CounterSeconds AS sample_seconds
        FROM Compared ORDER BY object_name, instance_name, counter_name;
        IF NOT EXISTS (SELECT 1 FROM #CounterSamples WHERE sample_no = 2)
            SELECT 'No selected performance counters available; check capture errors and performance-counter availability.' AS Warning;
        IF @Sample = 1
        BEGIN
            SELECT b.object_name, b.counter_name, b.instance_name,
                CASE WHEN @CounterSeconds IS NULL THEN 'CAPTURE_UNAVAILABLE' ELSE 'MISSING_AT_END' END AS sample_status
            FROM #CounterSamples b
            WHERE b.sample_no = 1 AND NOT EXISTS
                (SELECT 1 FROM #CounterSamples e WHERE e.sample_no = 2 AND e.object_name = b.object_name
                 AND e.counter_name = b.counter_name AND e.instance_name = b.instance_name);
            IF @CounterSeconds IS NULL
                SELECT 'Performance-counter interval unavailable: capture failed or elapsed time invalid. No cumulative fallback.' AS Warning;
        END;
    END;

    ----------------------------------------------------------------------
    -- 13. Query Store (only when @DatabaseName is passed and QS is ON there)
    ----------------------------------------------------------------------
    IF @IncludeQueryStore = 1 AND @DatabaseName IS NOT NULL
    BEGIN
        BEGIN TRY
            SET @sql = N'SELECT @state = actual_state_desc FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_query_store_options;';
            EXEC sp_executesql @sql, N'@state NVARCHAR(60) OUTPUT', @state = @QSState OUTPUT;

            IF @QSState IS NULL OR @QSState NOT IN ('READ_WRITE', 'READ_ONLY')
            BEGIN
                SELECT 'Query Store not readable for ' + @DatabaseName AS QueryStoreStatus, @QSState AS ActualState;
            END
            ELSE
            BEGIN
                SET @QSStartTimeStr = CONVERT(NVARCHAR(30), @QSStartTime, 121);

                SELECT '13. QUERY STORE - TOP RESOURCE CONSUMING QUERIES (' + @DatabaseName + ', state=' + @QSState
                       + ', since ' + @QSStartTimeStr + ' UTC)' AS Section;
                SELECT @QSStartTime AS LookbackStartUTC, @QSEndTime AS LookbackEndUTC,
                    'Overlapping aggregation intervals included in full; this is not an exact execution-level time filter.' AS TimeBoundaryNote;

                SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
                SELECT TOP (@n)
                    q.query_id,
                    rs.execution_type_desc,
                    OBJECT_SCHEMA_NAME(q.object_id) + N''.'' + OBJECT_NAME(q.object_id)  AS object_name,
                    qt.query_sql_text,
                    SUM(rs.count_executions)                                             AS total_executions,
                    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS DECIMAL(18,2))  AS total_cpu_ms,
                    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS DECIMAL(28,2)) AS avg_cpu_ms,
                    CAST(SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS DECIMAL(18,2))  AS total_duration_ms,
                    CAST(SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS DECIMAL(28,2)) AS avg_duration_ms,
                    CAST(SUM(rs.avg_logical_io_reads * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS DECIMAL(28,2)) AS avg_logical_reads,
                    CAST(SUM(rs.avg_rowcount * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS DECIMAL(28,2)) AS avg_row_count,
                    MAX(rs.last_execution_time)                                           AS last_execution_time
                FROM sys.query_store_query q
                    INNER JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
                    INNER JOIN sys.query_store_plan p ON q.query_id = p.query_id
                    INNER JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
                    INNER JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
                WHERE rsi.end_time > @since AND rsi.start_time < @until
                GROUP BY q.query_id, q.object_id, qt.query_sql_text, rs.execution_type_desc
                ORDER BY total_cpu_ms DESC;';

                EXEC sys.sp_executesql @sql, N'@n INT, @since DATETIMEOFFSET(7), @until DATETIMEOFFSET(7)',
                    @n = @TopN, @since = @QSStartTime, @until = @QSEndTime;
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
                DECLARE @ProcSchema SYSNAME = COALESCE(PARSENAME(@ProcedureName, 2), N'dbo');
                DECLARE @ProcBase SYSNAME = PARSENAME(@ProcedureName, 1);
                IF @ProcBase IS NULL OR PARSENAME(@ProcedureName, 3) IS NOT NULL OR PARSENAME(@ProcedureName, 4) IS NOT NULL
                    THROW 50000, '@ProcedureName must be a valid one- or two-part name.', 1;
                SET @ProcFullName = QUOTENAME(@DatabaseName) + N'.' + QUOTENAME(@ProcSchema) + N'.' + QUOTENAME(@ProcBase);
                SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
                    SELECT @id = p.object_id FROM sys.procedures p
                    JOIN sys.schemas s ON s.schema_id = p.schema_id
                    WHERE p.name = @name AND s.name = @schema;';
                EXEC sys.sp_executesql @sql, N'@name SYSNAME, @schema SYSNAME, @id INT OUTPUT',
                    @name = @ProcBase, @schema = @ProcSchema, @id = @ProcObjectId OUTPUT;

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
                            'Cache entries are not distinct plan shapes; SET options and execution contexts can create multiple entries.' AS interpretation_note
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
                            qs.query_hash,
                            qs.query_plan_hash,
                            qs.plan_generation_num,
                            SUBSTRING(st.text,
                                (qs.statement_start_offset / 2) + 1,
                                ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END - qs.statement_start_offset) / 2) + 1
                            )                                                                    AS statement_text,
                            qs.execution_count,
                            CAST(qs.total_worker_time / 1000.0 AS DECIMAL(18, 2))                AS total_cpu_ms,
                            CAST(qs.total_worker_time / 1000.0 / NULLIF(qs.execution_count, 0) AS DECIMAL(18, 2)) AS avg_cpu_ms,
                            CAST(qs.total_elapsed_time / 1000.0 AS DECIMAL(18, 2))                AS total_duration_ms,
                            qs.total_logical_reads,
                            1.0 * qs.total_logical_reads / NULLIF(qs.execution_count, 0)           AS avg_logical_reads,
                            qs.last_execution_time
                        FROM sys.dm_exec_procedure_stats ps
                            INNER JOIN sys.dm_exec_query_stats qs ON qs.plan_handle = ps.plan_handle
                            OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
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

                            IF @ProcQSState IS NULL OR @ProcQSState NOT IN ('READ_WRITE', 'READ_ONLY')
                            BEGIN
                                SELECT '14c. Query Store not readable in ' + @DatabaseName AS QueryStoreStatus, @ProcQSState AS ActualState;
                            END
                            ELSE
                            BEGIN
                                SELECT '14c. QUERY STORE HISTORY FOR THIS PROCEDURE (' + @DatabaseName + ', last ' + @QueryStoreTimeRange + ')' AS Section;

                                SET @sql = N'USE ' + QUOTENAME(@DatabaseName) + N';
                                SELECT TOP (@n)
                                    q.query_id,
                                    p.plan_id,
                                    rs.execution_type_desc,
                                    qt.query_sql_text,
                                    SUM(rs.count_executions)                                             AS total_executions,
                                    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS DECIMAL(18,2))  AS total_cpu_ms,
                                    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS DECIMAL(28,2)) AS avg_cpu_ms,
                                    CAST(MAX(rs.max_duration) / 1000.0 AS DECIMAL(18,2))                  AS worst_duration_ms,
                                    CAST(SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS DECIMAL(28,2)) AS avg_duration_ms,
                                    CAST(MAX(rs.max_duration) / NULLIF(SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0), 0) AS DECIMAL(28,2)) AS worst_vs_avg_duration_ratio,
                                    CAST(SUM(rs.avg_logical_io_reads * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS DECIMAL(28,2)) AS avg_logical_reads,
                                    MAX(rs.last_execution_time)                                           AS last_execution_time
                                FROM sys.query_store_query q
                                    INNER JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
                                    INNER JOIN sys.query_store_plan p ON q.query_id = p.query_id
                                    INNER JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
                                    INNER JOIN sys.query_store_runtime_stats_interval rsi ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
                                WHERE q.object_id = @object_id AND rsi.end_time > @since AND rsi.start_time < @until
                                GROUP BY q.query_id, p.plan_id, qt.query_sql_text, rs.execution_type_desc
                                ORDER BY total_cpu_ms DESC;';
                                EXEC sys.sp_executesql @sql,
                                    N'@n INT, @object_id INT, @since DATETIMEOFFSET(7), @until DATETIMEOFFSET(7)',
                                    @n = @TopN, @object_id = @ProcObjectId, @since = @QSStartTime, @until = @QSEndTime;

                                SELECT '14c-note. Runtime variability is a lead, not proof of parameter sensitivity: also check blocking, IO, spills, grants, and workload/input size. Intervals overlapping the lookback are included in full.' AS Note;
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
                        SELECT ps.plan_handle, qp.query_plan
                        FROM (SELECT DISTINCT plan_handle FROM sys.dm_exec_procedure_stats
                              WHERE object_id = @ProcObjectId AND database_id = @ProcDbId) ps
                            CROSS APPLY sys.dm_exec_query_plan(ps.plan_handle) qp
                        WHERE qp.query_plan IS NOT NULL;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS PlanFetchError;
                    END CATCH;

                    BEGIN TRY
                        IF EXISTS (SELECT 1 FROM @ProcPlans)
                        BEGIN
                            SELECT '14d. MISSING INDEX CANDIDATES FROM CACHED PLANS (review key order, overlap, width and write overhead before creating)' AS Section;

                            ;WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
                            SELECT
                                pp.plan_handle,
                                mig.value('(@Impact)[1]', 'float')                        AS impact_pct,
                                mi.value('(@Database)[1]', 'nvarchar(258)')               AS database_name,
                                mi.value('(@Schema)[1]', 'nvarchar(258)')                 AS schema_name,
                                mi.value('(@Table)[1]', 'nvarchar(258)')                  AS table_name,
                                eq.cols   AS equality_columns,
                                ineq.cols AS inequality_columns,
                                inc.cols  AS included_columns
                            FROM @ProcPlans pp
                                CROSS APPLY pp.query_plan.nodes('//MissingIndexGroup') AS t1(mig)
                                CROSS APPLY mig.nodes('MissingIndex') AS t2(mi)
                                OUTER APPLY (
                                    SELECT STUFF((SELECT ',' + c.value('(@Name)[1]', 'nvarchar(258)')
                                                  FROM mi.nodes('ColumnGroup[@Usage="EQUALITY"]/Column') AS cc(c)
                                                  FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 1, '') AS cols) eq
                                OUTER APPLY (
                                    SELECT STUFF((SELECT ',' + c.value('(@Name)[1]', 'nvarchar(258)')
                                                  FROM mi.nodes('ColumnGroup[@Usage="INEQUALITY"]/Column') AS cc(c)
                                                  FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 1, '') AS cols) ineq
                                OUTER APPLY (
                                    SELECT STUFF((SELECT ',' + c.value('(@Name)[1]', 'nvarchar(258)')
                                                  FROM mi.nodes('ColumnGroup[@Usage="INCLUDE"]/Column') AS cc(c)
                                                  FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 1, '') AS cols) inc
                            ORDER BY impact_pct DESC;
                        END
                        ELSE
                        BEGIN
                            SELECT '14d. MISSING INDEX HINTS' AS Section,
                                   'No cached plan XML available. It may be evicted, encrypted, recompiled, or inaccessible. Do not execute an unfamiliar procedure merely to populate the cache.' AS Message;
                        END;
                    END TRY
                    BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS MissingIndexXmlError;
                    END CATCH;

                    BEGIN TRY
                        ;WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
                        INSERT INTO @ImpactedTables (database_name, schema_name, table_name, index_name, physical_op)
                        SELECT DISTINCT
                            PARSENAME(obj.value('(@Database)[1]', 'nvarchar(258)'), 1),
                            PARSENAME(obj.value('(@Schema)[1]', 'nvarchar(258)'), 1),
                            PARSENAME(obj.value('(@Table)[1]', 'nvarchar(258)'), 1),
                            PARSENAME(obj.value('(@Index)[1]', 'nvarchar(258)'), 1),
                            obj.value('(../../@PhysicalOp)[1]', 'nvarchar(60)')
                        FROM @ProcPlans pp
                            CROSS APPLY pp.query_plan.nodes('//Object') AS t(obj)
                        WHERE obj.exist('@Table') = 1;

                        SELECT '14e. OBJECT/INDEX/OPERATOR EVIDENCE FROM AVAILABLE PLANS (not exhaustive: dynamic SQL, nested modules and uncached paths may be absent)' AS Section;
                        SELECT DISTINCT
                            database_name, schema_name, table_name, index_name, physical_op
                        FROM @ImpactedTables
                        ORDER BY database_name, schema_name, table_name, index_name, physical_op;
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
                                migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS improvement_measure
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
                                SELECT @TableFilter = STUFF((
                                    SELECT N' UNION ALL SELECT N' + QUOTENAME(schema_name, '''') + N' AS sch, N' + QUOTENAME(table_name, '''') + N' AS tbl'
                                    FROM (SELECT DISTINCT schema_name, table_name FROM @ImpactedTables
                                          WHERE database_name = @DbCursorName AND schema_name IS NOT NULL AND table_name IS NOT NULL) x
                                    ORDER BY schema_name, table_name
                                    FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 11, N'');

                                IF @TableFilter IS NOT NULL
                                BEGIN
                                    SET @sql = N'
USE ' + QUOTENAME(@DbCursorName) + N';

SELECT ''14g. INDEX INVENTORY / CUMULATIVE USAGE (NULL usage means unavailable, not proof of an unused index)'' AS Section, DB_NAME() AS database_name;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    i.name AS index_name, i.index_id, i.type_desc,
    i.is_unique, i.is_primary_key, i.is_unique_constraint, i.fill_factor,
    i.is_disabled, i.has_filter, i.filter_definition,
    STUFF((SELECT '','' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN '' DESC'' ELSE '''' END
           FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0
           ORDER BY ic.key_ordinal FOR XML PATH(''''), TYPE).value(''.'', ''nvarchar(max)''), 1, 1, '''') AS key_columns,
    STUFF((SELECT '','' + QUOTENAME(c.name)
           FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
           ORDER BY ic.index_column_id FOR XML PATH(''''), TYPE).value(''.'', ''nvarchar(max)''), 1, 1, '''') AS included_columns,
    ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates,
    ius.last_user_seek, ius.last_user_scan, ius.last_user_update
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.indexes i ON i.object_id = tbl.object_id
    LEFT JOIN sys.dm_db_index_usage_stats ius ON ius.database_id = DB_ID() AND ius.object_id = i.object_id AND ius.index_id = i.index_id
ORDER BY tgt.sch, tgt.tbl, i.index_id;

IF @fragment = 1
BEGIN
    SELECT ''14g-fragment. OPT-IN PHYSICAL STATS (per partition/allocation unit; LIMITED mode, not page density)'' AS Section, DB_NAME() AS database_name;
    SELECT tgt.sch AS schema_name, tgt.tbl AS table_name, i.name AS index_name,
        ips.index_id, ips.partition_number, ips.alloc_unit_type_desc,
        ips.avg_fragmentation_in_percent, ips.page_count, ips.forwarded_record_count
    FROM (' + @TableFilter + N') tgt
        JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
        CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), tbl.object_id, NULL, NULL, ''LIMITED'') ips
        JOIN sys.indexes i ON i.object_id = tbl.object_id AND i.index_id = ips.index_id
    ORDER BY tgt.sch, tgt.tbl, ips.index_id, ips.partition_number, ips.alloc_unit_type_desc;
END;

SELECT ''14h. APPROXIMATE ROW COUNTS / OBJECT DDL DATES (modify_date is not last data modification)'' AS Section, DB_NAME() AS database_name;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    SUM(p.rows) AS row_count_approx,
    tbl.create_date, tbl.modify_date
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.partitions p ON p.object_id = tbl.object_id AND p.index_id IN (0, 1)
GROUP BY tgt.sch, tgt.tbl, tbl.create_date, tbl.modify_date;

SELECT ''14i. FOREIGN KEYS TOUCHING IMPACTED TABLES'' AS Section, DB_NAME() AS database_name;
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) + ''.'' + OBJECT_NAME(fk.parent_object_id)         AS parent_table,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) + ''.'' + OBJECT_NAME(fk.referenced_object_id)  AS referenced_table,
    fk.name AS fk_name, fk.is_disabled, fk.is_not_trusted
FROM sys.foreign_keys fk
WHERE fk.parent_object_id IN (SELECT tbl.object_id FROM (' + @TableFilter + N') tgt JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch)
   OR fk.referenced_object_id IN (SELECT tbl.object_id FROM (' + @TableFilter + N') tgt JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch);

SELECT ''14j. TRIGGERS ON IMPACTED TABLES'' AS Section, DB_NAME() AS database_name;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    tr.name AS trigger_name, tr.is_disabled, tr.is_instead_of_trigger
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.triggers tr ON tr.parent_id = tbl.object_id;

SELECT ''14k. STATISTICS FRESHNESS (modification_counter is not an automatic UPDATE STATISTICS prescription)'' AS Section, DB_NAME() AS database_name;
SELECT
    tgt.sch AS schema_name, tgt.tbl AS table_name,
    s.name AS stats_name,
    s.auto_created, s.user_created, s.no_recompute, s.has_filter, s.filter_definition,
    sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter,
    100.0 * sp.rows_sampled / NULLIF(sp.rows, 0) AS sampled_pct
FROM (' + @TableFilter + N') tgt
    JOIN sys.tables tbl ON tbl.name = tgt.tbl AND SCHEMA_NAME(tbl.schema_id) = tgt.sch
    JOIN sys.stats s ON s.object_id = tbl.object_id
    OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
ORDER BY sp.modification_counter DESC;
';
                                    EXEC sys.sp_executesql @sql, N'@fragment BIT', @fragment = @IncludeIndexFragment;
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

    SELECT '=== Diagnostic collection ended; inspect section errors, warnings and sample_status before interpreting results ===' AS RunInfo;
END;
GO
