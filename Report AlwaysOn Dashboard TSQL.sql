USE tempdb;
GO

IF OBJECT_ID('dbo.usp_GetAGHealthStatus', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetAGHealthStatus;
GO

CREATE PROCEDURE dbo.usp_GetAGHealthStatus
(
    @AGName NVARCHAR(128) = NULL,
    @ReplicaServerName NVARCHAR(128) = NULL,
    @DBName NVARCHAR(128) = NULL,
    @QueueSizeThresholdMB INT = 1024,
    @LatencyThresholdMinutes INT = 10,
    @ReportMode TINYINT = 1
)
/*
-- =============================================
-- Author:      (Enhanced by AI)
-- Create date: 2023-10-27
-- Description: Monitors the health of Always On Availability Groups.
--
-- V3 Changes:
--   - CORRECTED TIME ZONE ISSUE: Replaced GETUTCDATE() with GETDATE().
--     DMV timestamps are in the server's local time, so GETDATE() must be
--     used for an accurate apples-to-apples comparison.
--
-- V2 Changes:
--   - Latency is now calculated conditionally to correctly handle idle databases.
-- =============================================
*/
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @QueueSizeThresholdKB BIGINT = @QueueSizeThresholdMB * 1024;
    DECLARE @LatencyThresholdSeconds INT = @LatencyThresholdMinutes * 60;

    WITH AG_Health AS
    (
        SELECT
            ag.name AS [AGName],
            ar.replica_server_name AS [ReplicaServerName],
            DB_NAME(drs.database_id) AS [DBName],
            ar.availability_mode_desc AS [SyncMode],
            drs.is_primary_replica AS [IsPrimary],
            drs.synchronization_state_desc AS [SyncState],
            drs.synchronization_health_desc AS [SyncHealth],
            drs.is_suspended AS [IsSuspended],
            drs.suspend_reason_desc AS [SuspendReason],
            drs.log_send_queue_size,
            drs.log_send_rate,
            drs.redo_queue_size,
            drs.redo_rate,
            -- ===================================================================
            -- CORRECTED TIMEZONE LOGIC
            -- Using GETDATE() to compare against the DMV's local time columns.
            -- ===================================================================
            CASE
                WHEN drs.log_send_queue_size > 0 THEN DATEDIFF(second, drs.last_sent_time, GETDATE())
                ELSE 0
            END AS [SendLatency_Sec],
            CASE
                WHEN drs.log_send_queue_size > 0 THEN DATEDIFF(second, drs.last_hardened_time, GETDATE())
                ELSE 0
            END AS [HardenLatency_Sec],
            CASE
                WHEN drs.redo_queue_size > 0 THEN DATEDIFF(second, drs.last_redone_time, GETDATE())
                ELSE 0
            END AS [RedoLatency_Sec],
            drs.last_sent_time,
            drs.last_redone_time,
            drs.last_commit_time
        FROM
            sys.dm_hadr_database_replica_states AS drs
        INNER JOIN
            sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id AND drs.group_id = ar.group_id
        INNER JOIN
            sys.availability_groups AS ag ON ar.group_id = ag.group_id
    )
    SELECT
        @@SERVERNAME AS [LocalServerName],
        h.AGName,
        h.ReplicaServerName,
        h.DBName,
        CASE WHEN h.IsPrimary = 1 THEN 'PRIMARY' ELSE 'SECONDARY' END AS [ReplicaRole],
        h.SyncMode,
        h.SyncState,
        h.SyncHealth,
        h.IsSuspended,
        h.SuspendReason,
        CASE WHEN h.IsPrimary = 1 THEN 'N/A'
             WHEN h.log_send_queue_size > 1048576 THEN FORMAT(h.log_send_queue_size / 1048576.0, 'N1') + ' GB'
             WHEN h.log_send_queue_size > 1024 THEN FORMAT(h.log_send_queue_size / 1024.0, 'N1') + ' MB'
             ELSE FORMAT(h.log_send_queue_size, 'N0') + ' KB' END AS [LogSendQueue],
        CASE WHEN h.IsPrimary = 1 THEN 'N/A'
             WHEN h.redo_queue_size > 1048576 THEN FORMAT(h.redo_queue_size / 1048576.0, 'N1') + ' GB'
             WHEN h.redo_queue_size > 1024 THEN FORMAT(h.redo_queue_size / 1024.0, 'N1') + ' MB'
             ELSE FORMAT(h.redo_queue_size, 'N0') + ' KB' END AS [RedoQueue],
        CASE WHEN h.IsPrimary = 1 THEN 'N/A' ELSE CONVERT(varchar(8), DATEADD(second, h.log_send_queue_size / NULLIF(h.log_send_rate, 0), 0), 108) END AS [EstimatedLogClearTime_HHMMSS],
        CASE WHEN h.IsPrimary = 1 THEN 'N/A' ELSE CONVERT(varchar(8), DATEADD(second, h.redo_queue_size / NULLIF(h.redo_rate, 0), 0), 108) END AS [EstimatedRedoClearTime_HHMMSS],
        CASE WHEN h.IsPrimary = 1 THEN 'N/A' ELSE CONVERT(varchar(8), DATEADD(second, h.SendLatency_Sec, 0), 108) END AS [SendLatency_HHMMSS],
        CASE WHEN h.IsPrimary = 1 THEN 'N/A' ELSE CONVERT(varchar(8), DATEADD(second, h.HardenLatency_Sec, 0), 108) END AS [HardenLatency_HHMMSS],
        CASE WHEN h.IsPrimary = 1 THEN 'N/A' ELSE CONVERT(varchar(8), DATEADD(second, h.RedoLatency_Sec, 0), 108) END AS [RedoLatency_HHMMSS],
        h.last_sent_time,
        h.last_redone_time,
        h.last_commit_time,
        GETDATE() AS [ReportRunTime_Local] -- Renamed to reflect the use of GETDATE()
    FROM
        AG_Health h
    WHERE
        (@AGName IS NULL OR h.AGName = @AGName)
        AND (@ReplicaServerName IS NULL OR h.ReplicaServerName = @ReplicaServerName)
        AND (@DBName IS NULL OR h.DBName = @DBName)
        AND (
            @ReportMode = 1
            OR
            (
                @ReportMode = 0 AND h.IsPrimary = 0 AND (
                    h.log_send_queue_size > @QueueSizeThresholdKB
                    OR h.redo_queue_size > @QueueSizeThresholdKB
                    OR h.SendLatency_Sec > @LatencyThresholdSeconds
                    OR h.RedoLatency_Sec > @LatencyThresholdSeconds
                )
            )
        )
    ORDER BY
        h.AGName,
        h.DBName,
        h.IsPrimary DESC;
END;
GO
EXEC usp_GetAGHealthStatus
