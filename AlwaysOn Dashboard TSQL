SELECT @@SERVERNAME LocalServerName,
       ar.replica_server_name ReplicaServerName,
       adc.database_name DBName,
       ag.name AS AGName,
       drs.synchronization_state_desc SyncStateDesc,
       drs.synchronization_health_desc SyncHealthDesc,
       drs.last_sent_time LastSentTime,
       drs.last_received_time LastReceiveTime,
       drs.last_hardened_time LastHardenedTime,
       drs.last_redone_time LastRedoneTime,
       Format(drs.log_send_queue_size, '##,##0') LogSendQueueSize_KB,
       FORMAT(drs.log_send_rate, '##,##0') LogSendRate_KBperSec,
       DATEADD(SS, drs.log_send_queue_size / drs.log_send_rate, GETDATE()) EstimaedLogSendFinishTime,
       FORMAT(drs.redo_queue_size, '##,##0') RedoQueueSize_KB,
       FORMAT(drs.redo_rate, '##,##0') RedoRate_KBperSec,
       DATEADD(SS, drs.redo_queue_size / drs.redo_rate, GETDATE()) EstimatedRedoFinishTIme,
       drs.last_commit_time LastCommitTime,
       GETDATE() ReportRunTime
FROM sys.dm_hadr_database_replica_states AS drs
     INNER JOIN sys.availability_databases_cluster AS adc ON drs.group_id = adc.group_id
                                                             AND drs.group_database_id = adc.group_database_id
     INNER JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
     INNER JOIN sys.availability_replicas AS ar ON drs.group_id = ar.group_id
                                                   AND drs.replica_id = ar.replica_id
WHERE 1 = 1
      AND last_sent_lsn IS NOT NULL
      AND (log_send_queue_size > 1024
           OR redo_queue_size > 1000)
      AND (last_redone_time < DATEADD(mi, -30, GETDATE())
           OR last_sent_time < DATEADD(mi, -30, GETDATE())
           OR last_received_time < DATEADD(mi, -30, GETDATE())
           OR last_hardened_time < DATEADD(mi, -30, GETDATE())
           OR last_commit_time < DATEADD(mi, -30, GETDATE()))
ORDER BY ag.name,
         ar.replica_server_name,
         adc.database_name;
