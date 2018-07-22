DECLARE @jobfailedcheckmin INT= 60;
IF OBJECT_ID('tempdb..#tblHealthCheck') IS NOT NULL
    DROP TABLE #tblHealthCheck;
CREATE TABLE #tblHealthCheck
(id   INT IDENTITY(1, 1) PRIMARY KEY,
 Col1 VARCHAR(MAX),
 Col2 VARCHAR(MAX),
 Col3 VARCHAR(MAX)
);
IF EXISTS
(
    SELECT 1
    FROM sys.databases
    WHERE name LIKE 'tempdb'
          AND create_date > DATEADD(DD, -@jobfailedcheckmin, GETDATE())
)
    BEGIN
        INSERT INTO #tblHealthCheck
               SELECT '  ',
                      '  ',
                      '  ';
        INSERT INTO #tblHealthCheck
               SELECT '',
                      '',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT 'OSBootTime',
                      'SQLStartTime',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT '',
                      '',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT '  ',
                      '  ',
                      '  ';
        INSERT INTO #tblHealthCheck
               SELECT
               (
                   SELECT CONVERT(VARCHAR(20), DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE()), 120)+' --> '+CONVERT(VARCHAR(10), DATEDIFF(HOUR, DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE()), GETDATE()))+' hour(s) ago'
                   FROM sys.[dm_os_sys_info]
               ) OSRebootTime,
               (
                   SELECT CONVERT(VARCHAR(20), create_date, 120)+' --> '+CONVERT(VARCHAR(10), DATEDIFF(HOUR, create_date, GETDATE()))+' hour(s) ago'
                   FROM sys.databases
                   WHERE name LIKE 'tempdb'
               ) SQLStartTime,
               '';
END;
INSERT INTO #tblHealthCheck
       SELECT '  ',
              '  ',
              '  ';
INSERT INTO #tblHealthCheck
       SELECT '',
              '',
              '';
INSERT INTO #tblHealthCheck
       SELECT 'DatabaseName',
              'LastFullBackup',
              'StatusDesc';
INSERT INTO #tblHealthCheck
       SELECT '',
              '',
              '';
INSERT INTO #tblHealthCheck
       SELECT '  ',
              '  ',
              '  ';
INSERT INTO #tblHealthCheck
       SELECT name AS [database_name],
              [D],
              CASE
		    
/* These conditions below will cause a CRITICAL status */

                  WHEN [D] IS NULL
                  THEN 'No FULL backups'															-- if last_full_backup is null then critical
                  WHEN [D] < DATEADD(DD, -1, CURRENT_TIMESTAMP)
                       AND [I] IS NULL
                  THEN 'FULL backup > 1 day; no DIFF backups'			-- if last_full_backup is more than 2 days old and last_differential_backup is null then critical
                  WHEN [D] < DATEADD(DD, -7, CURRENT_TIMESTAMP)
                       AND [I] < DATEADD(DD, -2, CURRENT_TIMESTAMP)
                  THEN 'FULL backup > 7 day; DIFF backup > 2 days'	-- if last_full_backup is more than 7 days old and last_differential_backup more than 2 days old then critical
                  WHEN recovery_model_desc <> 'SIMPLE'
                       AND name <> 'model'
                       AND [L] IS NULL
                  THEN 'No LOG backups'	-- if recovery_model_desc is SIMPLE and last_tlog_backup is null then critical
                  WHEN recovery_model_desc <> 'SIMPLE'
                       AND name <> 'model'
                       AND [L] < DATEADD(HH, -6, CURRENT_TIMESTAMP)
                  THEN 'LOG backup > 6 hours'		-- if last_tlog_backup is more than 6 hours old then critical
		    --/* These conditions below will cause a WARNING status */
                  WHEN [D] < DATEADD(DD, -2, CURRENT_TIMESTAMP)
                       AND [I] < DATEADD(DD, -1, CURRENT_TIMESTAMP)
                  THEN 'FULL backup > 24 hours; DIFF backup > 1 day'		-- if last_full_backup is more than 1 day old and last_differential_backup is greater than 1 days old then warning
                  WHEN recovery_model_desc <> 'SIMPLE'
                       AND name <> 'model'
                       AND [L] < DATEADD(HH, -3, CURRENT_TIMESTAMP)
                  THEN 'LOG backup > 3 hours'		-- if last_tlog_backup is more than 3 hours old then warning
/* Everything else will return a GOOD status */

                  ELSE 'No issues'
              END AS status_desc
       FROM
       (
           SELECT d.name,
                  d.recovery_model_desc,
                  bs.type,
                  MAX(bs.backup_finish_date) AS backup_finish_date
           FROM master.sys.databases d
                LEFT JOIN msdb.dbo.backupset bs ON d.name = bs.database_name
           WHERE(bs.type IN('D', 'I', 'L')
           OR bs.type IS NULL)
                AND d.database_id <> 2				-- exclude tempdb
                AND d.source_database_id IS NULL	-- exclude snapshot databases
                AND d.state NOT IN(1, 6, 10)			-- exclude offline, restoring, or secondary databases
           AND d.is_in_standby = 0				-- exclude log shipping secondary databases
           GROUP BY d.name,
                    d.recovery_model_desc,
                    bs.type
       ) AS SourceTable PIVOT(MAX(backup_finish_date) FOR type IN([D],
                                                                  [I],
                                                                  [L])) AS PivotTable
       ORDER BY database_name;
IF EXISTS
(
    SELECT DISTINCT
           1
    FROM msdb.dbo.sysjobs j
         INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    WHERE j.enabled = 1
          AND msdb.dbo.agent_datetime(run_date, run_time) > DATEADD(MINUTE, -@jobfailedcheckmin, GETDATE())  --Only Enabled Jobs
)
    BEGIN
        INSERT INTO #tblHealthCheck
               SELECT '  ',
                      '  ',
                      '  ';
        INSERT INTO #tblHealthCheck
               SELECT '',
                      '',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT 'JobName',
                      'RunDate/Time',
                      'Status';
        INSERT INTO #tblHealthCheck
               SELECT '',
                      '',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT '  ',
                      '  ',
                      '  ';
        INSERT INTO #tblHealthCheck
               SELECT DISTINCT
                      j.name AS 'JobName',
                      msdb.dbo.agent_datetime(run_date, run_time) AS 'RunDateTime',
                      CASE
                          WHEN h.run_status = 0
                          THEN 'Failed'
                          WHEN h.run_status = 1
                          THEN 'Succeeded'
                          WHEN h.run_status = 2
                          THEN 'Retry'
                          WHEN h.run_status = 3
                          THEN 'Cancelled'
                          ELSE 'Unknown'
                      END Status
               FROM msdb.dbo.sysjobs j
                    INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
               WHERE j.enabled = 1
                     AND msdb.dbo.agent_datetime(run_date, run_time) > DATEADD(MINUTE, -@jobfailedcheckmin, GETDATE())  --Only Enabled Jobs
               ORDER BY 3,
                        RunDateTime DESC;
END;
IF OBJECT_ID('tempdb..#Errorlog') IS NOT NULL
    DROP TABLE #Errorlog;
CREATE TABLE #Errorlog
(LogDate     DATETIME,
 ProcessInfo VARCHAR(20),
 Text        VARCHAR(MAX)
);
INSERT INTO #Errorlog
EXEC sp_readerrorlog
     0;
INSERT INTO #Errorlog
EXEC sp_readerrorlog
     1;
INSERT INTO #Errorlog
EXEC sp_readerrorlog
     2;
--INSERT INTO #Errorlog
--EXEC sp_readerrorlog
--     3;
--INSERT INTO #Errorlog
--EXEC sp_readerrorlog
--     4;
--INSERT INTO #Errorlog
--EXEC sp_readerrorlog
--     5;
DELETE FROM #Errorlog
WHERE LogDate < DATEADD(MINUTE, -@jobfailedcheckmin, GETDATE());
DELETE FROM #Errorlog
WHERE Text LIKE 'Log was backed up. Database:%'
      OR Text LIKE 'BACKUP DATABASE successfully%'
      OR Text LIKE '%This is an informational message%'
      OR Text LIKE 'Starting up database%'
      OR Text LIKE 'Clearing tempdb database.'
      OR Text LIKE '%start%'
      OR Text LIKE '%succe%'
      OR Text LIKE '%Microsoft Corporation%'
      OR Text LIKE '%start%'
      OR Text LIKE '%listen%'
      OR Text LIKE '%accept%'
      OR Text LIKE 'All rights reserved.';
IF EXISTS
(
    SELECT 1
    FROM #Errorlog
)
    BEGIN
        INSERT INTO #tblHealthCheck
               SELECT '  ',
                      '  ',
                      '  ';
        INSERT INTO #tblHealthCheck
               SELECT '',
                      '',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT 'Date/Time',
                      'ProcessInfo',
                      'Text';
        INSERT INTO #tblHealthCheck
               SELECT '',
                      '',
                      '';
        INSERT INTO #tblHealthCheck
               SELECT '  ',
                      '  ',
                      '  ';
        INSERT INTO #tblHealthCheck
               SELECT *
               FROM #Errorlog
               ORDER BY LogDate DESC;
END;
SELECT Col1,
       Col2,
       Col3
FROM #tblHealthCheck;
