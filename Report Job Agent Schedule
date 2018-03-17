USE msdb;
DECLARE @job_id VARCHAR(200), @sched_id VARCHAR(200), @freq_type INT, @freq_interval INT, @freq_subday_type INT, @freq_subday_interval INT, @freq_relative_interval INT, @freq_recurrence_factor INT, @active_start_date INT, @schedule VARCHAR(1000), @schedule_day VARCHAR(200), @start_time VARCHAR(10), @end_time VARCHAR(10);
IF OBJECT_ID('tempdb..#schedules') IS NOT NULL
    DROP TABLE #schedules;
IF OBJECT_ID('tempdb..#temp1') IS NOT NULL
    DROP TABLE #temp1;
CREATE TABLE #schedules
(job_id                 VARCHAR(200),
 sched_id               VARCHAR(200),
 job_name               SYSNAME,
 [status]               INT,
 scheduled              INT NULL,
 schedule               VARCHAR(1000) NULL,
 freq_type              INT NULL,
 freq_interval          INT NULL,
 freq_subday_type       INT NULL,
 freq_subday_interval   INT NULL,
 freq_relative_interval INT NULL,
 freq_recurrence_factor INT NULL,
 active_start_date      INT NULL,
 active_end_date        INT NULL,
 active_start_time      INT NULL,
 active_end_time        INT NULL,
 date_created           DATETIME NULL
);
INSERT INTO #schedules
(job_id,
 sched_id,
 job_name,
 [status],
 scheduled,
 schedule,
 freq_type,
 freq_interval,
 freq_subday_type,
 freq_subday_interval,
 freq_relative_interval,
 freq_recurrence_factor,
 active_start_date,
 active_end_date,
 active_start_time,
 active_end_time,
 date_created
)
       SELECT j.job_id,
              sched.schedule_id,
              j.name,
              j.enabled,
              sched1.enabled,
              NULL,
              sched1.freq_type,
              sched1.freq_interval,
              sched1.freq_subday_type,
              sched1.freq_subday_interval,
              sched1.freq_relative_interval,
              sched1.freq_recurrence_factor,
              sched1.active_start_date,
              sched1.active_end_date,
              sched1.active_start_time,
              sched1.active_end_time,
              j.date_created
       FROM sysjobs j
            INNER JOIN sysjobschedules s ON j.job_id = s.job_id
            INNER JOIN dbo.sysjobschedules sched ON s.schedule_id = sched.schedule_id
            INNER JOIN dbo.sysschedules sched1 ON s.schedule_id = sched1.schedule_id;
--inner join dbo.sysjobschedules sched1
--on s.schedule_id = sched.schedule_id

WHILE 1 = 1
    BEGIN
        SET @schedule = '';
        IF
(
    SELECT COUNT(*)
    FROM #schedules
    WHERE #schedules.scheduled = 1
          AND #schedules.schedule IS NULL
) = 0
            BREAK;
            ELSE
            BEGIN
                SELECT @job_id = job_id,
                       @sched_id = sched_id,
                       @freq_type = freq_type,
                       @freq_interval = freq_interval,
                       @freq_subday_type = freq_subday_type,
                       @freq_subday_interval = freq_subday_interval,
                       @freq_relative_interval = freq_relative_interval,
                       @freq_recurrence_factor = freq_recurrence_factor,
                       @active_start_date = active_start_date,
                       @start_time = CASE
                                         WHEN LEFT(active_start_time, 2) IN(22, 23)
                                              AND LEN(active_start_time) = 6
                                         THEN CONVERT(VARCHAR(2), LEFT(active_start_time, 2) - 12)+':'+SUBSTRING(CAST(active_start_time AS CHAR), 3, 2)+' p.m'
                                         WHEN LEFT(active_start_time, 2) = (12)
                                              AND LEN(active_start_time) = 6
                                         THEN CAST(LEFT(active_start_time, 2) AS CHAR(2))+':'+SUBSTRING(CAST(active_start_time AS CHAR), 3, 2)+' p.m.'
                                         WHEN LEFT(active_start_time, 2) BETWEEN 13 AND 24
                                              AND LEN(active_start_time) = 6
                                         THEN CONVERT(VARCHAR(2), LEFT(active_start_time, 2) - 12)+':'+SUBSTRING(CAST(active_start_time AS CHAR), 3, 2)+' p.m.'
                                         WHEN LEFT(active_start_time, 2) IN(10, 11)
                                              AND LEN(active_start_time) = 6
                                         THEN CAST(LEFT(active_start_time, 2) AS CHAR(2))+':'+SUBSTRING(CAST(active_start_time AS CHAR), 3, 2)+' a.m.'
                                         WHEN active_start_time = 0
                                         THEN '12:00 a.m.'
                                         WHEN LEN(active_start_time) = 4
                                         THEN '12:'+CONVERT(VARCHAR(2), LEFT(active_start_time, 2))+' a.m.'
                                         WHEN LEN(active_start_time) = 3
                                         THEN '12:0'+CONVERT(VARCHAR(2), LEFT(active_start_time, 1))+' a.m.'
                                         WHEN LEN(active_start_time) = 2
                                         THEN '12:00:'+CONVERT(VARCHAR(2), LEFT(active_start_time, 2))+' a.m.'
                                         WHEN LEN(active_start_time) = 1
                                         THEN '12:00:0'+CONVERT(VARCHAR(2), LEFT(active_start_time, 2))+' a.m.'
                                         ELSE CAST(LEFT(active_start_time, 1) AS CHAR(1))+':'+SUBSTRING(CAST(active_start_time AS CHAR), 2, 2)+' a.m.'
                                     END,
                       @end_time = CASE
                                       WHEN LEFT(active_end_time, 2) IN(22, 23)
                                            AND LEN(active_end_time) = 6
                                       THEN CONVERT(VARCHAR(2), LEFT(active_end_time, 2) - 12)+':'+SUBSTRING(CAST(active_end_time AS CHAR), 3, 2)+' p.m'
                                       WHEN LEFT(active_end_time, 2) = (12)
                                            AND LEN(active_end_time) = 6
                                       THEN CAST(LEFT(active_end_time, 2) AS CHAR(2))+':'+SUBSTRING(CAST(active_end_time AS CHAR), 3, 2)+' p.m.'
                                       WHEN LEFT(active_end_time, 2) BETWEEN 13 AND 24
                                            AND LEN(active_end_time) = 6
                                       THEN CONVERT(VARCHAR(2), LEFT(active_end_time, 2) - 12)+':'+SUBSTRING(CAST(active_end_time AS CHAR), 3, 2)+' p.m.'
                                       WHEN LEFT(active_end_time, 2) IN(10, 11)
                                            AND LEN(active_end_time) = 6
                                       THEN CAST(LEFT(active_end_time, 2) AS CHAR(2))+':'+SUBSTRING(CAST(active_end_time AS CHAR), 3, 2)+' a.m.'
                                       WHEN active_end_time = 0
                                       THEN '12:00 a.m.'
                                       WHEN LEN(active_end_time) = 4
                                       THEN '12:'+CONVERT(VARCHAR(2), LEFT(active_end_time, 2))+' a.m.'
                                       WHEN LEN(active_end_time) = 3
                                       THEN '12:0'+CONVERT(VARCHAR(2), LEFT(active_end_time, 1))+' a.m.'
                                       WHEN LEN(active_end_time) = 2
                                       THEN '12:00:'+CONVERT(VARCHAR(2), LEFT(active_end_time, 2))+' a.m.'
                                       WHEN LEN(active_end_time) = 1
                                       THEN '12:00:0'+CONVERT(VARCHAR(2), LEFT(active_end_time, 2))+' a.m.'
                                       ELSE CAST(LEFT(active_end_time, 1) AS CHAR(1))+':'+SUBSTRING(CAST(active_end_time AS CHAR), 2, 2)+' a.m.'
                                   END
                FROM #schedules
                WHERE #schedules.schedule IS NULL
                      AND #schedules.scheduled = 1;
                IF EXISTS
(
    SELECT @freq_type
    WHERE @freq_type IN(1, 64)
)
                    BEGIN
                        SELECT @schedule = CASE @freq_type
                                               WHEN 1
                                               THEN 'occurs once, on '+CAST(@active_start_date AS VARCHAR(8))+', at '+@start_time
                                               WHEN 64
                                               THEN 'occurs when sql server agent starts'
                                           END;
                    END;
                    ELSE
                    BEGIN
                        IF @freq_type = 4
                            BEGIN
                                SELECT @schedule = 'occurs every '+CAST(@freq_interval AS VARCHAR(10))+' day(s)';
                            END;
                        IF @freq_type = 8
                            BEGIN
                                SELECT @schedule = 'occurs every '+CAST(@freq_recurrence_factor AS VARCHAR(3))+' week(s)';
                                SELECT @schedule_day = '';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 1)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'sun';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 2)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'mon';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 4)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'tue';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 8)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'wed';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 16)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'thu';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 32)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'fri';
                                IF
(
    SELECT(CONVERT(INT, (@freq_interval / 64)) % 2)
) = 1
                                    SELECT @schedule_day = @schedule_day+'sat';
                                SELECT @schedule = @schedule+', on '+@schedule_day;
                            END;
                        IF @freq_type = 16
                            BEGIN
                                SELECT @schedule = 'occurs every '+CAST(@freq_recurrence_factor AS VARCHAR(3))+' month(s) on day '+CAST(@freq_interval AS VARCHAR(3))+' of that month';
                            END;
                        IF @freq_type = 32
                            BEGIN
                                SELECT @schedule = CASE @freq_relative_interval
                                                       WHEN 1
                                                       THEN 'first'
                                                       WHEN 2
                                                       THEN 'second'
                                                       WHEN 4
                                                       THEN 'third'
                                                       WHEN 8
                                                       THEN 'fourth'
                                                       WHEN 16
                                                       THEN 'last'
                                                       ELSE 'not applicable'
                                                   END;
                                SELECT @schedule = CASE @freq_interval
                                                       WHEN 1
                                                       THEN 'occurs every '+@schedule+' sunday of the month'
                                                       WHEN 2
                                                       THEN 'occurs every '+@schedule+' monday of the month'
                                                       WHEN 3
                                                       THEN 'occurs every '+@schedule+' tueday of the month'
                                                       WHEN 4
                                                       THEN 'occurs every '+@schedule+' wednesday of the month'
                                                       WHEN 5
                                                       THEN 'occurs every '+@schedule+' thursday of the month'
                                                       WHEN 6
                                                       THEN 'occurs every '+@schedule+' friday of the month'
                                                       WHEN 7
                                                       THEN 'occurs every '+@schedule+' saturday of the month'
                                                       WHEN 8
                                                       THEN 'occurs every '+@schedule+' day of the month'
                                                       WHEN 9
                                                       THEN 'occurs every '+@schedule+' weekday of the month'
                                                       WHEN 10
                                                       THEN 'occurs every '+@schedule+' weekend day of the month'
                                                   END;
                            END;
                        SELECT @schedule = CASE @freq_subday_type
                                               WHEN 1
                                               THEN @schedule+', at '+@start_time
                                               WHEN 2
                                               THEN @schedule+', every '+CAST(@freq_subday_interval AS VARCHAR(3))+' second(s) between '+@start_time+' and '+@end_time
                                               WHEN 4
                                               THEN @schedule+', every '+CAST(@freq_subday_interval AS VARCHAR(3))+' minute(s) between '+@start_time+' and '+@end_time
                                               WHEN 8
                                               THEN @schedule+', every '+CAST(@freq_subday_interval AS VARCHAR(3))+' hour(s) between '+@start_time+' and '+@end_time
                                           END;
                    END;
            END;
        UPDATE #schedules
          SET
              #schedules.schedule = @schedule
        WHERE #schedules.job_id = @job_id
              AND #schedules.sched_id = @sched_id;
    END;
SELECT job_name,
       a.outcome,
       a.run_duration,
       CONVERT(VARCHAR, a.last_run_date)+' '+STUFF(STUFF(RIGHT('000000'+CONVERT(VARCHAR, a.last_run_time), 6), 5, 0, ':'), 3, 0, ':') runtime,
       [status] = CASE status
                      WHEN 1
                      THEN 'enabled'
                      WHEN 0
                      THEN 'disabled'
                      ELSE ' '
                  END,
       scheduled = CASE scheduled
                       WHEN 1
                       THEN 'yes'
                       WHEN 0
                       THEN 'no'
                       ELSE ' '
                   END,
       schedule AS 'frequency',
       CONVERT(DATETIME, CONVERT(VARCHAR, active_start_date, 101)) AS schedule_start_date,
       CONVERT(DATETIME, CONVERT(VARCHAR, active_end_date, 101)) AS schedule_end_date,
       date_created,
       a.step_name,
       a.step_id,
       a.duration
INTO #temp1
FROM #schedules
     LEFT JOIN
(
    SELECT DISTINCT
           name,
           a.step_name,
           a.run_duration,
           a.step_id,
           CASE
               WHEN a.run_status = 0
               THEN 'failed'
               WHEN a.run_status = 1
               THEN 'success'
               WHEN a.run_status = 2
               THEN 'retry'
               WHEN a.run_status = 3
               THEN 'canceled'
               WHEN a.run_status = 4
               THEN 'success'
           END outcome,
           a.run_date AS last_run_date,
           a.run_time AS last_run_time,
           CASE
               WHEN LEN(a.run_duration) = 1
               THEN '0:0'+CONVERT(VARCHAR, a.run_duration)
               WHEN LEN(a.run_duration) = 2
               THEN '0:'+CONVERT(VARCHAR, a.run_duration)
               WHEN LEN(a.run_duration) = 3
               THEN LEFT(CONVERT(VARCHAR, a.run_duration), 1)+':'+RIGHT(CONVERT(VARCHAR, a.run_duration), 2)
               WHEN LEN(a.run_duration) = 4
               THEN LEFT(CONVERT(VARCHAR, a.run_duration), 2)+':'+RIGHT(CONVERT(VARCHAR, a.run_duration), 2)
               WHEN LEN(a.run_duration) = 5
               THEN LEFT(CONVERT(VARCHAR, a.run_duration), 3)+':'+RIGHT(CONVERT(VARCHAR, a.run_duration), 2)
           END AS duration
    FROM dbo.sysjobhistory a
         INNER JOIN dbo.sysjobhistory b ON a.job_id = b.job_id
         INNER JOIN sysjobs ON sysjobs.job_id = a.job_id
    WHERE a.step_name NOT LIKE '%outcome%'
          AND b.step_name LIKE '%outcome%'
) a ON a.name = #schedules.job_name
ORDER BY #schedules.job_name;
SELECT DISTINCT
       a.job_name,
       a.step_name,
       a.step_id,
       s.schedule,
       a.outcome,
       a.run_duration seconds,
       a.runtime starttime,
       DATEADD(ss, a.run_duration, a.runtime) endtime,
       a.status,
       a.scheduled
FROM #temp1 a
     INNER JOIN #schedules s ON a.job_name = s.job_name
ORDER BY a.runtime DESC;
SELECT DISTINCT
       #temp1.job_name,
       #temp1.frequency,
       #temp1.status,
       #temp1.scheduled
FROM #temp1;
GO
DROP TABLE #schedules;
DROP TABLE #temp1;
