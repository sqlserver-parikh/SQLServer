USE tempdb
GO
CREATE OR ALTER PROCEDURE usp_AgentJobReport
(
    @jStatus varchar(128) = 'ALL' --Enabled, Disabled, ALL
    , @jIsScheduled varchar(128) = 'ALL' --Yes, No, ALL
    , @ShowOnlyFailedJobsInPast24Hours bit = 0
    , @ShowOnlyFailedJobsInPast7Days bit = 0
    , @ShowOnlyFailedJobsInPast30Days bit = 0
    , @jobname varchar(128) = NULL
    , @EmailRecipients VARCHAR(MAX) = 'email@company.com'
    , @ReportOnly BIT = 1  -- 1 = just show data, 0 = email results
)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Get domain names from Windows logins
    DECLARE @DomainNames NVARCHAR(MAX) = '';
    
    SELECT @DomainNames = STUFF((
        SELECT DISTINCT ', ' + LEFT(name, CHARINDEX('\', name) - 1)
        FROM sys.server_principals
        WHERE type_desc IN ('WINDOWS_LOGIN', 'WINDOWS_GROUP')
          AND name LIKE '%\%' 
          AND name NOT LIKE 'NT %' 
          AND name NOT LIKE 'BUILTIN%'
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '');
    
    -- Set default job name filter if NULL
    IF @jobname IS NULL
        SET @jobname = '%'
        
    -- Set status filter
    IF @jStatus not in ('Enabled', 'Disabled')
        SET @jStatus = '%'
        
    -- Set scheduled filter
    IF @jIsScheduled not in ('Yes','No')
        SET @jIsScheduled = '%'
    
    -- Set default email address if not provided and report only mode is off
    IF @ReportOnly = 0 AND @EmailRecipients IS NULL
    BEGIN
        SELECT @EmailRecipients = email_address 
        FROM msdb..sysoperators
        WHERE name LIKE 'SQLDBATeam'
    END
    
    -- Get email profile if not in report only mode
    DECLARE @EmailProfile VARCHAR(128);
    IF @ReportOnly = 0
    BEGIN
        SELECT @EmailProfile = name
        FROM msdb..sysmail_profile
        WHERE profile_id = 1;
    END

    -- Define the time intervals
    DECLARE @Past7Days DATETIME = DATEADD(DAY, -7, GETDATE());
    DECLARE @Past24Hours DATETIME = DATEADD(HOUR, -24, GETDATE());
    DECLARE @Past30Days DATETIME = DATEADD(DAY, -30, GETDATE());

    -- Create a temporary table to store job run details
    IF OBJECT_ID('tempdb..#JobRunDetails') IS NOT NULL
        DROP TABLE #JobRunDetails;
        
    CREATE TABLE #JobRunDetails
    (
        JobName NVARCHAR(128)
      , RunDate DATETIME
      , RunStatus INT
      , Enabled bit
    );

    -- Insert job run details into the temporary table
    INSERT INTO #JobRunDetails
    (
        JobName
      , RunDate
      , RunStatus
      , Enabled
    )
    SELECT j.name AS JobName
         , CAST(CAST(ms.run_date AS CHAR(8)) + ' '
                + STUFF(STUFF(RIGHT('000000' + CAST(ms.run_time AS VARCHAR(6)), 6), 5, 0, ':'), 3, 0, ':') AS DATETIME) AS RunDate
         , ms.run_status AS RunStatus
         , j.enabled
    FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobhistory ms
            ON j.job_id = ms.job_id
    WHERE ms.step_id = 0; -- Only consider the final outcome of the job

    -- Calculate job stats and store in temp table
    IF OBJECT_ID('tempdb..#JobStats') IS NOT NULL
        DROP TABLE #JobStats;
        
    SELECT JobName
         , Enabled
         , CONCAT(
                 'RunsInPast24Hours = '
               , SUM(CASE WHEN RunDate >= @Past24Hours THEN 1 ELSE 0 END)
               , ' (Success: '
               , SUM(CASE WHEN RunDate >= @Past24Hours AND RunStatus = 1 THEN 1 ELSE 0 END)
               , ', Fail: '
               , SUM(CASE WHEN RunDate >= @Past24Hours AND RunStatus = 0 THEN 1 ELSE 0 END)
               , ')'
             ) AS RunsInPast24Hours
         , CONCAT(
                 'RunsInPast7Days = '
               , SUM(CASE WHEN RunDate >= @Past7Days THEN 1 ELSE 0 END)
               , ' (Success: '
               , SUM(CASE WHEN RunDate >= @Past7Days AND RunStatus = 1 THEN 1 ELSE 0 END)
               , ', Fail: '
               , SUM(CASE WHEN RunDate >= @Past7Days AND RunStatus = 0 THEN 1 ELSE 0 END)
               , ')'
             ) AS RunsInPast7Days
         , CONCAT(
                 'RunsInPast30Days = '
               , SUM(CASE WHEN RunDate >= @Past30Days THEN 1 ELSE 0 END)
               , ' (Success: '
               , SUM(CASE WHEN RunDate >= @Past30Days AND RunStatus = 1 THEN 1 ELSE 0 END)
               , ', Fail: '
               , SUM(CASE WHEN RunDate >= @Past30Days AND RunStatus = 0 THEN 1 ELSE 0 END)
               , ')'
             ) AS RunsInPast30Days
         , CAST(SUM(CASE WHEN RunDate >= @Past24Hours AND RunStatus = 0 THEN 1 ELSE 0 END) AS FLOAT) / 
           NULLIF(SUM(CASE WHEN RunDate >= @Past24Hours THEN 1 ELSE 0 END), 0) * 100 AS FailurePercentageInPast24Hours
         , CAST(SUM(CASE WHEN RunDate >= @Past7Days AND RunStatus = 0 THEN 1 ELSE 0 END) AS FLOAT) / 
           NULLIF(SUM(CASE WHEN RunDate >= @Past7Days THEN 1 ELSE 0 END), 0) * 100 AS FailurePercentageInPast7Days
         , CAST(SUM(CASE WHEN RunDate >= @Past30Days AND RunStatus = 0 THEN 1 ELSE 0 END) AS FLOAT) / 
           NULLIF(SUM(CASE WHEN RunDate >= @Past30Days THEN 1 ELSE 0 END), 0) * 100 AS FailurePercentageInPast30Days
    INTO #JobStats
    FROM #JobRunDetails
    GROUP BY JobName, Enabled;
    
    -- Process job schedules
    IF OBJECT_ID('tempdb..#schedules') IS NOT NULL
        DROP TABLE #schedules;
        
    CREATE TABLE #schedules
    (
        job_id VARCHAR(200)
      , sched_id VARCHAR(200)
      , job_name SYSNAME
      , [status] INT
      , scheduled INT NULL
      , schedule VARCHAR(1000) NULL
      , freq_type INT NULL
      , freq_interval INT NULL
      , freq_subday_type INT NULL
      , freq_subday_interval INT NULL
      , freq_relative_interval INT NULL
      , freq_recurrence_factor INT NULL
      , active_start_date INT NULL
      , active_end_date INT NULL
      , active_start_time INT NULL
      , active_end_time INT NULL
      , date_created DATETIME NULL
    );
    
    -- Get job schedule data
    INSERT INTO #schedules
    (
        job_id, sched_id, job_name, [status], scheduled, schedule,
        freq_type, freq_interval, freq_subday_type, freq_subday_interval,
        freq_relative_interval, freq_recurrence_factor,
        active_start_date, active_end_date, active_start_time, active_end_time, date_created
    )
    SELECT j.job_id, sched.schedule_id, j.name, j.enabled, sched1.enabled, NULL,
           sched1.freq_type, sched1.freq_interval, sched1.freq_subday_type, sched1.freq_subday_interval,
           sched1.freq_relative_interval, sched1.freq_recurrence_factor,
           sched1.active_start_date, sched1.active_end_date, sched1.active_start_time, sched1.active_end_time, j.date_created
    FROM msdb..sysjobs j
    INNER JOIN msdb..sysjobschedules s ON j.job_id = s.job_id
    INNER JOIN msdb.dbo.sysjobschedules sched ON s.schedule_id = sched.schedule_id
    INNER JOIN msdb.dbo.sysschedules sched1 ON s.schedule_id = sched1.schedule_id;

    -- Process schedule descriptions
    DECLARE @job_id VARCHAR(200), @sched_id VARCHAR(200), @freq_type INT, @freq_interval INT,
            @freq_subday_type INT, @freq_subday_interval INT, @freq_relative_interval INT,
            @freq_recurrence_factor INT, @active_start_date INT, @schedule VARCHAR(1000),
            @schedule_day VARCHAR(200), @start_time VARCHAR(10), @end_time VARCHAR(10);
    
    WHILE 1 = 1
    BEGIN
        SET @schedule = '';
        IF (SELECT COUNT(*) FROM #schedules WHERE scheduled = 1 AND schedule IS NULL) = 0
            BREAK;
        
        SELECT @job_id = job_id, @sched_id = sched_id, @freq_type = freq_type,
               @freq_interval = freq_interval, @freq_subday_type = freq_subday_type,
               @freq_subday_interval = freq_subday_interval, @freq_relative_interval = freq_relative_interval,
               @freq_recurrence_factor = freq_recurrence_factor, @active_start_date = active_start_date,
               @start_time = CASE
                              WHEN LEFT(active_start_time, 2) IN (22, 23) AND LEN(active_start_time) = 6 THEN
                                  CONVERT(VARCHAR(2), LEFT(active_start_time, 2) - 12) + ':' + SUBSTRING(CAST(active_start_time AS CHAR), 3, 2) + ' p.m'
                              WHEN LEFT(active_start_time, 2) = (12) AND LEN(active_start_time) = 6 THEN
                                  CAST(LEFT(active_start_time, 2) AS CHAR(2)) + ':' + SUBSTRING(CAST(active_start_time AS CHAR), 3, 2) + ' p.m.'
                              WHEN LEFT(active_start_time, 2) BETWEEN 13 AND 24 AND LEN(active_start_time) = 6 THEN
                                  CONVERT(VARCHAR(2), LEFT(active_start_time, 2) - 12) + ':' + SUBSTRING(CAST(active_start_time AS CHAR), 3, 2) + ' p.m.'
                              WHEN LEFT(active_start_time, 2) IN (10, 11) AND LEN(active_start_time) = 6 THEN
                                  CAST(LEFT(active_start_time, 2) AS CHAR(2)) + ':' + SUBSTRING(CAST(active_start_time AS CHAR), 3, 2) + ' a.m.'
                              WHEN active_start_time = 0 THEN '12:00 a.m.'
                              WHEN LEN(active_start_time) = 4 THEN '12:' + CONVERT(VARCHAR(2), LEFT(active_start_time, 2)) + ' a.m.'
                              WHEN LEN(active_start_time) = 3 THEN '12:0' + CONVERT(VARCHAR(2), LEFT(active_start_time, 1)) + ' a.m.'
                              WHEN LEN(active_start_time) = 2 THEN '12:00:' + CONVERT(VARCHAR(2), LEFT(active_start_time, 2)) + ' a.m.'
                              WHEN LEN(active_start_time) = 1 THEN '12:00:0' + CONVERT(VARCHAR(2), LEFT(active_start_time, 2)) + ' a.m.'
                              ELSE CAST(LEFT(active_start_time, 1) AS CHAR(1)) + ':' + SUBSTRING(CAST(active_start_time AS CHAR), 2, 2) + ' a.m.'
                           END,
               @end_time = CASE
                            WHEN LEFT(active_end_time, 2) IN (22, 23) AND LEN(active_end_time) = 6 THEN
                                CONVERT(VARCHAR(2), LEFT(active_end_time, 2) - 12) + ':' + SUBSTRING(CAST(active_end_time AS CHAR), 3, 2) + ' p.m'
                            WHEN LEFT(active_end_time, 2) = (12) AND LEN(active_end_time) = 6 THEN
                                CAST(LEFT(active_end_time, 2) AS CHAR(2)) + ':' + SUBSTRING(CAST(active_end_time AS CHAR), 3, 2) + ' p.m.'
                            WHEN LEFT(active_end_time, 2) BETWEEN 13 AND 24 AND LEN(active_end_time) = 6 THEN
                                CONVERT(VARCHAR(2), LEFT(active_end_time, 2) - 12) + ':' + SUBSTRING(CAST(active_end_time AS CHAR), 3, 2) + ' p.m.'
                            WHEN LEFT(active_end_time, 2) IN (10, 11) AND LEN(active_end_time) = 6 THEN
                                CAST(LEFT(active_end_time, 2) AS CHAR(2)) + ':' + SUBSTRING(CAST(active_end_time AS CHAR), 3, 2) + ' a.m.'
                            WHEN active_end_time = 0 THEN '12:00 a.m.'
                            WHEN LEN(active_end_time) = 4 THEN '12:' + CONVERT(VARCHAR(2), LEFT(active_end_time, 2)) + ' a.m.'
                            WHEN LEN(active_end_time) = 3 THEN '12:0' + CONVERT(VARCHAR(2), LEFT(active_end_time, 1)) + ' a.m.'
                            WHEN LEN(active_end_time) = 2 THEN '12:00:' + CONVERT(VARCHAR(2), LEFT(active_end_time, 2)) + ' a.m.'
                            WHEN LEN(active_end_time) = 1 THEN '12:00:0' + CONVERT(VARCHAR(2), LEFT(active_end_time, 2)) + ' a.m.'
                            ELSE CAST(LEFT(active_end_time, 1) AS CHAR(1)) + ':' + SUBSTRING(CAST(active_end_time AS CHAR), 2, 2) + ' a.m.'
                         END
        FROM #schedules
        WHERE schedule IS NULL AND scheduled = 1;
        
        -- Generate schedule description based on frequency type
        IF EXISTS (SELECT @freq_type WHERE @freq_type IN (1, 64))
        BEGIN
            SELECT @schedule = CASE @freq_type
                                WHEN 1 THEN 'occurs once, ON ' + CAST(@active_start_date AS VARCHAR(8)) + ', at ' + @start_time
                                WHEN 64 THEN 'occurs when sql server agent starts'
                              END;
        END
        ELSE
        BEGIN
            -- Daily schedule
            IF @freq_type = 4
                SELECT @schedule = 'occurs every ' + CAST(@freq_interval AS VARCHAR(10)) + ' day(s)';
            
            -- Weekly schedule
            IF @freq_type = 8
            BEGIN
                SELECT @schedule = 'occurs every ' + CAST(@freq_recurrence_factor AS VARCHAR(3)) + ' week(s)';
                SELECT @schedule_day = '';
                
                IF (SELECT (CONVERT(INT, (@freq_interval / 1)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'sun';
                IF (SELECT (CONVERT(INT, (@freq_interval / 2)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'mon';
                IF (SELECT (CONVERT(INT, (@freq_interval / 4)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'tue';
                IF (SELECT (CONVERT(INT, (@freq_interval / 8)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'wed';
                IF (SELECT (CONVERT(INT, (@freq_interval / 16)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'thu';
                IF (SELECT (CONVERT(INT, (@freq_interval / 32)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'fri';
                IF (SELECT (CONVERT(INT, (@freq_interval / 64)) % 2)) = 1
                    SELECT @schedule_day = @schedule_day + 'sat';
                
                SELECT @schedule = @schedule + ', ON ' + @schedule_day;
            END;
            
            -- Monthly schedule
            IF @freq_type = 16
            BEGIN
                SELECT @schedule = 'occurs every ' + CAST(@freq_recurrence_factor AS VARCHAR(3)) + 
                                   ' month(s) ON day ' + CAST(@freq_interval AS VARCHAR(3)) + ' of that month';
            END;
            
            -- Monthly relative schedule
            IF @freq_type = 32
            BEGIN
                SELECT @schedule = CASE @freq_relative_interval
                                    WHEN 1 THEN 'first'
                                    WHEN 2 THEN 'second'
                                    WHEN 4 THEN 'third'
                                    WHEN 8 THEN 'fourth'
                                    WHEN 16 THEN 'last'
                                    ELSE 'not applicable'
                                   END;
                
                SELECT @schedule = CASE @freq_interval
                                    WHEN 1 THEN 'occurs every ' + @schedule + ' sunday of the month'
                                    WHEN 2 THEN 'occurs every ' + @schedule + ' monday of the month'
                                    WHEN 3 THEN 'occurs every ' + @schedule + ' tueday of the month'
                                    WHEN 4 THEN 'occurs every ' + @schedule + ' wednesday of the month'
                                    WHEN 5 THEN 'occurs every ' + @schedule + ' thursday of the month'
                                    WHEN 6 THEN 'occurs every ' + @schedule + ' friday of the month'
                                    WHEN 7 THEN 'occurs every ' + @schedule + ' saturday of the month'
                                    WHEN 8 THEN 'occurs every ' + @schedule + ' day of the month'
                                    WHEN 9 THEN 'occurs every ' + @schedule + ' weekday of the month'
                                    WHEN 10 THEN 'occurs every ' + @schedule + ' weekend day of the month'
                                   END;
            END;
            
            -- Append frequency details
            SELECT @schedule = CASE @freq_subday_type
                                WHEN 1 THEN @schedule + ', at ' + @start_time
                                WHEN 2 THEN @schedule + ', every ' + CAST(@freq_subday_interval AS VARCHAR(3)) + 
                                            ' second(s) between ' + @start_time + ' and ' + @end_time
                                WHEN 4 THEN @schedule + ', every ' + CAST(@freq_subday_interval AS VARCHAR(3)) + 
                                            ' minute(s) between ' + @start_time + ' and ' + @end_time
                                WHEN 8 THEN @schedule + ', every ' + CAST(@freq_subday_interval AS VARCHAR(3)) + 
                                            ' hour(s) between ' + @start_time + ' and ' + @end_time
                               END;
        END;
        
        -- Update schedule description
        UPDATE #schedules
        SET schedule = @schedule
        WHERE job_id = @job_id AND sched_id = @sched_id;
    END;
    
    -- Get job details and create final schedule table
    IF OBJECT_ID('tempdb..#temp1') IS NOT NULL
        DROP TABLE #temp1;
        
    -- This is where the ambiguous column error occurs - fixed by fully qualifying column names
    SELECT s.job_name,
           CASE WHEN a.outcome IS NULL THEN NULL ELSE a.outcome END AS outcome,
           CASE WHEN a.run_duration IS NULL THEN NULL ELSE a.run_duration END AS run_duration,
           CASE WHEN a.last_run_date IS NULL THEN NULL
                ELSE CONVERT(VARCHAR, a.last_run_date) + ' ' +
                     STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR, a.last_run_time), 6), 5, 0, ':'), 3, 0, ':')
           END AS runtime,
           CASE s.status
               WHEN 1 THEN 'enabled'
               WHEN 0 THEN 'disabled'
               ELSE ' '
           END AS status,
           CASE s.scheduled
               WHEN 1 THEN 'yes'
               WHEN 0 THEN 'no'
               ELSE ' '
           END AS scheduled,
           s.schedule AS 'frequency',
           CONVERT(DATETIME, CONVERT(VARCHAR, s.active_start_date, 101)) AS schedule_start_date,
           CONVERT(DATETIME, CONVERT(VARCHAR, s.active_end_date, 101)) AS schedule_end_date,
           s.date_created,
           a.step_name,
           a.step_id,
           a.duration
    INTO #temp1
    FROM #schedules s
    LEFT JOIN (
        SELECT DISTINCT
            name,
            jh.step_name,
            jh.run_duration,
            jh.step_id,
            CASE
                WHEN jh.run_status = 0 THEN 'failed'
                WHEN jh.run_status = 1 THEN 'success'
                WHEN jh.run_status = 2 THEN 'retry'
                WHEN jh.run_status = 3 THEN 'canceled'
                WHEN jh.run_status = 4 THEN 'success'
            END AS outcome,
            jh.run_date AS last_run_date,
            jh.run_time AS last_run_time,
            CASE
                WHEN LEN(jh.run_duration) = 1 THEN '0:0' + CONVERT(VARCHAR, jh.run_duration)
                WHEN LEN(jh.run_duration) = 2 THEN '0:' + CONVERT(VARCHAR, jh.run_duration)
                WHEN LEN(jh.run_duration) = 3 THEN LEFT(CONVERT(VARCHAR, jh.run_duration), 1) + ':' + RIGHT(CONVERT(VARCHAR, jh.run_duration), 2)
                WHEN LEN(jh.run_duration) = 4 THEN LEFT(CONVERT(VARCHAR, jh.run_duration), 2) + ':' + RIGHT(CONVERT(VARCHAR, jh.run_duration), 2)
                WHEN LEN(jh.run_duration) = 5 THEN LEFT(CONVERT(VARCHAR, jh.run_duration), 3) + ':' + RIGHT(CONVERT(VARCHAR, jh.run_duration), 2)
            END AS duration
        FROM msdb.dbo.sysjobhistory jh
        INNER JOIN msdb.dbo.sysjobhistory b ON jh.job_id = b.job_id
        INNER JOIN msdb.dbo.sysjobs ON sysjobs.job_id = jh.job_id
        WHERE jh.step_name NOT LIKE '%outcome%' AND b.step_name LIKE '%outcome%'
    ) a ON a.name = s.job_name;

    -- Create final schedule table
    IF OBJECT_ID('tempdb..#finalschedule') IS NOT NULL
        DROP TABLE #finalschedule;
        
    SELECT DISTINCT
        job_name AS JobName,
        frequency AS Schedule,
        status AS Status,
        scheduled AS IsScheduled
    INTO #finalschedule
    FROM #temp1;
    
    -- Create final results table
    IF OBJECT_ID('tempdb..#FinalResults') IS NOT NULL
        DROP TABLE #FinalResults;
        
    CREATE TABLE #FinalResults (
        JobName NVARCHAR(128),
        Schedule NVARCHAR(1000),
        Status VARCHAR(10),
        IsScheduled VARCHAR(10),
        RunsInPast24Hours NVARCHAR(100),
        RunsInPast7Days NVARCHAR(100),
        RunsInPast30Days NVARCHAR(100),
        FailurePercentageInPast24Hours FLOAT,
        FailurePercentageInPast7Days FLOAT,
        FailurePercentageInPast30Days FLOAT
    );
    
    -- Insert results based on parameters
    IF @ShowOnlyFailedJobsInPast24Hours = 1
    BEGIN
        INSERT INTO #FinalResults
        SELECT a.JobName, a.Schedule, a.Status, a.IsScheduled,
               b.RunsInPast24Hours, b.RunsInPast7Days, b.RunsInPast30Days,
               b.FailurePercentageInPast24Hours, b.FailurePercentageInPast7Days, b.FailurePercentageInPast30Days
        FROM #finalschedule a
        LEFT JOIN #JobStats b ON a.JobName = b.JobName
        WHERE a.Status LIKE @jStatus
          AND a.IsScheduled LIKE @jIsScheduled
          AND ISNULL(b.FailurePercentageInPast24Hours, 0) > 0;
    END
    ELSE IF @ShowOnlyFailedJobsInPast7Days = 1
    BEGIN
        INSERT INTO #FinalResults
        SELECT a.JobName, a.Schedule, a.Status, a.IsScheduled,
               b.RunsInPast24Hours, b.RunsInPast7Days, b.RunsInPast30Days,
               b.FailurePercentageInPast24Hours, b.FailurePercentageInPast7Days, b.FailurePercentageInPast30Days
        FROM #finalschedule a
        LEFT JOIN #JobStats b ON a.JobName = b.JobName
        WHERE a.Status LIKE @jStatus
          AND a.IsScheduled LIKE @jIsScheduled
          AND ISNULL(b.FailurePercentageInPast7Days, 0) > 0;
    END
    ELSE IF @ShowOnlyFailedJobsInPast30Days = 1
    BEGIN
        INSERT INTO #FinalResults
        SELECT a.JobName, a.Schedule, a.Status, a.IsScheduled,
               b.RunsInPast24Hours, b.RunsInPast7Days, b.RunsInPast30Days,
               b.FailurePercentageInPast24Hours, b.FailurePercentageInPast7Days, b.FailurePercentageInPast30Days
        FROM #finalschedule a
        LEFT JOIN #JobStats b ON a.JobName = b.JobName
        WHERE a.Status LIKE @jStatus
          AND a.IsScheduled LIKE @jIsScheduled
          AND ISNULL(b.FailurePercentageInPast30Days, 0) > 0;
    END
    ELSE
    BEGIN
        INSERT INTO #FinalResults
        SELECT a.JobName, a.Schedule, a.Status, a.IsScheduled,
               b.RunsInPast24Hours, b.RunsInPast7Days, b.RunsInPast30Days,
               b.FailurePercentageInPast24Hours, b.FailurePercentageInPast7Days, b.FailurePercentageInPast30Days
        FROM #finalschedule a
        LEFT JOIN #JobStats b ON a.JobName = b.JobName
        WHERE a.Status LIKE @jStatus
          AND a.IsScheduled LIKE @jIsScheduled
          AND a.JobName LIKE @jobname;
    END
    
    -- Display results to user
    SELECT * FROM #FinalResults ORDER BY
        CASE 
            WHEN @ShowOnlyFailedJobsInPast24Hours = 1 THEN FailurePercentageInPast24Hours
            WHEN @ShowOnlyFailedJobsInPast7Days = 1 THEN FailurePercentageInPast7Days
            WHEN @ShowOnlyFailedJobsInPast30Days = 1 THEN FailurePercentageInPast30Days
            ELSE FailurePercentageInPast24Hours 
        END DESC;
    
    -- If specific job details requested
    IF LEN(@jobname) > 1 AND @jobname <> '%'
    BEGIN
        SELECT * FROM #temp1
        WHERE job_name LIKE @jobname;
    END
    
    -- Send email if not report only mode
    IF @ReportOnly = 0 AND EXISTS (SELECT 1 FROM #FinalResults)
    BEGIN
        -- Get summary statistics
        DECLARE @TotalJobs INT;
        DECLARE @FailedJobs24hrs INT;
        DECLARE @FailedJobs7Days INT;
        DECLARE @FailedJobs30Days INT;
        
        SELECT @TotalJobs = COUNT(*),
               @FailedJobs24hrs = SUM(CASE WHEN FailurePercentageInPast24Hours > 0 THEN 1 ELSE 0 END),
               @FailedJobs7Days = SUM(CASE WHEN FailurePercentageInPast7Days > 0 THEN 1 ELSE 0 END),
               @FailedJobs30Days = SUM(CASE WHEN FailurePercentageInPast30Days > 0 THEN 1 ELSE 0 END)
        FROM #FinalResults;
        
        -- Create HTML email body
        DECLARE @TableHTML NVARCHAR(MAX);
        DECLARE @TableRows NVARCHAR(MAX) = '';
        DECLARE @Subject VARCHAR(255);
        
        SET @TableHTML = N'
        <html>
        <head>
            <style>
                body { font-family: Calibri, Arial, sans-serif; font-size: 11pt; }
                h2 { color: #00008B; }
                h3 { color: #0000CD; }
                table { border-collapse: collapse; width: 100%; }
                th { background-color: #D8D8D8; padding: 5px; text-align: left; border: 1px solid #A9A9A9; }
                td { padding: 5px; border: 1px solid #A9A9A9; }
                .highlight { background-color: #FFFACD; }
                .failed { background-color: #FFC0CB; font-weight: bold; }
                .summary { background-color: #E6E6FA; padding: 10px; margin-bottom: 15px; border-radius: 5px; }
            </style>
        </head>
        <body>
            <h2>SQL Server Job Failure Report: ' + @@SERVERNAME + '</h2>
            
<div class="summary">
                <h3>Jobs Summary:</h3>
                <ul>
                    <li><strong>Total Jobs Analyzed:</strong> ' + CAST(@TotalJobs AS VARCHAR) + '</li>
                    <li><strong>Failed Jobs (Last 24 Hours):</strong> ' + CAST(@FailedJobs24hrs AS VARCHAR) + '</li>
                    <li><strong>Failed Jobs (Last 7 Days):</strong> ' + CAST(@FailedJobs7Days AS VARCHAR) + '</li>
                    <li><strong>Failed Jobs (Last 30 Days):</strong> ' + CAST(@FailedJobs30Days AS VARCHAR) + '</li>
                    <li><strong>Report Generated:</strong> ' + CONVERT(VARCHAR, GETDATE(), 120) + '</li>
                </ul>
            </div>';
            
        -- Add the jobs table
        SET @TableHTML = @TableHTML + N'
            <h3>Failed SQL Agent Jobs:</h3>
            <table>
            <tr>
                <th>Job Name</th>
                <th>Status</th>
                <th>Scheduled</th>
                <th>Schedule</th>
                <th>24 Hour Failures</th>
                <th>7 Day Failures</th>
                <th>30 Day Failures</th>
            </tr>';
            
        -- Add table rows
        DECLARE job_cursor CURSOR FOR
        SELECT JobName, Status, IsScheduled, Schedule, 
               RunsInPast24Hours, RunsInPast7Days, RunsInPast30Days,
               FailurePercentageInPast24Hours, FailurePercentageInPast7Days, FailurePercentageInPast30Days
        FROM #FinalResults
        ORDER BY 
            CASE 
                WHEN @ShowOnlyFailedJobsInPast24Hours = 1 THEN FailurePercentageInPast24Hours
                WHEN @ShowOnlyFailedJobsInPast7Days = 1 THEN FailurePercentageInPast7Days
                WHEN @ShowOnlyFailedJobsInPast30Days = 1 THEN FailurePercentageInPast30Days
                ELSE FailurePercentageInPast24Hours
            END DESC;
        
        DECLARE @CurJobName NVARCHAR(128);
        DECLARE @CurStatus VARCHAR(10);
        DECLARE @CurIsScheduled VARCHAR(10);
        DECLARE @CurSchedule NVARCHAR(1000);
        DECLARE @CurRunsInPast24Hours NVARCHAR(100);
        DECLARE @CurRunsInPast7Days NVARCHAR(100);
        DECLARE @CurRunsInPast30Days NVARCHAR(100);
        DECLARE @CurFailPct24 FLOAT;
        DECLARE @CurFailPct7 FLOAT;
        DECLARE @CurFailPct30 FLOAT;
        DECLARE @RowClass VARCHAR(20);
        
        OPEN job_cursor;
        FETCH NEXT FROM job_cursor INTO 
            @CurJobName, @CurStatus, @CurIsScheduled, @CurSchedule,
            @CurRunsInPast24Hours, @CurRunsInPast7Days, @CurRunsInPast30Days,
            @CurFailPct24, @CurFailPct7, @CurFailPct30;
            
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Determine row class based on failure percentage
            SET @RowClass = 
                CASE 
                    WHEN @CurFailPct24 >= 50 OR @CurFailPct7 >= 50 OR @CurFailPct30 >= 50 
                    THEN 'failed'
                    WHEN @CurFailPct24 > 0 OR @CurFailPct7 > 0 OR @CurFailPct30 > 0 
                    THEN 'highlight'
                    ELSE ''
                END;
                
            -- Build the table row with HTML class
            SET @TableRows = @TableRows +
                CASE WHEN @RowClass = '' 
                     THEN '<tr>'
                     ELSE '<tr class="' + @RowClass + '">'
                END +
                '<td>' + @CurJobName + '</td>' +
                '<td>' + @CurStatus + '</td>' +
                '<td>' + @CurIsScheduled + '</td>' +
                '<td>' + ISNULL(@CurSchedule, 'N/A') + '</td>' +
                '<td>' + ISNULL(@CurRunsInPast24Hours, 'N/A') + '</td>' +
                '<td>' + ISNULL(@CurRunsInPast7Days, 'N/A') + '</td>' +
                '<td>' + ISNULL(@CurRunsInPast30Days, 'N/A') + '</td>' +
                '</tr>';
                
            FETCH NEXT FROM job_cursor INTO 
                @CurJobName, @CurStatus, @CurIsScheduled, @CurSchedule,
                @CurRunsInPast24Hours, @CurRunsInPast7Days, @CurRunsInPast30Days,
                @CurFailPct24, @CurFailPct7, @CurFailPct30;
        END
        
        CLOSE job_cursor;
        DEALLOCATE job_cursor;
        
        -- Close the table and add footer information
        SET @TableHTML = @TableHTML + @TableRows + N'
            </table>
            
            <h3>Monitoring Parameters:</h3>
            <ul>
                <li><strong>Jobs Status Filter:</strong> ' + CASE WHEN @jStatus = '%' THEN 'ALL' ELSE @jStatus END + '</li>
                <li><strong>Jobs Scheduled Filter:</strong> ' + CASE WHEN @jIsScheduled = '%' THEN 'ALL' ELSE @jIsScheduled END + '</li>
                <li><strong>Job Name Filter:</strong> ' + CASE WHEN @jobname = '%' THEN 'ALL' ELSE @jobname END + '</li>
                <li><strong>Show Only Failed Jobs (24h):</strong> ' + CASE WHEN @ShowOnlyFailedJobsInPast24Hours = 1 THEN 'Yes' ELSE 'No' END + '</li>
                <li><strong>Show Only Failed Jobs (7 days):</strong> ' + CASE WHEN @ShowOnlyFailedJobsInPast7Days = 1 THEN 'Yes' ELSE 'No' END + '</li>
                <li><strong>Show Only Failed Jobs (30 days):</strong> ' + CASE WHEN @ShowOnlyFailedJobsInPast30Days = 1 THEN 'Yes' ELSE 'No' END + '</li>
                <li><strong>Domain Name:</strong> ' + CAST(@DomainNames AS VARCHAR) + '</li>
            </ul>
            
            <p style="font-size: 10pt; color: #666666;">
                This is an automated message from SQL Server job monitoring system.<br/>
                Generated on ' + CONVERT(VARCHAR, GETDATE(), 120) + ' for server ' + @@SERVERNAME + '
            </p>
        </body>
        </html>';
        
        -- Send the email
        SET @Subject = 'SQL Agent Job Failure Report - ' + @@SERVERNAME;
        
        IF @FailedJobs24hrs > 0
            SET @Subject = 'ALERT: ' + @Subject + ' - ' + CAST(@FailedJobs24hrs AS VARCHAR) + ' jobs failed in last 24h';
            
        EXEC msdb.dbo.sp_send_dbmail
            @recipients = @EmailRecipients,
            @profile_name = @EmailProfile,
            @subject = @Subject,
            @body = @TableHTML,
            @body_format = 'HTML';
            
        PRINT 'Email sent to: ' + @EmailRecipients;
    END
    
    -- Clean up temp tables
    IF OBJECT_ID('tempdb..#schedules') IS NOT NULL
        DROP TABLE #schedules;
    IF OBJECT_ID('tempdb..#temp1') IS NOT NULL
        DROP TABLE #temp1;
    IF OBJECT_ID('tempdb..#finalschedule') IS NOT NULL
        DROP TABLE #finalschedule;
    IF OBJECT_ID('tempdb..#JobStats') IS NOT NULL
        DROP TABLE #JobStats;
    IF OBJECT_ID('tempdb..#JobRunDetails') IS NOT NULL
        DROP TABLE #JobRunDetails;
    IF OBJECT_ID('tempdb..#FinalResults') IS NOT NULL
        DROP TABLE #FinalResults;
END
GO

-- Example execution
EXEC usp_AgentJobReport 
    @jStatus = 'Enabled',
    @jIsScheduled = 'Yes',
    @ShowOnlyFailedJobsInPast24Hours = 0,
    @ShowOnlyFailedJobsInPast7Days = 1,
    @ShowOnlyFailedJobsInPast30Days = 0,
    @ReportOnly = 1;  -- Set to 0 to send email
