USE DBASupport
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_BlockingMonitor]
(
    @MinBlockedSessions         INT = 1,              -- Minimum number of sessions a blocker must be blocking
    @WaitTimeThresholdSec       INT = 300,            -- Initial wait time threshold in seconds
    @FollowupWaitTimeThresholdSec INT = 700,          -- Follow-up wait time threshold in seconds
    @EmailRecipients            VARCHAR(MAX) = 'mymail@company.com',  -- Email recipients for the alert (NULL uses operator)
    @ReportOnly                 BIT = 0,              -- Only display the report, don't send email (1=Yes, 0=No)
    @FollowupIntervalMins       INT = 60,              -- Minimum time between follow-up emails in minutes
    @DBName                     NVARCHAR(128) = ''  -- Database name to filter on, NULL/''/all means all databases
)AS
BEGIN
    SET NOCOUNT ON;
     SET @DBName = NULLIF(NULLIF(@DBName, ''), 'all');
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
    
    -- Determine which wait time threshold to use based on whether a recent alert has been sent
    DECLARE @CurrentWaitThreshold INT = @WaitTimeThresholdSec;
    DECLARE @LastEmailTime DATETIME = NULL;
    
    IF @FollowupIntervalMins > 0
    BEGIN
        SELECT TOP 1 @LastEmailTime = send_request_date
        FROM msdb.dbo.sysmail_allitems
        WHERE subject LIKE 'SQL Blocking Alert on ' + @@SERVERNAME + '%'
          AND sent_status = 'sent' -- Successfully sent
        ORDER BY send_request_date DESC;
        
        -- If a previous email was sent recently, use the higher follow-up threshold
        IF @LastEmailTime IS NOT NULL
        BEGIN
            SET @CurrentWaitThreshold = @FollowupWaitTimeThresholdSec;
        END
    END
    
    -- Create a temp table to store blocking information
    IF OBJECT_ID('tempdb..#BlockingData') IS NOT NULL
        DROP TABLE #BlockingData;
    
    CREATE TABLE #BlockingData (
        session_id INT,
        blocking_session_id INT,
        wait_type NVARCHAR(60),
        wait_duration_sec INT,
        wait_resource NVARCHAR(256),
        status NVARCHAR(30),
        login_name NVARCHAR(128),
        host_name NVARCHAR(128),
        program_name NVARCHAR(128),
        database_name NVARCHAR(128),
        command NVARCHAR(32),
        sql_text NVARCHAR(MAX),
        blocked_session_count INT,
        open_transaction_count INT
    );
    
    -- Get the head blocker information by analyzing blocking chains
    WITH BlockingHierarchy AS (
        -- Find sessions involved in blocking
        SELECT 
            s.session_id,
            ISNULL(r.blocking_session_id, 0) AS blocking_session_id,
            s.login_name,
            s.host_name,
            s.program_name,
            DB_NAME(r.database_id) AS database_name,
            r.command,
            r.wait_type,
            CASE 
                WHEN r.wait_time < 0 THEN 0
                ELSE r.wait_time / 1000 -- Convert to seconds
            END AS wait_duration_sec,
            r.wait_resource,
            s.status,
            -- Get the SQL text if available
            ISNULL((SELECT text FROM sys.dm_exec_sql_text(r.sql_handle)), '') AS sql_text,
            -- Get transaction count
            s.open_transaction_count AS open_transaction_count
        FROM sys.dm_exec_sessions s
        LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
        WHERE s.session_id <> @@SPID -- Exclude our own session
          AND (r.blocking_session_id > 0 OR s.session_id IN (SELECT blocking_session_id FROM sys.dm_exec_requests WHERE blocking_session_id > 0))
		  AND (@DBName IS NULL OR DB_NAME(r.database_id) = @DBName) 
    ),
    BlockedSessionCount AS (
        -- Count the number of sessions each session is blocking
        SELECT 
            blocking_session_id,
            COUNT(*) AS blocked_count
        FROM sys.dm_exec_requests
        WHERE blocking_session_id > 0
		 AND (@DBName IS NULL OR DB_NAME(database_id) = @DBName) 
        GROUP BY blocking_session_id
    )
    -- Populate the temp table with detailed blocking information
    INSERT INTO #BlockingData
    SELECT 
        bh.session_id,
        bh.blocking_session_id,
        bh.wait_type,
        bh.wait_duration_sec,
        bh.wait_resource,
        bh.status,
        bh.login_name,
        bh.host_name,
        bh.program_name,
        bh.database_name,
        bh.command,
        bh.sql_text,
        ISNULL(bsc.blocked_count, 0) AS blocked_session_count,
        bh.open_transaction_count
    FROM BlockingHierarchy bh
    LEFT JOIN BlockedSessionCount bsc ON bh.session_id = bsc.blocking_session_id
    WHERE 
		
        -- Either this session is being blocked
        bh.blocking_session_id > 0
        OR 
        -- Or this session is blocking others
        EXISTS (SELECT 1 FROM BlockedSessionCount WHERE blocking_session_id = bh.session_id);
    
    -- Filter blocked sessions by wait time threshold if specified
-- Filter blocked sessions by wait time threshold if specified
-- Filter blocked sessions by wait time threshold if specified
IF @CurrentWaitThreshold > 0
BEGIN
    -- First, delete blocked sessions that don't meet the wait time threshold
    DELETE FROM #BlockingData
    WHERE blocking_session_id > 0
      AND (wait_duration_sec IS NULL OR wait_duration_sec < @CurrentWaitThreshold);
    
    -- Now update the blocked_session_count for all sessions based on what's remaining
    -- This ensures the count is accurate after filtering
    UPDATE b
    SET blocked_session_count = ISNULL(c.actual_count, 0)
    FROM #BlockingData b
    LEFT JOIN (
        SELECT blocking_session_id, COUNT(*) AS actual_count
        FROM #BlockingData
        WHERE blocking_session_id > 0
        GROUP BY blocking_session_id
    ) c ON b.session_id = c.blocking_session_id;
    
    -- Delete lead blockers that no longer have any sessions blocked
    -- after applying the wait time threshold
    DELETE FROM #BlockingData
    WHERE blocking_session_id = 0 
      AND blocked_session_count < @MinBlockedSessions;
END

    -- Also filter lead blockers that don't block enough sessions
    DELETE FROM #BlockingData
    WHERE blocking_session_id = 0 
      AND blocked_session_count < @MinBlockedSessions;
    
    -- Get the number of blocked sessions
    DECLARE @BlockCount INT;
    SELECT @BlockCount = COUNT(*) FROM #BlockingData;
    
    -- Get lead blocker and blocked session counts
    DECLARE @LeadBlockerCount INT = 0;
    DECLARE @BlockedSessionCount INT = 0;
    DECLARE @MostAffectedDB VARCHAR(128) = 'Unknown';
    DECLARE @LongestWaitTime INT = 0;
    
    -- Get summary statistics
    SELECT @LeadBlockerCount = COUNT(*) 
    FROM #BlockingData 
    WHERE blocking_session_id = 0 AND blocked_session_count > 0;
    
    SELECT @BlockedSessionCount = COUNT(*) 
    FROM #BlockingData 
    WHERE blocking_session_id > 0;
    
    SELECT @LongestWaitTime = MAX(wait_duration_sec)
    FROM #BlockingData;
    
    SELECT TOP 1 @MostAffectedDB = database_name
    FROM #BlockingData
    WHERE database_name IS NOT NULL
    GROUP BY database_name
    ORDER BY COUNT(*) DESC;
    
    -- Output monitoring parameters
    PRINT 'SQL Blocking Monitor Report';
    PRINT '==========================';
    PRINT 'Server: ' + @@SERVERNAME;
    PRINT 'Date/Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
    PRINT 'Wait time threshold: ' + CAST(@CurrentWaitThreshold AS VARCHAR) + ' seconds';
    PRINT 'Follow-up interval: ' + CAST(@FollowupIntervalMins AS VARCHAR) + ' minutes';
    PRINT 'Session count: ' + CAST(@BlockCount AS VARCHAR);
    PRINT '==========================';
    
    -- If we found blocking events and not in report only mode, check if we should send an email
    IF @BlockCount > 0 AND @ReportOnly = 0
    BEGIN
        -- Check whether to send email based on follow-up interval
        DECLARE @SendEmail BIT = 1;
        
        -- Only check for last email if follow-up interval is > 0
        IF @FollowupIntervalMins > 0 AND @LastEmailTime IS NOT NULL
        BEGIN
            -- If a previous email was found and it's within the follow-up interval, don't send a new one
            IF DATEDIFF(MINUTE, @LastEmailTime, GETDATE()) < @FollowupIntervalMins
            BEGIN
                SET @SendEmail = 0;
                PRINT 'Skipping email alert - last alert was sent at ' + CONVERT(VARCHAR, @LastEmailTime, 120) + 
                      ' (less than ' + CAST(@FollowupIntervalMins AS VARCHAR) + ' minutes ago)';
            END
        END
        
        -- If we should send an email, proceed with creating and sending it
        IF @SendEmail = 1
        BEGIN
            -- Create a more detailed and informative email body
            DECLARE @TableHTML VARCHAR(MAX);
            
            -- Add header with server info and summary
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
                    .leadblocker { background-color: #FFC0CB; font-weight: bold; }
                    .summary { background-color: #E6E6FA; padding: 10px; margin-bottom: 15px; border-radius: 5px; }
                </style>
            </head>
            <body>
                <h2>SQL Server Blocking Alert: ' + @@SERVERNAME + '</h2>
                
                <div class="summary">
                    <h3>Blocking Summary:</h3>
                    <ul>
                        <li><strong>Sessions Affected:</strong> ' + CAST(@BlockCount AS VARCHAR) + '</li>
                        <li><strong>Lead Blockers:</strong> ' + CAST(@LeadBlockerCount AS VARCHAR) + '</li>
                        <li><strong>Blocked Sessions:</strong> ' + CAST(@BlockedSessionCount AS VARCHAR) + '</li>
                        <li><strong>Most Affected Database:</strong> ' + @MostAffectedDB + '</li>
                        <li><strong>Longest Wait Time:</strong> ' + 
                            ISNULL(CAST(@LongestWaitTime AS VARCHAR) + ' seconds', 'Unknown') + '</li>
                        <li><strong>Detection Time:</strong> ' + CONVERT(VARCHAR, GETDATE(), 120) + '</li>
                    </ul>
                </div>';
                
            -- Add the detailed table without blocking chains
            SET @TableHTML = @TableHTML + N'
                <h3>Detailed Blocking Information:</h3>
                <table>
                <tr>
                    <th>Session ID</th>
                    <th>Blocked By</th>
                    <th>Wait Type</th>
                    <th>Wait Seconds</th>
                    <th>Blocking Count</th>
                    <th>Login Name</th>
                    <th>Status</th>
                    <th>Open Trans</th>
                    <th>Database</th>
                    <th>Command</th>
                    <th>SQL Text</th>
                    <th>Host Name</th>
                    <th>Program</th>
                </tr>';
                
            -- Add rows to the table with highlighting for lead blockers
            DECLARE @TableRows NVARCHAR(MAX) = '';
            
            -- Use cursor to build HTML rows 
            DECLARE @row_class VARCHAR(20);
            DECLARE @session_id INT;
            DECLARE @blocking_session_id INT;
            DECLARE @wait_type NVARCHAR(60);
            DECLARE @wait_duration_sec INT;
            DECLARE @blocked_session_count INT;
            DECLARE @login_name NVARCHAR(128);
            DECLARE @status NVARCHAR(30);
            DECLARE @open_transaction_count INT;
            DECLARE @database_name NVARCHAR(128);
            DECLARE @command NVARCHAR(32);
            DECLARE @sql_text NVARCHAR(MAX);
            DECLARE @host_name NVARCHAR(128);
            DECLARE @program_name NVARCHAR(128);
            
            DECLARE block_data_cursor CURSOR FOR
            SELECT 
                session_id,
                blocking_session_id,
                wait_type,
                wait_duration_sec,
                blocked_session_count,
                login_name,
                status,
                open_transaction_count,
                database_name,
                command,
                sql_text,
                host_name,
                program_name
            FROM #BlockingData
            ORDER BY 
                CASE WHEN blocking_session_id = 0 THEN 0 ELSE 1 END, -- Lead blockers first
                blocked_session_count DESC,                           -- Most impact first
                ISNULL(wait_duration_sec, 0) DESC;                    -- Longest waits next
                
            OPEN block_data_cursor;
            FETCH NEXT FROM block_data_cursor INTO 
                @session_id, @blocking_session_id, @wait_type, @wait_duration_sec,
                @blocked_session_count, @login_name, @status, @open_transaction_count,
                @database_name, @command, @sql_text, @host_name, @program_name;
                
            WHILE @@FETCH_STATUS = 0
            BEGIN
                -- Determine row class for highlighting
                SET @row_class = 
                    CASE 
                        WHEN @blocking_session_id = 0 AND @blocked_session_count > 0 
                        THEN 'leadblocker'
                        WHEN @wait_duration_sec > @CurrentWaitThreshold * 2
                        THEN 'highlight'
                        ELSE ''
                    END;
                    
                -- Build the table row with proper HTML
                SET @TableRows = @TableRows +
                    CASE WHEN @row_class = '' 
                         THEN '<tr>'
                         ELSE '<tr class="' + @row_class + '">'
                    END +
                    '<td>' + CAST(@session_id AS VARCHAR) + '</td>' +
                    '<td>' + CASE
                              WHEN @blocking_session_id = 0 THEN 'Lead Blocker'
                              ELSE CAST(@blocking_session_id AS VARCHAR)
                            END + '</td>' +
                    '<td>' + ISNULL(@wait_type, 'N/A') + '</td>' +
                    '<td>' + ISNULL(CAST(@wait_duration_sec AS VARCHAR), 'N/A') + '</td>' +
                    '<td>' + CAST(@blocked_session_count AS VARCHAR) + '</td>' +
                    '<td>' + ISNULL(@login_name, 'N/A') + '</td>' +
                    '<td>' + ISNULL(@status, 'N/A') + '</td>' +
                    '<td>' + ISNULL(CAST(@open_transaction_count AS VARCHAR), 'N/A') + '</td>' +
                    '<td>' + ISNULL(@database_name, 'N/A') + '</td>' +
                    '<td>' + ISNULL(@command, 'N/A') + '</td>' +
                    '<td>' + ISNULL(LEFT(@sql_text, 100) + CASE WHEN LEN(@sql_text) > 100 THEN '...' ELSE '' END, 'N/A') + '</td>' +
                    '<td>' + ISNULL(@host_name, 'N/A') + '</td>' +
                    '<td>' + ISNULL(@program_name, 'N/A') + '</td>' +
                    '</tr>';
                    
                FETCH NEXT FROM block_data_cursor INTO 
                    @session_id, @blocking_session_id, @wait_type, @wait_duration_sec,
                    @blocked_session_count, @login_name, @status, @open_transaction_count,
                    @database_name, @command, @sql_text, @host_name, @program_name;
            END
            
            CLOSE block_data_cursor;
            DEALLOCATE block_data_cursor;
                
            -- Close the table
            SET @TableHTML = @TableHTML + @TableRows + N'
                </table>
                
                <h3>Monitoring Parameters:</h3>
                <ul>
                    <li><strong>Current Wait Threshold:</strong> ' + CAST(@CurrentWaitThreshold AS VARCHAR) + ' seconds</li>
                    <li><strong>Initial Wait Threshold:</strong> ' + CAST(@WaitTimeThresholdSec AS VARCHAR) + ' seconds</li>
                    <li><strong>Follow-up Wait Threshold:</strong> ' + CAST(@FollowupWaitTimeThresholdSec AS VARCHAR) + ' seconds</li>
                    <li><strong>Follow-up Interval:</strong> ' + CAST(@FollowupIntervalMins AS VARCHAR) + ' minutes</li>
                    <li><strong>Min Blocked Session Count:</strong> ' + CAST(@MinBlockedSessions AS VARCHAR) + '</li>
                </ul>
                
                <p style="font-size: 10pt; color: #666666;">
                    This is an automated message from SQL Server blocking monitoring system.<br/>
                    Generated on ' + CONVERT(VARCHAR, GETDATE(), 120) + ' for server ' + @@SERVERNAME + '
                </p>
            </body>
            </html>';
            
            DECLARE @Subject VARCHAR(255) = 'SQL Blocking Alert on ' + @@SERVERNAME + 
                                         ' - ' + CAST(@BlockCount AS VARCHAR) + ' sessions affected' +
                                         ' (' + CAST(@LeadBlockerCount AS VARCHAR) + ' blockers, ' + 
                                         CAST(@BlockedSessionCount AS VARCHAR) + ' blocked)';
            
            EXEC msdb.dbo.sp_send_dbmail
                @recipients = @EmailRecipients,
                @profile_name = @EmailProfile,
                @subject = @Subject,
                @body = @TableHTML,
                @body_format = 'HTML';
                
            PRINT 'Email sent to: ' + @EmailRecipients;
        END
        ELSE
        BEGIN
            PRINT 'Email alert suppressed due to follow-up interval setting.';
        END
    END;
    
    -- Return the result set
    SELECT 
        session_id,
        blocking_session_id,
        wait_type,
        wait_duration_sec,
        wait_resource,
        blocked_session_count,
        login_name,
        host_name,
        program_name,
        database_name,
        command,
        LEFT(sql_text, 100) AS sql_text_excerpt,
        status,
        open_transaction_count
    FROM #BlockingData
    ORDER BY 
        CASE WHEN blocking_session_id = 0 THEN 0 ELSE 1 END, -- Lead blockers first
        blocked_session_count DESC,                          -- Most impact first
        ISNULL(wait_duration_sec, 0) DESC;                   -- Longest waits next
        
    -- Clean up temp table
    IF OBJECT_ID('tempdb..#BlockingData') IS NOT NULL
        DROP TABLE #BlockingData;
END;
GO
