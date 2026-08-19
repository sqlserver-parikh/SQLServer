-- ============================================================================
-- THE COMPLETE LOGIN & DATABASE AUDIT SOLUTION
-- Target Database: dbasupport
-- ============================================================================

-- ============================================================================
-- STEP 1: CREATE THE EXTENDED EVENTS SESSION (Server Level)
-- ============================================================================
USE master;
GO

IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'xe_LoginActivity')
BEGIN
    DROP EVENT SESSION [xe_LoginActivity] ON SERVER;
END
GO

CREATE EVENT SESSION [xe_LoginActivity] ON SERVER 
-- Capture ONLY Successful Logins (Failures remain in the SQL Error Log)
ADD EVENT sqlserver.login(
    ACTION(
        sqlserver.client_app_name,
        sqlserver.client_hostname,
        sqlserver.database_name,
        sqlserver.nt_username,
        sqlserver.username
    )
    -- Exclude system processes to reduce noise
    WHERE (sqlserver.is_system = 0)
)
ADD TARGET package0.event_file(
    -- Omitting the file path forces it to the default SQL Server LOG folder
    SET filename = N'xe_LoginActivity.xel', 
        max_file_size = 25,      -- 25 MB per file
        max_rollover_files = 20  -- Retain up to 10 files (250MB total buffer)
)
WITH (
    MAX_MEMORY = 4096 KB, 
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MAX_DISPATCH_LATENCY = 30 SECONDS,
    MAX_EVENT_SIZE = 0 KB,
    MEMORY_PARTITION_MODE = NONE,
    TRACK_CAUSALITY = OFF,
    STARTUP_STATE = ON -- Starts automatically if SQL Server restarts
);
GO

-- Start the session immediately
ALTER EVENT SESSION [xe_LoginActivity] ON SERVER STATE = START;
GO


-- ============================================================================
-- STEP 2: SET DATABASE CONTEXT & CREATE THE TARGET TABLE
-- ============================================================================
USE dbasupport; 
GO

IF OBJECT_ID('dbo.tblLoginAudit', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.tblLoginAudit (
        AuditID             BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoginName           NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        HostName            NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        ApplicationName     NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        DatabaseName        NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        SessionLoginName    NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        NTUserName          NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        Status              VARCHAR(20) NOT NULL, -- 'Success'
        PrincipalID         INT,
        SID                 VARBINARY(85), 
        TypeDesc            NVARCHAR(128) COLLATE Latin1_General_CI_AS_KS_WS,
        FirstSeen           DATETIME,
        LastSeen            DATETIME,
        EventCount          BIGINT,
        RunTimeUTC          DATETIME DEFAULT GETUTCDATE()
    ) WITH (DATA_COMPRESSION = PAGE);
    
    CREATE UNIQUE NONCLUSTERED INDEX UX_LoginAudit_Merge 
        ON dbo.tblLoginAudit (LoginName, HostName, ApplicationName, DatabaseName, Status, SID)
        WITH (DATA_COMPRESSION = PAGE);
END
GO


-- ============================================================================
-- STEP 3: CREATE THE STORED PROCEDURE
-- ============================================================================
IF OBJECT_ID('dbo.usp_LoadLoginAuditXE', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.usp_LoadLoginAuditXE;
GO

CREATE PROCEDURE dbo.usp_LoadLoginAuditXE
(
    @RetentionDays INT = 365  -- Automatically purge records older than this
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        ---------------------------------------------------------------------------
        -- A. Determine High-Water Mark for Incremental Load
        ---------------------------------------------------------------------------
        DECLARE @MaxLastUsed DATETIME;
        SELECT @MaxLastUsed = ISNULL(MAX(LastSeen), '2000-01-01') 
        FROM dbo.tblLoginAudit WITH (NOLOCK);

        ---------------------------------------------------------------------------
        -- B. Extract and Shred XE Data into Temp Table
        ---------------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#XELoginData') IS NOT NULL DROP TABLE #XELoginData;

        SELECT
            -- Convert XE UTC timestamp to local server time
            DATEADD(MINUTE, DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()), 
                x.event_data.value('(event/@timestamp)[1]', 'DATETIME2')) AS StartTime,
            x.event_data.value('(event/action[@name="username"]/value)[1]', 'NVARCHAR(128)') AS LoginName,
            x.event_data.value('(event/action[@name="client_hostname"]/value)[1]', 'NVARCHAR(128)') AS HostName,
            x.event_data.value('(event/action[@name="client_app_name"]/value)[1]', 'NVARCHAR(128)') AS ApplicationName,
            x.event_data.value('(event/action[@name="database_name"]/value)[1]', 'NVARCHAR(128)') AS DatabaseName,
            x.event_data.value('(event/action[@name="nt_username"]/value)[1]', 'NVARCHAR(128)') AS NTUserName
        INTO #XELoginData
        FROM (
            -- Read from the rollover files in the default LOG directory
            SELECT CAST(event_data AS XML) AS event_data
            FROM sys.fn_xe_file_target_read_file('xe_LoginActivity*.xel', NULL, NULL, NULL)
        ) AS x
        WHERE 
            -- Only process events newer than what we already have in the table
            DATEADD(MINUTE, DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()), 
                x.event_data.value('(event/@timestamp)[1]', 'DATETIME2')) > @MaxLastUsed;

        ---------------------------------------------------------------------------
        -- C. Aggregate and MERGE into dbo.tblLoginAudit
        ---------------------------------------------------------------------------
        MERGE dbo.tblLoginAudit AS target
        USING (
            SELECT 
                CAST(I.LoginName AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS LoginName,
                CAST(ISNULL(I.HostName, 'Unknown') AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS HostName,
                CAST(ISNULL(I.ApplicationName, 'Unknown') AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS ApplicationName,
                CAST(ISNULL(I.DatabaseName, '') AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS DatabaseName,
                'Success' AS Status,
                CAST(S.sid AS VARBINARY(85)) AS sid,
                MAX(CAST(I.NTUserName AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS) AS NTUserName,
                MAX(CAST(I.LoginName AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS) AS SessionLoginName,
                MIN(I.StartTime) AS FirstSeen,
                MAX(I.StartTime) AS LastSeen,
                COUNT(*) AS EventCount,
                MAX(S.principal_id) AS principal_id,
                MAX(CAST(S.type_desc AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS) AS type_desc
            FROM #XELoginData I
            LEFT JOIN sys.server_principals S ON I.LoginName COLLATE DATABASE_DEFAULT = S.name COLLATE DATABASE_DEFAULT
            WHERE I.LoginName IS NOT NULL 
            GROUP BY 
                I.LoginName, 
                I.HostName, 
                I.ApplicationName, 
                I.DatabaseName, 
                S.sid
        ) AS source
        ON (
            target.LoginName = source.LoginName
            AND target.HostName = source.HostName
            AND target.ApplicationName = source.ApplicationName
            AND target.DatabaseName = source.DatabaseName
            AND target.Status = source.Status
            AND ISNULL(target.SID, 0x00) = ISNULL(source.sid, 0x00)
        )
        WHEN MATCHED THEN 
            UPDATE SET 
                target.LastSeen = CASE WHEN source.LastSeen > target.LastSeen THEN source.LastSeen ELSE target.LastSeen END,
                target.EventCount = target.EventCount + source.EventCount,
                target.RunTimeUTC = GETUTCDATE(),
                target.NTUserName = ISNULL(source.NTUserName, target.NTUserName),
                target.SessionLoginName = ISNULL(source.SessionLoginName, target.SessionLoginName)
        WHEN NOT MATCHED THEN 
            INSERT (
                NTUserName, LoginName, HostName, ApplicationName, SessionLoginName, 
                DatabaseName, Status, FirstSeen, LastSeen, EventCount, PrincipalID, SID, TypeDesc
            )
            VALUES (
                source.NTUserName, source.LoginName, source.HostName, source.ApplicationName, source.SessionLoginName, 
                source.DatabaseName, source.Status, source.FirstSeen, source.LastSeen, source.EventCount, source.principal_id, source.sid, source.type_desc
            );

        ---------------------------------------------------------------------------
        -- D. Apply Retention Cleanup
        ---------------------------------------------------------------------------
        IF @RetentionDays > 0
        BEGIN
            DECLARE @CutoffDate DATETIME = DATEADD(DAY, -@RetentionDays, GETDATE());
            DELETE FROM dbo.tblLoginAudit WHERE LastSeen < @CutoffDate;
        END

        IF OBJECT_ID('tempdb..#XELoginData') IS NOT NULL DROP TABLE #XELoginData;

    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#XELoginData') IS NOT NULL DROP TABLE #XELoginData;
        DECLARE @Msg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Msg, 16, 1);
    END CATCH
END
GO


-- ============================================================================
-- STEP 4: CREATE THE LOGIN LIFECYCLE METRICS VIEW
-- ============================================================================
IF OBJECT_ID('dbo.vw_LoginUsageSummary', 'V') IS NOT NULL
    DROP VIEW dbo.vw_LoginUsageSummary;
GO

CREATE VIEW dbo.vw_LoginUsageSummary
AS
SELECT 
    sp.name AS LoginName,
    sp.type_desc AS LoginType,
    sp.is_disabled AS IsDisabled,
    sp.create_date AS AccountCreatedDate,
    sp.modify_date AS AccountModifiedDate,
    
    -- Activity tracking from XE table
    MIN(la.FirstSeen) AS FirstSeenLocal,
    MAX(la.LastSeen) AS LastSeenLocal,
    ISNULL(SUM(la.EventCount), 0) AS TotalLogins,
    
    -- Granular Status Classification
    CASE 
        WHEN sp.is_disabled = 1 THEN 'Disabled'
        WHEN MAX(la.LastSeen) IS NULL THEN 'Never Logged In (Since Audit Started)'
        WHEN MAX(la.LastSeen) < DATEADD(DAY, -90, GETDATE()) THEN 'Stale (Inactive > 90 Days)'
        ELSE 'Active'
    END AS UsageStatus
FROM sys.server_principals sp
LEFT JOIN dbo.tblLoginAudit la 
    ON sp.sid = la.SID
WHERE 
    sp.type IN ('S', 'U')                  -- ONLY SQL Logins and Individual Windows Users (Excludes Groups)
    AND sp.name NOT LIKE '##%'             -- Exclude internal system principals
    AND sp.name NOT LIKE 'NT SERVICE\%'    -- Exclude service accounts
    AND sp.name NOT LIKE 'NT AUTHORITY\%'  -- Exclude built-in OS system accounts
GROUP BY 
    sp.name, 
    sp.type_desc, 
    sp.is_disabled, 
    sp.create_date, 
    sp.modify_date;
GO


-- ============================================================================
-- STEP 5: CREATE THE DATABASE LIFECYCLE METRICS VIEW
-- ============================================================================
IF OBJECT_ID('dbo.vw_DatabaseUsageSummary', 'V') IS NOT NULL
    DROP VIEW dbo.vw_DatabaseUsageSummary;
GO

CREATE VIEW dbo.vw_DatabaseUsageSummary
AS
SELECT 
    d.name AS DatabaseName,
    d.state_desc AS DatabaseState,
    d.create_date AS DatabaseCreatedDate,
    
    -- Connection tracking from XE table
    MIN(la.FirstSeen) AS FirstAppConnectionLocal,
    MAX(la.LastSeen) AS LastAppConnectionLocal,
    ISNULL(SUM(la.EventCount), 0) AS TotalAppConnections,
    
    -- Granular Status Classification
    CASE 
        WHEN d.state_desc <> 'ONLINE' THEN d.state_desc
        WHEN MAX(la.LastSeen) IS NULL THEN 'Never Connected (Since Audit Started)'
        WHEN MAX(la.LastSeen) < DATEADD(DAY, -90, GETDATE()) THEN 'Stale (Inactive > 90 Days)'
        ELSE 'Active'
    END AS UsageStatus
FROM sys.databases d
LEFT JOIN dbo.tblLoginAudit la 
    -- COLLATE handles the clash between system DB defaults and the table schema
    ON d.name COLLATE DATABASE_DEFAULT = la.DatabaseName COLLATE DATABASE_DEFAULT
    -- Optional: Exclude automated admin logins to see pure app connections
    -- AND la.LoginName NOT IN ('DOMAIN\BackupService', 'DOMAIN\MonitoringTool', 'sa')
WHERE 
    d.database_id > 4 -- Exclude system databases
GROUP BY 
    d.name, 
    d.state_desc, 
    d.create_date;
GO
