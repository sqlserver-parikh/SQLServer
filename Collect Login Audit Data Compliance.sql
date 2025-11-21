USE tempdb
GO
-- ============================================================================
-- 1. CLEANUP / RESET 
-- ============================================================================
IF OBJECT_ID('dbo.tblAuditDefaultTrace', 'U') IS NOT NULL 
    DROP TABLE dbo.tblAuditDefaultTrace;
GO
IF OBJECT_ID('dbo.tblAuditLoginStats', 'U') IS NOT NULL 
    DROP TABLE dbo.tblAuditLoginStats;
GO
IF OBJECT_ID('dbo.tblLoginAudit', 'U') IS NOT NULL 
    DROP TABLE dbo.tblLoginAudit;
GO
IF OBJECT_ID('dbo.usp_AuditDefaultTrace', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.usp_AuditDefaultTrace;
GO
IF OBJECT_ID('dbo.usp_Audit_CaptureLog', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.usp_Audit_CaptureLog; 
GO
IF OBJECT_ID('dbo.usp_LoginAudit', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.usp_LoginAudit; 
GO

-- ============================================================================
-- 2. CREATE CONSOLIDATED PROCEDURE
-- ============================================================================
CREATE PROCEDURE [dbo].[usp_AuditDefaultTrace]
(
    @LogToTable        BIT  = 1,        -- 1 = Log to tables, 0 = Display Only
    @RetentionDays     INT  = 90,       -- Retention for Detail Logs
    @IncludeTextData   BIT  = 1,        -- Keep SQL text for DDL events
    @IncludeSystem     BIT  = 0,        -- 0 = Exclude system DBs (master, msdb)
    @TrackLoginStats   BIT  = 1,        -- 1 = Maintain the Login Statistics table
    @MinStartTime      DATETIME = NULL  -- Force load from specific date
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

        ---------------------------------------------------------------------------
        -- 1. Resolve Default Trace Path
        ---------------------------------------------------------------------------
        DECLARE @TracePath NVARCHAR(500);
        SELECT TOP (1) @TracePath = CAST(path AS NVARCHAR(500))
        FROM sys.traces
        WHERE is_default = 1 AND status = 1;

        IF @TracePath IS NULL
        BEGIN
            RAISERROR('Default Trace is not enabled/active on this server.', 16, 1);
            RETURN;
        END

        ---------------------------------------------------------------------------
        -- 2. Define System Databases List
        ---------------------------------------------------------------------------
        DECLARE @SystemDBs TABLE (Name NVARCHAR(128) PRIMARY KEY);
        INSERT INTO @SystemDBs (Name) VALUES 
        ('master'), ('model'), ('msdb'), ('tempdb'), ('distribution'), 
        ('ReportServer'), ('ReportServerTempDB'), ('SSISDB');

        ---------------------------------------------------------------------------
        -- 3. Define Object Type Mapping
        ---------------------------------------------------------------------------
        DECLARE @ObjectTypeMap TABLE (ID INT PRIMARY KEY, Description VARCHAR(100));
        INSERT INTO @ObjectTypeMap (ID, Description) VALUES
        (8259,'Check Constraint'),(8260,'Default'),(8262,'Foreign-key'),(8272,'Stored Procedure'),
        (8274,'Rule'),(8275,'System Table'),(8276,'Trigger (Server)'),(8277,'Table (User)'),
        (8278,'View'),(8280,'Extended SP'),(16724,'CLR Trigger'),(16964,'Database'),
        (16975,'Object'),(17222,'FullText Catalog'),(17232,'CLR SP'),(17235,'Schema'),
        (17475,'Credential'),(17491,'DDL Event'),(17741,'Management Event'),(17747,'Security Event'),
        (17749,'User Event'),(17985,'CLR Agg Func'),(17993,'Inline TVF'),(18000,'Partition Func'),
        (18002,'Rep Filter Proc'),(18004,'Table Valued Func'),(18259,'Server Role'),(18263,'Windows Group'),
        (19265,'Asymmetric Key'),(19277,'Master Key'),(19280,'Primary Key'),(19283,'ObfusKey'),
        (19521,'Asymmetric Key Login'),(19523,'Cert Login'),(19538,'Role'),(19539,'SQL Login'),
        (19543,'Windows Login'),(20034,'Remote Svc Binding'),(20036,'Event Notif (DB)'),(20037,'Event Notif'),
        (20038,'Scalar SQL Func'),(20047,'Event Notif (Obj)'),(20051,'Synonym'),(20549,'End Point'),
        (20801,'Adhoc Query'),(20816,'Prepared Query'),(20819,'Service Broker Queue'),(20821,'Unique Constraint'),
        (21057,'App Role'),(21059,'Certificate'),(21075,'Server'),(21076,'TSQL Trigger'),
        (21313,'Assembly'),(21318,'CLR Scalar Func'),(21321,'Inline Scalar Func'),(21328,'Partition Scheme'),
        (21333,'User'),(21571,'SB Contract'),(21572,'Trigger (DB)'),(21574,'CLR TVF'),
        (21577,'Internal Table'),(21581,'SB Msg Type'),(21586,'SB Route'),(21587, 'Statistics'),
        (22099,'SB Service'),(22601,'Index'),(22604,'Cert Login'),(22611,'XMLSchema'),(22868,'Type');

        ---------------------------------------------------------------------------
        -- 4. Table Setup (WITH PAGE COMPRESSION)
        ---------------------------------------------------------------------------
        IF @LogToTable = 1
        BEGIN
            -- TABLE A: Detailed Events
            IF OBJECT_ID('dbo.tblAuditDefaultTrace', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.tblAuditDefaultTrace
                (
                    LogID              BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    EventTime          DATETIME  NOT NULL,
                    EventCategory      VARCHAR(50) NULL,      
                    EventName          NVARCHAR(128) NULL,    
                    EventSubClassName  NVARCHAR(128) NULL,    
                    ObjectType         NVARCHAR(128) NULL,
                    DatabaseName       NVARCHAR(128) NULL,
                    ObjectName         NVARCHAR(256) NULL,
                    HostName           NVARCHAR(128) NULL,
                    ApplicationName    NVARCHAR(256) NULL,
                    LoginName          NVARCHAR(128) NULL,
                    TargetLoginName    NVARCHAR(128) NULL,
                    SPID               INT NULL,
                    TextData           NVARCHAR(MAX) NULL,    
                    RowHash            VARBINARY(32) NULL,    
                    CapturedAt         DATETIME NOT NULL DEFAULT (GETDATE())
                ) WITH (DATA_COMPRESSION = PAGE);

                CREATE NONCLUSTERED INDEX IX_AuditTrace_Time 
                    ON dbo.tblAuditDefaultTrace (EventTime DESC) 
                    INCLUDE (EventCategory, EventName, DatabaseName, LoginName)
                    WITH (DATA_COMPRESSION = PAGE);
                
                EXEC sp_executesql N'CREATE UNIQUE NONCLUSTERED INDEX UX_AuditTrace_RowHash ON dbo.tblAuditDefaultTrace (RowHash) WHERE RowHash IS NOT NULL WITH (DATA_COMPRESSION = PAGE);';
            END

            -- TABLE B: Basic Login Stats (Success & Fail)
            IF OBJECT_ID('dbo.tblAuditLoginStats', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.tblAuditLoginStats
                (
                    StatID             BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    LoginName          NVARCHAR(128),
                    HostName           NVARCHAR(128),
                    DatabaseName       NVARCHAR(128),
                    ApplicationName    NVARCHAR(256),
                    Status             VARCHAR(20), -- 'Success' or 'Failed'
                    FirstSeenTime      DATETIME,
                    LastSeenTime       DATETIME,
                    EventCount         BIGINT DEFAULT 1,
                    LastUpdated        DATETIME DEFAULT GETDATE()
                ) WITH (DATA_COMPRESSION = PAGE);

                CREATE UNIQUE NONCLUSTERED INDEX UX_LoginStats_Keys 
                    ON dbo.tblAuditLoginStats (LoginName, HostName, DatabaseName, ApplicationName, Status)
                    WITH (DATA_COMPRESSION = PAGE);
            END

            -- TABLE C: Detailed Principal Audit
            IF OBJECT_ID('dbo.tblLoginAudit', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.tblLoginAudit (
                    NTUserName NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    loginname NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    HostName NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    ApplicationName NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    SessionLoginName NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    databasename NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    first_used DATETIME,
                    last_used DATETIME,
                    usage_count BIGINT,
                    principal_id INT,
                    sid VARBINARY(MAX),
                    type_desc NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    name NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    RunTimeUTC DATETIME DEFAULT GETUTCDATE()
                ) WITH (DATA_COMPRESSION = PAGE);
                
                CREATE NONCLUSTERED INDEX IX_tblLoginAudit_Lookup 
                    ON dbo.tblLoginAudit (loginname, HostName, ApplicationName)
                    WITH (DATA_COMPRESSION = PAGE);
            END
            
            -- Schema Recovery for existing tables
            IF COL_LENGTH('dbo.tblAuditDefaultTrace', 'RowHash') IS NULL
            BEGIN
                ALTER TABLE dbo.tblAuditDefaultTrace ADD RowHash VARBINARY(32) NULL;
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_AuditTrace_RowHash' AND object_id = OBJECT_ID('dbo.tblAuditDefaultTrace'))
                BEGIN
                     EXEC sp_executesql N'CREATE UNIQUE NONCLUSTERED INDEX UX_AuditTrace_RowHash ON dbo.tblAuditDefaultTrace (RowHash) WHERE RowHash IS NOT NULL WITH (DATA_COMPRESSION = PAGE);';
                END
            END
        END

        ---------------------------------------------------------------------------
        -- 5. Load Trace Data into Temp Table
        ---------------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#TraceRows') IS NOT NULL DROP TABLE #TraceRows;

        SELECT
            T.StartTime,
            TE.name AS EventName,
            T.EventClass,
            SV.subclass_name AS EventSubClassName,
            CASE
                WHEN TE.name LIKE 'Audit Add%' OR TE.name LIKE 'Audit Login%' THEN 'Security'
                WHEN TE.name LIKE 'Object:%' THEN 'DDL'
                ELSE 'Other'
            END AS EventCategory,
            ISNULL(OT.Description, 'Type:' + CAST(T.ObjectType AS VARCHAR(20))) AS ObjectType,
            T.DatabaseName,
            T.ObjectName,
            T.HostName,
            T.ApplicationName,
            T.LoginName,
            COALESCE(T.TargetUserName, T.TargetLoginName, T.SessionLoginName) AS TargetLoginName,
            T.SPID,
            CAST(T.TextData AS NVARCHAR(MAX)) AS TextData,
            T.LoginSid,
            T.NTUserName,
            T.SessionLoginName
        INTO #TraceRows
        FROM sys.fn_trace_gettable(@TracePath, DEFAULT) AS T
        INNER JOIN sys.trace_events AS TE ON T.EventClass = TE.trace_event_id
        LEFT JOIN sys.trace_subclass_values AS SV ON T.EventClass = SV.trace_event_id AND T.EventSubClass = SV.subclass_value
        LEFT JOIN @ObjectTypeMap AS OT ON T.ObjectType = OT.ID
        WHERE
            (@MinStartTime IS NULL OR T.StartTime >= @MinStartTime)
            AND (
                -- Filter Logic:
                -- 1. Always include Login Events (Class 14, 20) regardless of DB Name
                (TE.name LIKE '%Login%')
                OR 
                -- 2. For other events, apply the System DB filter
                (
                    @IncludeSystem = 1 
                    OR T.DatabaseName IS NULL 
                    OR T.DatabaseName NOT IN (SELECT Name FROM @SystemDBs)
                )
            )
            AND (T.ObjectName NOT LIKE '_WA_Sys_%' OR T.ObjectName IS NULL);

        ---------------------------------------------------------------------------
        -- 6. Mode: Display Only
        ---------------------------------------------------------------------------
        IF @LogToTable = 0 
        BEGIN
            SELECT * FROM #TraceRows ORDER BY StartTime DESC;
            RETURN;
        END

        ---------------------------------------------------------------------------
        -- 7. Process Detailed Events (DDL & Security Changes)
        ---------------------------------------------------------------------------
        DECLARE @LastMaxDate DATETIME;
        SELECT @LastMaxDate = MAX(EventTime) FROM dbo.tblAuditDefaultTrace;
        SET @LastMaxDate = ISNULL(@LastMaxDate, '1900-01-01');

        INSERT INTO dbo.tblAuditDefaultTrace
        (
            EventTime, EventCategory, EventName, EventSubClassName, ObjectType, 
            DatabaseName, ObjectName, HostName, ApplicationName, LoginName, 
            TargetLoginName, SPID, TextData, RowHash
        )
        SELECT DISTINCT
            src.StartTime, src.EventCategory, src.EventName, src.EventSubClassName, src.ObjectType, 
            src.DatabaseName, src.ObjectName, src.HostName, src.ApplicationName, src.LoginName, 
            src.TargetLoginName, src.SPID, 
            CASE WHEN @IncludeTextData=1 THEN src.TextData ELSE NULL END, 
            HashVal.RowHash
        FROM #TraceRows AS src
        CROSS APPLY (
            SELECT HASHBYTES('SHA2_256', CONCAT(
                CONVERT(VARCHAR(33), src.StartTime, 126), '|',
                ISNULL(src.EventName,'') , '|',
                ISNULL(src.EventSubClassName,'') , '|',
                ISNULL(src.DatabaseName,'') , '|',
                ISNULL(src.ObjectName,'') , '|',
                ISNULL(src.LoginName,'') , '|',
                ISNULL(src.HostName,'') , '|',
                ISNULL(SUBSTRING(src.TextData, 1, 4000),'') 
            )) AS RowHash
        ) AS HashVal
        WHERE src.StartTime > @LastMaxDate
          AND (
             (src.EventName LIKE 'Object:%' AND src.EventSubClassName = 'Commit')
             OR (src.EventName LIKE 'Audit%' AND src.EventName NOT LIKE 'Audit Login%') 
          )
          AND NOT EXISTS (SELECT 1 FROM dbo.tblAuditDefaultTrace T WHERE T.RowHash = HashVal.RowHash);

        ---------------------------------------------------------------------------
        -- 8. Process Basic Login Stats (Success & Fail)
        ---------------------------------------------------------------------------
        -- This looks at ALL events in the trace to determine "Success" presence
        IF @TrackLoginStats = 1
        BEGIN
            ;WITH CurrentTraceLogins AS (
                SELECT 
                    LoginName,
                    ISNULL(HostName, 'Unknown') AS HostName,
                    ISNULL(DatabaseName, 'Unknown') AS DatabaseName,
                    ISNULL(ApplicationName, 'Unknown') AS ApplicationName,
                    CASE 
                        WHEN EventName LIKE '%Failed%' THEN 'Failed' 
                        ELSE 'Success' 
                    END AS Status,
                    MIN(StartTime) AS SessionFirst,
                    MAX(StartTime) AS SessionLast,
                    COUNT(*) AS SessionCount
                FROM #TraceRows 
                WHERE LoginName IS NOT NULL 
                GROUP BY LoginName, HostName, DatabaseName, ApplicationName,
                         CASE WHEN EventName LIKE '%Failed%' THEN 'Failed' ELSE 'Success' END
            )
            MERGE dbo.tblAuditLoginStats AS Target
            USING CurrentTraceLogins AS Source
            ON (
                ISNULL(Target.LoginName,'') = ISNULL(Source.LoginName,'') AND
                ISNULL(Target.HostName,'') = ISNULL(Source.HostName,'') AND
                ISNULL(Target.DatabaseName,'') = ISNULL(Source.DatabaseName,'') AND
                ISNULL(Target.ApplicationName,'') = ISNULL(Source.ApplicationName,'') AND
                Target.Status = Source.Status
            )
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.LastSeenTime = CASE WHEN Source.SessionLast > Target.LastSeenTime THEN Source.SessionLast ELSE Target.LastSeenTime END,
                    Target.EventCount = Target.EventCount + Source.SessionCount,
                    Target.LastUpdated = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (LoginName, HostName, DatabaseName, ApplicationName, Status, FirstSeenTime, LastSeenTime, EventCount, LastUpdated)
                VALUES (Source.LoginName, Source.HostName, Source.DatabaseName, Source.ApplicationName, Source.Status, Source.SessionFirst, Source.SessionLast, Source.SessionCount, GETDATE());
        END

        ---------------------------------------------------------------------------
        -- 9. Process DETAILED PRINCIPAL AUDIT
        ---------------------------------------------------------------------------
        IF @TrackLoginStats = 1
        BEGIN
            DECLARE @MaxLastUsed DATETIME;
            SELECT @MaxLastUsed = ISNULL(MAX(last_used), '2000-01-01') FROM dbo.tblLoginAudit WITH (NOLOCK);

            MERGE dbo.tblLoginAudit AS target
            USING (
                SELECT 
                    I.NTUserName COLLATE Latin1_General_CI_AS_KS_WS AS NTUserName,
                    I.loginname COLLATE Latin1_General_CI_AS_KS_WS AS loginname,
                    I.HostName COLLATE Latin1_General_CI_AS_KS_WS AS HostName,
                    I.ApplicationName COLLATE Latin1_General_CI_AS_KS_WS AS ApplicationName,
                    I.SessionLoginName COLLATE Latin1_General_CI_AS_KS_WS AS SessionLoginName,
                    I.databasename COLLATE Latin1_General_CI_AS_KS_WS AS databasename,
                    MIN(I.StartTime) AS first_used,
                    MAX(I.StartTime) AS last_used,
                    COUNT(I.StartTime) AS usage_count,
                    S.principal_id,
                    S.sid,
                    S.type_desc COLLATE Latin1_General_CI_AS_KS_WS AS type_desc,
                    S.name COLLATE Latin1_General_CI_AS_KS_WS AS name
                FROM #TraceRows I
                LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.LoginSid) = S.sid
                WHERE I.LoginSid IS NOT NULL
                  AND I.StartTime > @MaxLastUsed
                GROUP BY 
                    I.NTUserName,
                    I.loginname,
                    I.SessionLoginName,
                    I.databasename,
                    S.principal_id,
                    S.sid,
                    S.type_desc,
                    S.name,
                    I.HostName,
                    I.ApplicationName
            ) AS source
            ON (
                ISNULL(target.NTUserName,'') = ISNULL(source.NTUserName,'')
                AND target.loginname = source.loginname
                AND target.HostName = source.HostName
                AND target.ApplicationName = source.ApplicationName
                AND target.SessionLoginName = source.SessionLoginName
                AND target.databasename = source.databasename
                AND ISNULL(target.principal_id, -1) = ISNULL(source.principal_id, -1)
                AND target.sid = source.sid
                AND ISNULL(target.type_desc,'') = ISNULL(source.type_desc,'')
                AND ISNULL(target.name,'') = ISNULL(source.name,'')
            )
            WHEN MATCHED THEN 
                UPDATE SET 
                    target.first_used = CASE 
                                            WHEN target.first_used IS NULL OR source.first_used < target.first_used 
                                            THEN source.first_used 
                                            ELSE target.first_used 
                                        END,
                    target.last_used = CASE 
                                            WHEN target.last_used IS NULL OR source.last_used > target.last_used 
                                            THEN source.last_used 
                                            ELSE target.last_used 
                                        END,
                    target.usage_count = source.usage_count + target.usage_count,
                    target.RunTimeUTC = GETUTCDATE()
            WHEN NOT MATCHED THEN 
                INSERT (NTUserName, loginname, HostName, ApplicationName, SessionLoginName, databasename, first_used, last_used, usage_count, principal_id, sid, type_desc, name)
                VALUES (source.NTUserName, source.loginname, source.HostName, source.ApplicationName, source.SessionLoginName, source.databasename, source.first_used, source.last_used, source.usage_count, source.principal_id, source.sid, source.type_desc, source.name);
        END

        ---------------------------------------------------------------------------
        -- 10. Cleanup
        ---------------------------------------------------------------------------
        IF @RetentionDays > 0
        BEGIN
            DELETE FROM dbo.tblAuditDefaultTrace
            WHERE EventTime < DATEADD(DAY, -@RetentionDays, GETDATE());
        END
        
        IF OBJECT_ID('tempdb..#TraceRows') IS NOT NULL DROP TABLE #TraceRows;

    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#TraceRows') IS NOT NULL DROP TABLE #TraceRows;
        DECLARE @Msg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Msg, 16, 1);
    END CATCH
END
GO
