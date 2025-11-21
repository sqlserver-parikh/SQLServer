USE tempdb
GO
-- ============================================================================
-- 1. CLEANUP / RESET 
-- ============================================================================
-- Remove the old separate stats table
IF OBJECT_ID('dbo.tblAuditLoginStats', 'U') IS NOT NULL 
    DROP TABLE dbo.tblAuditLoginStats;
GO
-- Remove the detailed tables to recreate with new schema
IF OBJECT_ID('dbo.tblLoginAudit', 'U') IS NOT NULL 
    DROP TABLE dbo.tblLoginAudit;
GO
IF OBJECT_ID('dbo.tblAuditDefaultTrace', 'U') IS NOT NULL 
    DROP TABLE dbo.tblAuditDefaultTrace;
GO
IF OBJECT_ID('dbo.usp_AuditDefaultTrace', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.usp_AuditDefaultTrace;
GO
-- Clean up old procs if they exist
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
    @LogToTable        BIT  = 0,        -- 1 = Log to tables, 0 = Display Previews
    @RetentionDays     INT  = 90,       -- Retention for Logs
    @IncludeTextData   BIT  = 1,        -- Keep SQL text for DDL events
    @IncludeSystem     BIT  = 0,        -- 0 = Exclude system DBs
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
        -- 2. Define System Databases
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
        -- 4. Table Setup (Consolidated & Compressed)
        ---------------------------------------------------------------------------
        IF @LogToTable = 1
        BEGIN
            -- TABLE A: Event Log (DDL, Security changes)
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

            -- TABLE B: Consolidated Login Audit (Stats + Identity + Status)
            IF OBJECT_ID('dbo.tblLoginAudit', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.tblLoginAudit (
                    AuditID             BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    LoginName           NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    HostName            NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    ApplicationName     NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    DatabaseName        NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    SessionLoginName    NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    NTUserName          NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    Status              VARCHAR(20) NOT NULL, -- 'Success' or 'Failed'
                    PrincipalID         INT,
                    SID                 VARBINARY(MAX),
                    TypeDesc            NVARCHAR(256) COLLATE Latin1_General_CI_AS_KS_WS,
                    FirstSeen           DATETIME,
                    LastSeen            DATETIME,
                    EventCount          BIGINT,
                    RunTimeUTC          DATETIME DEFAULT GETUTCDATE()
                ) WITH (DATA_COMPRESSION = PAGE);
                
                -- Unique Index to handle the Upsert/Merge logic
                CREATE UNIQUE NONCLUSTERED INDEX UX_LoginAudit_Merge 
                    ON dbo.tblLoginAudit (LoginName, HostName, ApplicationName, DatabaseName, Status, SID)
                    WITH (DATA_COMPRESSION = PAGE);
            END
            
            -- Schema Recovery
            IF COL_LENGTH('dbo.tblAuditDefaultTrace', 'RowHash') IS NULL
            BEGIN
                ALTER TABLE dbo.tblAuditDefaultTrace ADD RowHash VARBINARY(32) NULL;
                EXEC sp_executesql N'CREATE UNIQUE NONCLUSTERED INDEX UX_AuditTrace_RowHash ON dbo.tblAuditDefaultTrace (RowHash) WHERE RowHash IS NOT NULL WITH (DATA_COMPRESSION = PAGE);';
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
                (TE.name LIKE '%Login%') -- Always capture login/failure
                OR 
                (
                    @IncludeSystem = 1 
                    OR T.DatabaseName IS NULL 
                    OR T.DatabaseName NOT IN (SELECT Name FROM @SystemDBs)
                )
            )
            AND (T.ObjectName NOT LIKE '_WA_Sys_%' OR T.ObjectName IS NULL);

        ---------------------------------------------------------------------------
        -- 6. PREVIEW MODE (Display Both Result Sets)
        ---------------------------------------------------------------------------
        IF @LogToTable = 0 
        BEGIN
            PRINT '>>> PREVIEW: Events that would go into tblAuditDefaultTrace (DDL/Security) <<<';
            SELECT 
                StartTime AS EventTime, EventCategory, EventName, EventSubClassName, 
                ObjectType, DatabaseName, ObjectName, LoginName, TargetLoginName, TextData
            FROM #TraceRows
            WHERE EventName NOT LIKE 'Audit Login%'
            ORDER BY StartTime DESC;

            PRINT '>>> PREVIEW: Aggregated Stats that would go into tblLoginAudit (Success & Failed) <<<';
            SELECT 
                I.LoginName, I.HostName, I.ApplicationName, I.DatabaseName,
                CASE WHEN I.EventName LIKE '%Failed%' THEN 'Failed' ELSE 'Success' END AS Status,
                COUNT(*) as NewEvents,
                MIN(I.StartTime) as FirstSeen,
                MAX(I.StartTime) as LastSeen,
                S.name as PrincipalName,
                S.type_desc
            FROM #TraceRows I
            LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.LoginSid) = S.sid
            WHERE I.LoginName IS NOT NULL
            GROUP BY 
                I.LoginName, I.HostName, I.ApplicationName, I.DatabaseName,
                CASE WHEN I.EventName LIKE '%Failed%' THEN 'Failed' ELSE 'Success' END,
                S.name, S.type_desc
            ORDER BY LastSeen DESC;

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
        -- 8. Process CONSOLIDATED LOGIN AUDIT (One Table)
        ---------------------------------------------------------------------------
        DECLARE @MaxLastUsed DATETIME;
        SELECT @MaxLastUsed = ISNULL(MAX(LastSeen), '2000-01-01') FROM dbo.tblLoginAudit WITH (NOLOCK);

        MERGE dbo.tblLoginAudit AS target
        USING (
            SELECT 
                I.NTUserName COLLATE Latin1_General_CI_AS_KS_WS AS NTUserName,
                I.LoginName COLLATE Latin1_General_CI_AS_KS_WS AS LoginName,
                ISNULL(I.HostName, 'Unknown') COLLATE Latin1_General_CI_AS_KS_WS AS HostName,
                ISNULL(I.ApplicationName, 'Unknown') COLLATE Latin1_General_CI_AS_KS_WS AS ApplicationName,
                I.SessionLoginName COLLATE Latin1_General_CI_AS_KS_WS AS SessionLoginName,
                ISNULL(I.DatabaseName, '') COLLATE Latin1_General_CI_AS_KS_WS AS DatabaseName,
                CASE 
                    WHEN I.EventName LIKE '%Failed%' THEN 'Failed' 
                    ELSE 'Success' 
                END AS Status,
                MIN(I.StartTime) AS FirstSeen,
                MAX(I.StartTime) AS LastSeen,
                COUNT(*) AS EventCount,
                S.principal_id,
                S.sid,
                S.type_desc COLLATE Latin1_General_CI_AS_KS_WS AS type_desc
            FROM #TraceRows I
            LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.LoginSid) = S.sid
            WHERE I.LoginName IS NOT NULL
              AND I.StartTime > @MaxLastUsed
            GROUP BY 
                I.NTUserName,
                I.LoginName,
                I.SessionLoginName,
                I.DatabaseName,
                CASE WHEN I.EventName LIKE '%Failed%' THEN 'Failed' ELSE 'Success' END,
                S.principal_id,
                S.sid,
                S.type_desc,
                I.HostName,
                I.ApplicationName
        ) AS source
        ON (
            target.LoginName = source.LoginName
            AND target.HostName = source.HostName
            AND target.ApplicationName = source.ApplicationName
            AND target.DatabaseName = source.DatabaseName
            AND target.Status = source.Status
            AND ISNULL(target.SID, 0x00) = ISNULL(source.sid, 0x00) -- Handle NULL SIDs for failed logins
        )
        WHEN MATCHED THEN 
            UPDATE SET 
                target.LastSeen = CASE 
                                    WHEN source.LastSeen > target.LastSeen THEN source.LastSeen 
                                    ELSE target.LastSeen 
                                  END,
                target.EventCount = target.EventCount + source.EventCount,
                target.RunTimeUTC = GETUTCDATE()
        WHEN NOT MATCHED THEN 
            INSERT (
                NTUserName, LoginName, HostName, ApplicationName, SessionLoginName, DatabaseName, 
                Status, FirstSeen, LastSeen, EventCount, PrincipalID, SID, TypeDesc
            )
            VALUES (
                source.NTUserName, source.LoginName, source.HostName, source.ApplicationName, source.SessionLoginName, source.DatabaseName, 
                source.Status, source.FirstSeen, source.LastSeen, source.EventCount, source.principal_id, source.sid, source.type_desc
            );

        ---------------------------------------------------------------------------
        -- 9. Cleanup
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
