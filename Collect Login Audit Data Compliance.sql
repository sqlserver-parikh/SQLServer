USE tempdb
GO

-- ============================================================================
-- 1. CLEANUP / RESET 
-- (Drops all objects first to ensure Schema is created correctly)
-- ============================================================================

-- Drop Procedures
IF OBJECT_ID('dbo.usp_DefaultTrace', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.usp_DefaultTrace;
GO

-- Drop Tables
IF OBJECT_ID('dbo.tblAuditDefaultTrace', 'U') IS NOT NULL 
    DROP TABLE dbo.tblAuditDefaultTrace;
GO
IF OBJECT_ID('dbo.tblLoginAudit', 'U') IS NOT NULL 
    DROP TABLE dbo.tblLoginAudit;
GO
IF OBJECT_ID('dbo.tblConfigChanges', 'U') IS NOT NULL 
    DROP TABLE dbo.tblConfigChanges;
GO

-- ============================================================================
-- 2. CREATE PROCEDURE [usp_DefaultTrace]
-- ============================================================================
CREATE PROCEDURE [dbo].[usp_DefaultTrace]
(
    @LogToTable        BIT  = 1,        -- 1 = Log to tables, 0 = Display Previews
    @RetentionDays     INT  = 365,      -- Retention for Logs
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
        -- 2. Define System Databases & Object Types
        ---------------------------------------------------------------------------
        DECLARE @SystemDBs TABLE (Name NVARCHAR(128) PRIMARY KEY);
        INSERT INTO @SystemDBs (Name) VALUES 
        ('master'), ('model'), ('msdb'), ('tempdb'), ('distribution'), 
        ('ReportServer'), ('ReportServerTempDB'), ('SSISDB');

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
        -- 3. Table Setup (Create if not exists)
        ---------------------------------------------------------------------------
        IF @LogToTable = 1
        BEGIN
            -- TABLE A: General DDL & Security Audit
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

            -- TABLE B: Login Statistics
            -- Note: NVARCHAR(128) used to prevent Index Key Limit (>1700 bytes) errors
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
                    Status              VARCHAR(20) NOT NULL, -- 'Success' or 'Failed'
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

            -- TABLE C: Configuration Changes
            -- Updated: Added HostName column
            IF OBJECT_ID('dbo.tblConfigChanges', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.tblConfigChanges(
                    ChangeID        BIGINT IDENTITY(1,1) PRIMARY KEY,
                    ConfigOption    NVARCHAR(MAX) NULL,
                    ChangeTime      DATETIME NULL,
                    LoginName       SYSNAME NOT NULL,
                    HostName        NVARCHAR(128) NULL, -- Added Column
                    OldValue        NVARCHAR(MAX) NULL,
                    NewValue        NVARCHAR(MAX) NULL,
                    CapturedAt      DATETIME DEFAULT GETDATE()
                ) WITH (DATA_COMPRESSION = PAGE);
                
                CREATE NONCLUSTERED INDEX IX_ConfigChanges_Time ON dbo.tblConfigChanges(ChangeTime DESC);
            END
        END

        ---------------------------------------------------------------------------
        -- 4. Load Trace Data into Temp Table
        ---------------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#TraceRows') IS NOT NULL DROP TABLE #TraceRows;

        DECLARE @FetchStartTime DATETIME;
        IF @MinStartTime IS NOT NULL
            SET @FetchStartTime = @MinStartTime;
        ELSE
            SET @FetchStartTime = DATEADD(DAY, -7, GETDATE());

        SELECT
            T.StartTime,
            TE.name AS EventName,
            T.EventClass,
            SV.subclass_name AS EventSubClassName,
            CASE
                WHEN TE.name LIKE 'Audit Add%' OR TE.name LIKE 'Audit Login%' THEN 'Security'
                WHEN TE.name LIKE 'Object:%' THEN 'DDL'
                WHEN T.EventClass IN (22, 116) THEN 'Configuration'
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
            CAST(T.LoginSid AS VARBINARY(85)) AS LoginSid,
            T.NTUserName,
            T.SessionLoginName,
            T.Error
        INTO #TraceRows
        FROM sys.fn_trace_gettable(@TracePath, DEFAULT) AS T
        INNER JOIN sys.trace_events AS TE ON T.EventClass = TE.trace_event_id
        LEFT JOIN sys.trace_subclass_values AS SV ON T.EventClass = SV.trace_event_id AND T.EventSubClass = SV.subclass_value
        LEFT JOIN @ObjectTypeMap AS OT ON T.ObjectType = OT.ID
        WHERE
            T.StartTime >= @FetchStartTime
            AND (
                (TE.name LIKE '%Login%') 
                OR 
                (T.EventClass IN (22, 116)) 
                OR
                (
                    (@IncludeSystem = 1 OR T.DatabaseName IS NULL OR T.DatabaseName NOT IN (SELECT Name FROM @SystemDBs))
                    AND (T.ObjectName NOT LIKE '_WA_Sys_%' OR T.ObjectName IS NULL)
                )
            );

        ---------------------------------------------------------------------------
        -- 5. PREVIEW MODE
        ---------------------------------------------------------------------------
        IF @LogToTable = 0 
        BEGIN
            PRINT '>>> PREVIEW: Events for tblAuditDefaultTrace (DDL/Security) <<<';
            SELECT StartTime, EventCategory, EventName, ObjectName, LoginName, TextData
            FROM #TraceRows WHERE EventName NOT LIKE 'Audit Login%' AND EventCategory != 'Configuration'
            ORDER BY StartTime DESC;

            PRINT '>>> PREVIEW: Configuration Changes for tblConfigChanges <<<';
            SELECT StartTime, LoginName, HostName, TextData FROM #TraceRows WHERE EventCategory = 'Configuration';
            RETURN;
        END

        ---------------------------------------------------------------------------
        -- 6. PROCESS: General Audit (DDL & Security)
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
          AND src.EventCategory IN ('DDL', 'Security') 
          AND src.EventName NOT LIKE 'Audit Login%'
          AND NOT EXISTS (SELECT 1 FROM dbo.tblAuditDefaultTrace T WHERE T.RowHash = HashVal.RowHash);

        ---------------------------------------------------------------------------
        -- 7. PROCESS: Login Stats
        ---------------------------------------------------------------------------
        DECLARE @MaxLastUsed DATETIME;
        SELECT @MaxLastUsed = ISNULL(MAX(LastSeen), '2000-01-01') FROM dbo.tblLoginAudit WITH (NOLOCK);

        MERGE dbo.tblLoginAudit AS target
        USING (
            SELECT 
                CAST(I.NTUserName AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS NTUserName,
                CAST(I.LoginName AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS LoginName,
                CAST(ISNULL(I.HostName, 'Unknown') AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS HostName,
                CAST(ISNULL(I.ApplicationName, 'Unknown') AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS ApplicationName,
                CAST(I.SessionLoginName AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS SessionLoginName,
                CAST(ISNULL(I.DatabaseName, '') AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS DatabaseName,
                CASE WHEN I.EventName LIKE '%Failed%' THEN 'Failed' ELSE 'Success' END AS Status,
                MIN(I.StartTime) AS FirstSeen,
                MAX(I.StartTime) AS LastSeen,
                COUNT(*) AS EventCount,
                S.principal_id,
                S.sid,
                CAST(S.type_desc AS NVARCHAR(128)) COLLATE Latin1_General_CI_AS_KS_WS AS type_desc
            FROM #TraceRows I
            LEFT JOIN sys.server_principals S ON I.LoginSid = S.sid
            WHERE I.LoginName IS NOT NULL AND I.StartTime > @MaxLastUsed
            GROUP BY I.NTUserName, I.LoginName, I.SessionLoginName, I.DatabaseName,
                CASE WHEN I.EventName LIKE '%Failed%' THEN 'Failed' ELSE 'Success' END,
                S.principal_id, S.sid, S.type_desc, I.HostName, I.ApplicationName
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
                target.RunTimeUTC = GETUTCDATE()
        WHEN NOT MATCHED THEN 
            INSERT (NTUserName, LoginName, HostName, ApplicationName, SessionLoginName, DatabaseName, Status, FirstSeen, LastSeen, EventCount, PrincipalID, SID, TypeDesc)
            VALUES (source.NTUserName, source.LoginName, source.HostName, source.ApplicationName, source.SessionLoginName, source.DatabaseName, source.Status, source.FirstSeen, source.LastSeen, source.EventCount, source.principal_id, source.sid, source.type_desc);

        ---------------------------------------------------------------------------
        -- 8. PROCESS: Configuration Changes
        ---------------------------------------------------------------------------
        DECLARE @LastConfigDate DATETIME;
        SELECT @LastConfigDate = ISNULL(MAX(ChangeTime), '1900-01-01') FROM dbo.tblConfigChanges;

        INSERT INTO dbo.tblConfigChanges (ConfigOption, ChangeTime, LoginName, HostName, OldValue, NewValue)
        SELECT 
            CASE EventClass
                WHEN 116 THEN 'Trace Flag ' + SUBSTRING(TextData, PATINDEX('%(%', TextData), LEN(TextData)-PATINDEX('%(%', TextData)+1)
                WHEN 22  THEN SUBSTRING(TextData, 58, PATINDEX('%changed from%', TextData)-60)
            END AS ConfigOption,
            StartTime AS ChangeTime,
            LoginName,
            HostName, -- Inserted HostName
            CASE EventClass
                WHEN 116 THEN '--'
                WHEN 22  THEN SUBSTRING(SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)), PATINDEX('%changed from%', SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)))+13, PATINDEX('%to%', SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)))-PATINDEX('%from%', SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)))-6)
            END AS OldValue,
            CASE EventClass
                WHEN 116 THEN SUBSTRING(TextData, PATINDEX('%TRACE%', TextData)+5, PATINDEX('%(%', TextData)-PATINDEX('%TRACE%', TextData)-5)
                WHEN 22  THEN SUBSTRING(SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)), PATINDEX('%to%', SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)))+3, PATINDEX('%. Run%', SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)))-PATINDEX('%to%', SUBSTRING(TextData, PATINDEX('%changed from%', TextData), LEN(TextData)-PATINDEX('%changed from%', TextData)))-3)
            END AS NewValue
        FROM #TraceRows
        WHERE StartTime > @LastConfigDate
          AND (
              (EventClass = 22 AND Error = 15457) 
              OR 
              (EventClass = 116 AND TextData LIKE '%TRACEO%(%')
          );

        ---------------------------------------------------------------------------
        -- 9. CLEANUP
        ---------------------------------------------------------------------------
        IF @RetentionDays > 0
        BEGIN
            DECLARE @CutoffDate DATETIME = DATEADD(DAY, -@RetentionDays, GETDATE());
            DELETE FROM dbo.tblAuditDefaultTrace WHERE EventTime < @CutoffDate;
            DELETE FROM dbo.tblConfigChanges WHERE ChangeTime < @CutoffDate;
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
exec usp_DefaultTrace
