CREATE OR ALTER PROCEDURE usp_LoginAudit
    @LogToTable BIT = 0
AS
BEGIN
    IF @LogToTable = 1
    BEGIN
        -- Check if the table exists; if not, create it
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tblLoginAudit')
        BEGIN
            CREATE TABLE tblLoginAudit (
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
            )
        END

        -- Insert or update records in the TraceInfo table
        MERGE tblLoginAudit AS target
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
            FROM sys.traces T
            CROSS APPLY ::fn_trace_gettable(
                CASE
                    WHEN CHARINDEX('_', T.[path]) <> 0
                    THEN SUBSTRING(T.PATH, 1, CHARINDEX('_', T.[path])-1) + '.trc'
                    ELSE T.[path]
                END, T.max_files
            ) I
            LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.loginsid) = S.sid
            WHERE T.id = 1
              AND I.LoginSid IS NOT NULL
              AND I.StartTime > ISNULL((SELECT MAX(last_used) FROM tblLoginAudit WITH (NOLOCK)), '01-01-2001')
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
            AND target.principal_id = source.principal_id
            AND target.sid = source.sid
            AND target.type_desc = source.type_desc
            AND target.name = source.name
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
    ELSE
    BEGIN
        -- Just show the current result
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
        FROM sys.traces T
        CROSS APPLY ::fn_trace_gettable(
            CASE
                WHEN CHARINDEX('_', T.[path]) <> 0
                THEN SUBSTRING(T.PATH, 1, CHARINDEX('_', T.[path])-1) + '.trc'
                ELSE T.[path]
            END, T.max_files
        ) I
        LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.loginsid) = S.sid
        WHERE T.id = 1
          AND I.LoginSid IS NOT NULL
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
        ORDER BY 
            MAX(I.StartTime) DESC, 
            I.loginname, 
            I.HostName, 
            I.ApplicationName;
    END
END
GO
