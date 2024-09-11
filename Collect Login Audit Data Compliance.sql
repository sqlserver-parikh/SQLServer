CREATE OR ALTER PROCEDURE usp_AuditLogins
(@CleanUpDays int = 90)
AS
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblAuditLogins]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblAuditLogins](
	[EventName] [nvarchar](128) NULL,
	[subclass_name] [nvarchar](128) NULL,
	[DatabaseName] [nvarchar](256) NULL,
	[DatabaseID] [int] NULL,
	[NTDomainName] [nvarchar](256) NULL,
	[ApplicationName] [nvarchar](256) NULL,
	[LoginName] [nvarchar](256) NULL,
	[SPID] [int] NULL,
	[TextData] nvarchar(max) NULL,
	[StartTime] [datetime] NULL,
	[RoleName] [nvarchar](256) NULL,
	[TargetUserName] [nvarchar](256) NULL,
	[TargetLoginName] [nvarchar](256) NULL,
	[SessionLoginName] [nvarchar](256) NULL
) ON [PRIMARY]
END
IF EXISTS
(
    SELECT 1
    FROM tblAuditLogins
)
    BEGIN
        INSERT INTO tblAuditLogins
               SELECT TE.name AS [EventName],
                      v.subclass_name,
                      T.DatabaseName,
                      t.DatabaseID,
                      t.NTDomainName,
                      t.ApplicationName,
                      t.LoginName,
                      t.SPID, 
					  t.TextData,
                      t.StartTime,
                      t.RoleName,
                      t.TargetUserName,
                      t.TargetLoginName,
                      t.SessionLoginName
               FROM sys.fn_trace_gettable
               (CONVERT(VARCHAR(150),
                       (
                           SELECT TOP 1 f.[value]
                           FROM sys.fn_trace_getinfo(NULL) f
                           WHERE f.property = 2
                       )), DEFAULT
               ) T
                    JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
                    JOIN sys.trace_subclass_values v ON v.trace_event_id = TE.trace_event_id
                                                        AND v.subclass_value = t.EventSubClass
               WHERE te.name IN('Audit Addlogin Event', 'Audit Add DB User Event', 'Audit Add Member to DB Role Event', 'Audit Add Login to Server Role Event')
               AND StartTime >
               (
                   SELECT MAX(StartTime)
                   FROM tblAuditLogins
               )
               ORDER BY StartTime DESC;
END;
    ELSE
    BEGIN
        INSERT INTO tblAuditLogins
        SELECT TE.name AS [EventName],
               v.subclass_name,
               T.DatabaseName,
               t.DatabaseID,
               t.NTDomainName,
               t.ApplicationName,
               t.LoginName,
               t.SPID,
			   t.TextData,
               t.StartTime,
               t.RoleName,
               t.TargetUserName,
               t.TargetLoginName,
               t.SessionLoginName
        FROM sys.fn_trace_gettable
        (CONVERT(VARCHAR(150),
                (
                    SELECT TOP 1 f.[value]
                    FROM sys.fn_trace_getinfo(NULL) f
                    WHERE f.property = 2
                )), DEFAULT
        ) T
             JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
             JOIN sys.trace_subclass_values v ON v.trace_event_id = TE.trace_event_id
                                                 AND v.subclass_value = t.EventSubClass
        WHERE te.name IN('Audit Addlogin Event', 'Audit Add DB User Event', 'Audit Add Member to DB Role Event', 'Audit Add Login to Server Role Event')
        ORDER BY StartTime DESC;
END;
DELETE FROM tblAuditLogins
WHERE StartTime < DATEADD(DD,-@CleanUpDays, getdate())
GO
