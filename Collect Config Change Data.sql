use tempdb
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spConfigChanges]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spConfigChanges]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spConfigChanges]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spConfigChanges] AS' 
END
GO

ALTER PROCEDURE [dbo].[spConfigChanges] (@retaindays INT = 365)
AS
SET NOCOUNT ON;
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblConfigChanges]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblConfigChanges](
	[ConfigOption] [nvarchar](max) NULL,
	[ChangeTime] [datetime] NULL,
	[LoginName] [sysname] NOT NULL,
	[OldValue] [nvarchar](max) NULL,
	[NewValue] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END

BEGIN TRY
     DELETE dbo.[tblConfigChanges]
     WHERE ChangeTime < DATEADD(DD, -@retaindays, GETDATE());
    DECLARE @enable INT;
    SELECT @enable = CONVERT(INT, value_in_use)
    FROM sys.configurations
    WHERE name = 'default trace enabled';
    IF @enable = 1 --default trace is enabled
        BEGIN
            DECLARE @d1 DATETIME;
            DECLARE @diff INT;
            DECLARE @curr_tracefilename VARCHAR(500);
            DECLARE @base_tracefilename VARCHAR(500);
            DECLARE @indx INT;
            DECLARE @temp_trace TABLE
            (textdata    NVARCHAR(MAX) COLLATE database_default,
             login_name  SYSNAME COLLATE database_default,
             start_time  DATETIME,
             event_class INT
            );
            SELECT @curr_tracefilename = path
            FROM sys.traces
            WHERE is_default = 1;
            SET @curr_tracefilename = REVERSE(@curr_tracefilename);
            SELECT @indx = PATINDEX('%\%', @curr_tracefilename);
            SET @curr_tracefilename = REVERSE(@curr_tracefilename);
            SET @base_tracefilename = LEFT(@curr_tracefilename, LEN(@curr_tracefilename) - @indx)+'\log.trc';
            INSERT INTO @temp_trace
                   SELECT TextData,
                          LoginName,
                          StartTime,
                          EventClass
                   FROM ::fn_trace_gettable(@base_tracefilename, DEFAULT)
                   WHERE((EventClass = 22
                          AND Error = 15457)
                         OR (EventClass = 116
                             AND TextData LIKE '%TRACEO%(%'));
            SELECT @d1 = MIN(start_time)
            FROM @temp_trace;
            SET @diff = DATEDIFF(hh, @d1, GETDATE());
            SET @diff = @diff / 24;
            INSERT INTO tblConfigChanges
                   SELECT CASE event_class
                              WHEN 116
                              THEN 'Trace Flag '+SUBSTRING(textdata, PATINDEX('%(%', textdata), LEN(textdata)-PATINDEX('%(%', textdata)+1)
                              WHEN 22
                              THEN SUBSTRING(textdata, 58, PATINDEX('%changed from%', textdata)-60)
                          END AS ConfigOption,
                          start_time ChangeTime,
                          login_name LoginName,
                          CASE event_class
                              WHEN 116
                              THEN '--'
                              WHEN 22
                              THEN SUBSTRING(SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)), PATINDEX('%changed from%', SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)))+13, PATINDEX('%to%', SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)))-PATINDEX('%from%', SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)))-6)
                          END AS OldValue,
                          CASE event_class
                              WHEN 116
                              THEN SUBSTRING(textdata, PATINDEX('%TRACE%', textdata)+5, PATINDEX('%(%', textdata)-PATINDEX('%TRACE%', textdata)-5)
                              WHEN 22
                              THEN SUBSTRING(SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)), PATINDEX('%to%', SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)))+3, PATINDEX('%. Run%', SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)))-PATINDEX('%to%', SUBSTRING(textdata, PATINDEX('%changed from%', textdata), LEN(textdata)-PATINDEX('%changed from%', textdata)))-3)
                          END AS NewValue
                   FROM @temp_trace
			    WHERE start_time > ISNULL( (SELECT MAX(CHANGETIME) FROM tblConfigChanges),DATEADD(DD,-15,GETDATE()))
    END;
        ELSE
        BEGIN
            INSERT INTO tblConfigChanges
            SELECT TOP 0 1 AS config_option,
                         1 AS start_time,
                         1 AS login_name,
                         1 AS old_value,
                         1 AS new_value;
    END;
END TRY
BEGIN CATCH
    INSERT INTO tblConfigChanges
           SELECT ERROR_STATE() AS config_option,
                  1 AS start_time,
                  ERROR_MESSAGE() AS login_name,
                  1 AS old_value,
                  1 AS new_value;
END CATCH;
GO
--SELECT * FROM [tblConfigChanges]
