CREATE TABLE #objecttype
(id         INT, 
 objectname NVARCHAR(255)
);
INSERT INTO #objecttype
       SELECT 8259, 
              'Check Constraint'
       UNION ALL
       SELECT 8260, 
              'Default (constraint or standalone)'
       UNION ALL
       SELECT 8262, 
              'Foreign-key Constraint'
       UNION ALL
       SELECT 8272, 
              'Stored Procedure'
       UNION ALL
       SELECT 8274, 
              'Rule'
       UNION ALL
       SELECT 8275, 
              'System Table'
       UNION ALL
       SELECT 8276, 
              'Trigger on Server'
       UNION ALL
       SELECT 8277, 
              '(User-defined) Table'
       UNION ALL
       SELECT 8278, 
              'View'
       UNION ALL
       SELECT 8280, 
              'Extended Stored Procedure'
       UNION ALL
       SELECT 16724, 
              'CLR Trigger'
       UNION ALL
       SELECT 16964, 
              'Database'
       UNION ALL
       SELECT 16975, 
              'Object'
       UNION ALL
       SELECT 17222, 
              'FullText Catalog'
       UNION ALL
       SELECT 17232, 
              'CLR Stored Procedure'
       UNION ALL
       SELECT 17235, 
              'Schema'
       UNION ALL
       SELECT 17475, 
              'Credential'
       UNION ALL
       SELECT 17491, 
              'DDL Event'
       UNION ALL
       SELECT 17741, 
              'Management Event'
       UNION ALL
       SELECT 17747, 
              'Security Event'
       UNION ALL
       SELECT 17749, 
              'User Event'
       UNION ALL
       SELECT 17985, 
              'CLR Aggregate Function'
       UNION ALL
       SELECT 17993, 
              'Inline Table-valued SQL Function'
       UNION ALL
       SELECT 18000, 
              'Partition Function'
       UNION ALL
       SELECT 18002, 
              'Replication Filter Procedure'
       UNION ALL
       SELECT 18004, 
              'Table-valued SQL Function'
       UNION ALL
       SELECT 18259, 
              'Server Role'
       UNION ALL
       SELECT 18263, 
              'Microsoft Windows Group'
       UNION ALL
       SELECT 19265, 
              'Asymmetric Key'
       UNION ALL
       SELECT 19277, 
              'Master Key'
       UNION ALL
       SELECT 19280, 
              'Primary Key'
       UNION ALL
       SELECT 19283, 
              'ObfusKey'
       UNION ALL
       SELECT 19521, 
              'Asymmetric Key Login'
       UNION ALL
       SELECT 19523, 
              'Certificate Login'
       UNION ALL
       SELECT 19538, 
              'Role'
       UNION ALL
       SELECT 19539, 
              'SQL Login'
       UNION ALL
       SELECT 19543, 
              'Windows Login'
       UNION ALL
       SELECT 20034, 
              'Remote Service Binding'
       UNION ALL
       SELECT 20036, 
              'Event Notification on Database'
       UNION ALL
       SELECT 20037, 
              'Event Notification'
       UNION ALL
       SELECT 20038, 
              'Scalar SQL Function'
       UNION ALL
       SELECT 20047, 
              'Event Notification on Object'
       UNION ALL
       SELECT 20051, 
              'Synonym'
       UNION ALL
       SELECT 20549, 
              'End Point'
       UNION ALL
       SELECT 20801, 
              'Adhoc Queries which may be cached'
       UNION ALL
       SELECT 20816, 
              'Prepared Queries which may be cached'
       UNION ALL
       SELECT 20819, 
              'Service Broker Service Queue'
       UNION ALL
       SELECT 20821, 
              'Unique Constraint'
       UNION ALL
       SELECT 21057, 
              'Application Role'
       UNION ALL
       SELECT 21059, 
              'Certificate'
       UNION ALL
       SELECT 21075, 
              'Server'
       UNION ALL
       SELECT 21076, 
              'Transact-SQL Trigger'
       UNION ALL
       SELECT 21313, 
              'Assembly'
       UNION ALL
       SELECT 21318, 
              'CLR Scalar Function'
       UNION ALL
       SELECT 21321, 
              'Inline scalar SQL Function'
       UNION ALL
       SELECT 21328, 
              'Partition Scheme'
       UNION ALL
       SELECT 21333, 
              'User'
       UNION ALL
       SELECT 21571, 
              'Service Broker Service Contract'
       UNION ALL
       SELECT 21572, 
              'Trigger on Database'
       UNION ALL
       SELECT 21574, 
              'CLR Table-valued Function'
       UNION ALL
       SELECT 21577, 
              'Internal Table (For example, XML Node Table, Queue Table.)'
       UNION ALL
       SELECT 21581, 
              'Service Broker Message Type'
       UNION ALL
       SELECT 21586, 
              'Service Broker Route'
       UNION ALL
       SELECT 21825, 
              'User'
       UNION ALL
       SELECT 21827, 
              'User'
       UNION ALL
       SELECT 21831, 
              'User'
       UNION ALL
       SELECT 21843, 
              'User'
       UNION ALL
       SELECT 21847, 
              'User'
       UNION ALL
       SELECT 21587, 
              'Statistics'
       UNION ALL
       SELECT 22099, 
              'Service Broker Service'
       UNION ALL
       SELECT 22601, 
              'Index'
       UNION ALL
       SELECT 22604, 
              'Certificate Login'
       UNION ALL
       SELECT 22611, 
              'XMLSchema'
       UNION ALL
       SELECT 22868, 
              'Type';
GO
DECLARE @d1 DATETIME;
DECLARE @diff INT;
DECLARE @curr_tracefilename VARCHAR(500);
DECLARE @base_tracefilename VARCHAR(500);
DECLARE @indx INT;
SELECT @curr_tracefilename = path
FROM sys.traces
WHERE is_default = 1;
SET @curr_tracefilename = REVERSE(@curr_tracefilename); 
SELECT @indx = PATINDEX('%\%', @curr_tracefilename);
SET @curr_tracefilename = REVERSE(@curr_tracefilename);
SET @base_tracefilename = LEFT(@curr_tracefilename, LEN(@curr_tracefilename) - @indx) + '\log.trc';
SELECT DISTINCT 
       DatabaseName, 
       a.ObjectName, 
       ObjectID, 
       b.name Operation, 
       c.objectname ObjType, 
       StartTime, 
       LoginName, 
       ApplicationName
FROM ::fn_trace_gettable(@base_tracefilename, DEFAULT) a
     LEFT JOIN sys.trace_events b ON a.EventClass = b.trace_event_id
     LEFT JOIN #objecttype c ON a.ObjectType = c.id
WHERE EventSubclass = 0
    --  AND ApplicationName LIKE 'Microsoft SQL Server Management Studio%';
--and DatabaseID = db_id() and DatabaseName not like 'reportserver%' 
--and DatabaseName not like 'tempdb' and c.objectname not like 'statistics' and c.objectname is not null 

GO
DROP TABLE #objecttype;
