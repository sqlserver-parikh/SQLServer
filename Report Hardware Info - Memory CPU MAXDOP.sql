USE tempdb 
GO
CREATE OR ALTER PROCEDURE usp_SQLInformation
(@LogToTable bit = 0, @Retention int = 26) 
AS
BEGIN

IF @LogToTable = 1
BEGIN
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[tblSQLInformation]')
          AND type IN (N'U')
)
CREATE TABLE [tblSQLInformation](
	[ServerName] [sql_variant] NULL,
	[PortNumber] [sql_variant] NULL,
	[SQLVersionDesc] [varchar](18) NULL,
	[SQLVersion] [sql_variant] NULL,
	[ServicePack] [sql_variant] NULL,
	[AGName] [sysname] NULL,
	[AGListenerName] [varchar](128) NULL,
	[AGPrimaryServer] [varchar](128) NULL,
	[AGServerList] [varchar](max) NULL,
	[AGDBList] [varchar](max) NULL,
	[TotalNoOfInstances] [int] NULL,
	[AllInstancesName] [nvarchar](max) NULL,
	[RunningNode] [sql_variant] NULL,
	[IPAddress] [sql_variant] NULL,
	[DomainNameList] [nvarchar](max) NULL,
	[AllNodes] [nvarchar](max) NULL,
	[Edition] [sql_variant] NULL,
	[ErrorLogLocation] [sql_variant] NULL,
	[Data Files] [nvarchar](512) NULL,
	[Log Files] [nvarchar](512) NULL,
	[SQLDataRoot] [nvarchar](512) NULL,
	[DefaultBackup] [nvarchar](4000) NULL,
	[DBCount] [int] NULL,
	[TotalDataSizeMB] [decimal](25, 0) NULL,
	[TotalLogSizeMB] [decimal](25, 0) NULL,
	[ServerCollation] [sql_variant] NULL,
	[TempDBDataFileCount] [int] NULL,
	[ProcessorCount] [nvarchar](30) NULL,
	[MAXDOP] [sql_variant] NULL,
	[CostThreshold] [int] NULL,
	[TotalMemory] [nvarchar](30) NULL,
	[MinMemory] [sql_variant] NULL,
	[MaxMemory] [sql_variant] NULL,
	[LockPagesInMemory] varchar(128) NULL,
	 Enabled_Trace_Flags varchar(max) NULL,
NonStandardConfigurations varchar(max) NULL,
	[WindowsName] [varchar](128) NULL,
	[WindowsRDPPort] [int] NULL,
	[InstantFileInitialization] [varchar](40) NOT NULL,
	[SystemManufacturer] [varchar](500) NOT NULL,
	[Physica/Virtual] [varchar](8) NULL,
	[SystemProductName] [varchar](100) NOT NULL,
	[CPU Description] [varchar](500) NULL,
	[IsClustered] [varchar](3) NULL,
	[WindowsCluster] [varchar](128) NOT NULL,
	[DBEngineLogin] [varchar](100) NULL,
	[AgentLogin] [varchar](100) NULL,
	[SQLStartTime] [datetime] NULL,
	[OSRebootTime] [datetime] NULL,
	[SQLInstallDate] [datetime] NULL,
	[ServerTimeZone] varchar(100) NULL,
	[RunTimeUTC] [datetime] NOT NULL
) ON [PRIMARY]
END 
DECLARE @DomainNames NVARCHAR(MAX) = '';

SELECT @DomainNames = STUFF((
    SELECT DISTINCT ', ' + LEFT(name, CHARINDEX('\', name) - 1)
    FROM sys.server_principals
    WHERE type_desc IN ('WINDOWS_LOGIN', 'WINDOWS_GROUP')
      AND name LIKE '%\%' 
      AND name NOT LIKE 'NT %' 
	  AND name NOT LIKE 'BUILTIN%'
	  --and name like '%dba%'
    FOR XML PATH(''), TYPE
).value('.', 'NVARCHAR(MAX)'), 1, 2, '');

DECLARE @TimeZone VARCHAR(50);
DECLARE @IsDST BIT;
DECLARE @UTCOffset VARCHAR(10);

-- Read the time zone key name from the registry
EXEC MASTER.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
    'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
    'TimeZoneKeyName',
    @TimeZone OUTPUT;

-- Check if DST is currently in effect
EXEC MASTER.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
    'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
    'ActiveTimeBias',
    @IsDST OUTPUT;

-- Get the UTC offset
SET @UTCOffset = (SELECT convert(varchar(30), current_utc_offset) FROM sys.time_zone_info WHERE name = @TimeZone);

-- Format the time zone information
SET @TimeZone = (SELECT name + ' (UTC' + @UTCOffset + '), ' + ' DST: ' + CASE WHEN @IsDST = 0 THEN 'OFF' ELSE 'ON' END FROM sys.time_zone_info WHERE name = @TimeZone);

-- Return the formatted time zone information

SET NOCOUNT ON;
BEGIN TRY
 CREATE TABLE #WinNames
    (WinID   VARCHAR(128), 
     WinName VARCHAR(MAX)
    );

INSERT INTO #WinNames
VALUES
('5.2 (3790)', 
 'Windows Server 2003 R2'
),
('5.2 ()', 
 'Windows Server 2003 R2'
),
('6.0 (6002)', 
 'Windows Server 2008'
),
('6.1 (7601)', 
 'Windows Server 2008 R2'
),
('6.2 (9200)', 
 'Windows Server 2012'
),
('6.3 (9600)', 
 'Windows Server 2012 R2'
),
('6.3 (14393)', 
 'Windows Server 2016'
),
('6.3 (20348)', 
 'Windows Server 2022'
),
('10.0 (14393)', 
 'Windows Server 2016'
),
('10.0 (17763)', 
 'Windows Server 2019'
),
('10.0 (20348)', 
 'Windows Server 2022'
), ('6.3 (17763)', 'Windows Server 2022'),
('10.0 (10240)', 
 'Windows 10'
),
('10.0 (19041)', 
 'Windows 10'
),
('10.0 (19042)', 
 'Windows 10'
),
('10.0 (19043)', 
 'Windows 10'
),
('10.0 (19044)', 
 'Windows 10'
),
('10.0 (19045)', 
 'Windows 10'
),
('10.0 (22000)', 
 'Windows 11'
),
('10.0 (22621)', 
 'Windows 11'
);

DECLARE @config TABLE (
    name NVARCHAR(35),
    default_value SQL_VARIANT
);

-- Insert default configuration values into the table variable
INSERT INTO @config (name, default_value) VALUES
('access check cache bucket count', 0),
('access check cache quota', 0),
('Ad Hoc Distributed Queries', 0),
('affinity I/O mask', 0),
('affinity64 I/O mask', 0),
('affinity mask', 0),
('affinity64 mask', 0),
('Agent XPs', 1), -- Changes to 1 if SQL Agent is started, so I check for that
('allow updates', 0),
('awe enabled', 0),
('backup compression default', 0),
('blocked process threshold (s)', 0),
('c2 audit mode', 0),
('clr enabled', 0),
('common criteria compliance enabled', 0),
('contained database authentication', 0), 
('cost threshold for parallelism', 80),
('cross db ownership chaining', 0),
('cursor threshold', -1),
('Database Mail XPs', 1),
('default full-text language', 1033),
('default language', 0),
('default trace enabled', 1),
('disallow results from triggers', 0),
('EKM provider enabled', 0),
('filestream access level', 0),
('fill factor (%)', 0),
('ft crawl bandwidth (max)', 100),
('ft crawl bandwidth (min)', 0),
('ft notify bandwidth (max)', 100),
('ft notify bandwidth (min)', 0),
('index create memory (KB)', 0),
('in-doubt xact resolution', 0),
('lightweight pooling', 0),
('locks', 0),
('max degree of parallelism', 0),
('max full-text crawl range', 4),
('max server memory (MB)', 2147483647),
('max text repl size (B)', 65536),
('max worker threads', 0),
('media retention', 0),
('min memory per query (KB)', 1024),
('min server memory (MB)', 0),
('nested triggers', 1),
('network packet size (B)', 4096),
('Ole Automation Procedures', 0),
('open objects', 0),
('optimize for ad hoc workloads', 0),
('PH timeout (s)', 60),
('precompute rank', 0),
('priority boost', 0),
('query governor cost limit', 0),
('query wait (s)', -1),
('recovery interval (min)', 0),
('remote access', 1),
('remote admin connections', 0),
('remote login timeout (s)', 10),
('remote proc trans', 0),
('remote query timeout (s)', 600),
('Replication XPs', 0),
('scan for startup procs', 0),
('server trigger recursion', 1),
('set working set size', 0),
('show advanced options', 0),
('SMO and DMO XPs', 1),
('SQL Mail XPs', 0),
('transform noise words', 0),
('two digit year cutoff', 2049),
('user connections', 0),
('user options', 0),
('Web Assistant Procedures', 0),
('xp_cmdshell', 0);

-- Variable to store the result
DECLARE @NonStandardConfigs NVARCHAR(MAX);
DECLARE @Count INT;

-- Concatenate non-standard configuration values into a single comma-separated string
SELECT @NonStandardConfigs = STRING_AGG(
    CONCAT(sc.name, ' (Default: ', CONVERT(NVARCHAR(MAX), c.default_value), ', Current: ', CONVERT(NVARCHAR(MAX), sc.value_in_use), ')'), ', '),
    @Count = COUNT(*)
FROM sys.configurations sc
INNER JOIN @config c ON sc.name = c.name
WHERE sc.value_in_use <> c.default_value;


DECLARE @TraceFlags VARCHAR(MAX) = '';
DECLARE @TraceFlagCount INT = 0;

-- Drop the temporary table if it already exists
IF OBJECT_ID('tempdb..#TraceFlags') IS NOT NULL
    DROP TABLE #TraceFlags;

-- Temporary table to store trace flag status
CREATE TABLE #TraceFlags (TraceFlag INT, Status INT, Global INT, Session INT);

-- Insert trace flag status into the temporary table
INSERT INTO #TraceFlags (TraceFlag, Status, Global, Session)
EXEC ('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS');

-- Concatenate trace flags into a single comma-separated string and count them
SELECT @TraceFlags = STRING_AGG(CAST(TraceFlag AS VARCHAR), ', '),
       @TraceFlagCount = COUNT(*)
FROM #TraceFlags
WHERE Status = 1;

   DECLARE @Plat TABLE
    (Id             INT, 
     Name           VARCHAR(180), 
     InternalValue  VARCHAR(50), 
     Charactervalue VARCHAR(50)
    );
    DECLARE @Platform VARCHAR(100), @WinName VARCHAR(128);
    INSERT INTO @Plat
    EXEC xp_msver 
         WindowsVersion;
    SELECT @WinName = WinName
    FROM #WinNames A
         INNER JOIN @Plat B ON A.WinID = B.Charactervalue;
    DELETE @PLAT;
    DECLARE @CurrID INT, @ExistValue INT, @MaxID INT, @SQL NVARCHAR(1000);
    DECLARE @TCPPorts TABLE
    (PortType NVARCHAR(180), 
     Port     INT
    );
    DECLARE @agname SYSNAME, @listnername VARCHAR(128), @primaryserver VARCHAR(128), @agserverlist VARCHAR(MAX), @agdblist VARCHAR(MAX);
    IF
    (
        SELECT compatibility_level
        FROM sys.databases
        WHERE database_id = 1
    ) >= 110
        BEGIN
            SELECT name AS AGname, 
                   agl.dns_name, 
                   replica_server_name, 
                   ADC.database_name,
                   CASE
                       WHEN(primary_replica = replica_server_name)
                       THEN 1
                       ELSE ''
                   END AS IsPrimaryServer, 
                   secondary_role_allow_connections_desc AS ReadableSecondary, 
                   [availability_mode] AS [Synchronous], 
                   failover_mode_desc, 
                   read_only_routing_url, 
                   availability_mode_desc
            INTO #aginfo
            FROM master.sys.availability_groups Groups
                 LEFT JOIN master.sys.availability_replicas Replicas ON Groups.group_id = Replicas.group_id
                 LEFT JOIN master.sys.dm_hadr_availability_group_states States ON Groups.group_id = States.group_id
                 LEFT JOIN sys.availability_databases_cluster ADC ON ADC.group_id = Groups.group_id
                 LEFT JOIN sys.availability_group_listeners agl ON agl.group_id = groups.group_id;
            IF @@ROWCOUNT = 0
                BEGIN
                    SET @agname = 'No AlwaysON';
                    SET @listnername = 'No AlwaysON';
                    SET @primaryserver = 'No AlwaysON';
                    SET @agserverlist = 'No AlwaysON';
                    SET @agdblist = 'No AlwaysON';
                END;
                ELSE
                BEGIN
                    SELECT DISTINCT TOP 1 @agname = a.AGName, 
                                          @listnername = a.DNS_Name, 
                                          @primaryserver =
                    (
                        SELECT DISTINCT 
                               replica_server_name
                        FROM #aginfo b
                        WHERE IsPrimaryServer = 1
                              AND a.agname = b.agname
                              AND ISNULL(a.dns_name,'') = ISNULL(b.dns_name,'')
                    ), 
                                          @agserverlist = SUBSTRING(
                    (
                        SELECT DISTINCT 
                               ', ' + b.replica_server_name + '(' + failover_mode_desc + ', ' + availability_mode_desc + ')'
                        FROM #aginfo b
                        WHERE a.agname = b.agname
                              AND ISNULL(a.dns_name,'') = ISNULL(b.dns_name,'') FOR XML PATH('')
                    ), 3, 8000), 
                                          @agdblist = SUBSTRING(
                    (
                        SELECT DISTINCT 
                               ', ' + b.database_name
                        FROM #aginfo b
                        WHERE a.agname = b.agname
                              AND ISNULL(a.dns_name,'') = ISNULL(b.dns_name,'')
                        ORDER BY 1 FOR XML PATH('')
                    ), 3, 8000)
                    FROM #aginfo a;
                END;
        END;
        ELSE
        BEGIN
            SET @agname = 'No AlwaysON';
            SET @listnername = 'No AlwaysON';
            SET @primaryserver = 'No AlwaysON';
            SET @agserverlist = 'No AlwaysON';
            SET @agdblist = 'No AlwaysON';
        END;
    IF OBJECT_ID('tempdb..#InstanceName') IS NOT NULL
        DROP TABLE #InstanceName;
    CREATE TABLE #InstanceName
    (Data1        VARCHAR(128), 
     InstanceName VARCHAR(128), 
     Data3        VARCHAR(128)
    );
    INSERT INTO #InstanceName
    EXECUTE xp_regread 
            @rootkey = 'HKEY_LOCAL_MACHINE', 
            @key = 'SOFTWARE\Microsoft\Microsoft SQL Server', 
            @value_name = 'InstalledInstances';
    UPDATE #InstanceName
      SET 
          InstanceName = REPLACE(InstanceName, 'MSSQLSERVER', 'Default');
    DECLARE @Ret_Value INT, @CPU_0_Desc VARCHAR(500), @CPU_0_MHz INTEGER, @CPU_1_Desc VARCHAR(500), @CPU_1_MHz INTEGER, @SystemManufacturer VARCHAR(500), @SystemFamily VARCHAR(100), @SystemProductName VARCHAR(100), @AutoUpdate VARCHAR(100);
    CREATE TABLE #memorydetails
    (indexs INT, 
     name   VARCHAR(30), 
     Value  NVARCHAR(30), 
     CValue NVARCHAR(30)
    );
    INSERT INTO #memorydetails
    EXEC xp_msver 
         PhysicalMemory;
    DECLARE @memory NVARCHAR(30);
    SELECT @memory = Value
    FROM #memorydetails;
    CREATE TABLE #cpudetails
    (indexs INT, 
     name   VARCHAR(30), 
     Value  NVARCHAR(30), 
     CValue NVARCHAR(30)
    );
    DECLARE @SQLDataRoot NVARCHAR(512);
    DECLARE @DefaultData NVARCHAR(512);
    DECLARE @DefaultLog NVARCHAR(512);

    --Installation Root Info
    EXEC master.dbo.xp_instance_regread 
         N'HKEY_LOCAL_MACHINE', 
         N'Software\Microsoft\MSSQLServer\Setup', 
         N'SQLDataRoot', 
         @SQLDataRoot OUTPUT;

    -- SQL Data file Info
    EXEC master.dbo.xp_instance_regread 
         N'HKEY_LOCAL_MACHINE', 
         N'Software\Microsoft\MSSQLServer\MSSQLServer', 
         N'DefaultData', 
         @DefaultData OUTPUT;

    -- SQL Default Default Log file info
    EXEC master.dbo.xp_instance_regread 
         N'HKEY_LOCAL_MACHINE', 
         N'Software\Microsoft\MSSQLServer\MSSQLServer', 
         N'DefaultLog', 
         @DefaultLog OUTPUT;
    DECLARE @BackupPath NVARCHAR(4000);
    EXEC master.dbo.xp_instance_regread 
         N'HKEY_LOCAL_MACHINE', 
         N'Software\Microsoft\MSSQLServer\MSSQLServer', 
         N'BackupDirectory', 
         @BackupPath OUTPUT;
    INSERT INTO #cpudetails
    EXEC xp_msver 
         ProcessorCount;
    DECLARE @ProcessorCount NVARCHAR(30);
    SELECT @ProcessorCount = Value
    FROM #cpudetails;
    EXEC @Ret_Value = master..xp_instance_regread 
         'HKEY_LOCAL_MACHINE', 
         'HARDWARE\DESCRIPTION\System\BIOS', 
         'SystemManufacturer', 
         @param = @SystemManufacturer OUTPUT;
    DECLARE @WindowsCluster VARCHAR(128);
    EXEC master..xp_instance_regread 
         N'HKEY_LOCAL_MACHINE', 
         N'CLUSTER', 
         N'CLUSTERNAME', 
         @param = @WindowsCluster OUTPUT;
    DECLARE @WindowsRDP INT;
    EXEC master..xp_instance_regread 
         N'HKEY_LOCAL_MACHINE', 
         N'System\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp', 
         N'PortNumber', 
         @param = @WindowsRDP OUTPUT;
    EXEC @Ret_Value = master..xp_instance_regread 
         'HKEY_LOCAL_MACHINE', 
         'HARDWARE\DESCRIPTION\System\BIOS', 
         'SystemFamily', 
         @param = @SystemFamily OUTPUT;
    EXEC @Ret_Value = master..xp_instance_regread 
         'HKEY_LOCAL_MACHINE', 
         'HARDWARE\DESCRIPTION\System\BIOS', 
         'SystemProductName', 
         @param = @SystemProductName OUTPUT;
    DECLARE @DBEngineLogin VARCHAR(100);
    DECLARE @AgentLogin VARCHAR(100);
    EXECUTE master.dbo.xp_instance_regread 
            @rootkey = N'HKEY_LOCAL_MACHINE', 
            @key = N'SYSTEM\CurrentControlSet\Services\MSSQLServer', 
            @value_name = N'ObjectName', 
            @value = @DBEngineLogin OUTPUT;
    EXECUTE master.dbo.xp_instance_regread 
            @rootkey = N'HKEY_LOCAL_MACHINE', 
            @key = N'SYSTEM\CurrentControlSet\Services\SQLServerAgent', 
            @value_name = N'ObjectName', 
            @value = @AgentLogin OUTPUT;
    DECLARE @Domain VARCHAR(100), @key VARCHAR(100);
    SET @key = 'SYSTEM\ControlSet001\Services\Tcpip\Parameters\';
    EXEC master..xp_regread 
         @rootkey = 'HKEY_LOCAL_MACHINE', 
         @key = @key, 
         @value_name = 'Domain', 
         @value = @Domain OUTPUT;
    EXECUTE master.dbo.xp_instance_regread 
            'HKEY_LOCAL_MACHINE', 
            'SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU', 
            'AUOptions', 
            @param = @AutoUpdate OUTPUT;
    EXECUTE @Ret_Value = master.dbo.xp_instance_regread 
            'HKEY_LOCAL_MACHINE', 
            'HARDWARE\DESCRIPTION\System\CentralProcessor\0', 
            'ProcessorNameString', 
            @param = @CPU_0_Desc OUTPUT;
    EXECUTE @Ret_Value = master.dbo.xp_instance_regread 
            'HKEY_LOCAL_MACHINE', 
            'HARDWARE\DESCRIPTION\System\CentralProcessor\0', 
            '~MHz', 
            @param = @CPU_0_MHz OUTPUT;
    EXECUTE @Ret_Value = master.dbo.xp_instance_regread 
            'HKEY_LOCAL_MACHINE', 
            'HARDWARE\DESCRIPTION\System\CentralProcessor\1', 
            'ProcessorNameString', 
            @param = @CPU_1_Desc OUTPUT;
    EXECUTE @Ret_Value = master.dbo.xp_instance_regread 
            'HKEY_LOCAL_MACHINE', 
            'HARDWARE\DESCRIPTION\System\CentralProcessor\1', 
            '~MHz', 
            @param = @CPU_1_MHz OUTPUT;
	DECLARE @IFIValue INT;

EXEC master.dbo.xp_regread
    @rootkey = 'HKEY_LOCAL_MACHINE',
    @key = 'SYSTEM\CurrentControlSet\Services\SqlServer',
    @value_name = 'InstantFileInitializationEnabled',
    @value = @IFIValue OUTPUT;
	IF @LogToTable  = 1
	BEGIN
	INSERT INTO tblSQLInformation
    SELECT SERVERPROPERTY('ServerName') ServerName, 
           CONNECTIONPROPERTY('local_tcp_port') PortNumber,
           CASE
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '8%'
               THEN 'SQL Server 2000'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '9%'
               THEN 'SQL Server 2005'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.0%'
               THEN 'SQL Server 2008'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.5%'
               THEN 'SQL Server 2008 R2'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '11%'
               THEN 'SQL Server 2012'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '12%'
               THEN 'SQL Server 2014'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '13%'
               THEN 'SQL Server 2016'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '14%'
               THEN 'SQL Server 2017'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '15%'
               THEN 'SQL Server 2019'
			   WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '16%'
               THEN 'SQL Server 2022'
           END SQLVersionDesc, 
           SERVERPROPERTY(N'ProductVersion') SQLVersion, 
           SERVERPROPERTY('ProductLevel') ServicePack, 
           @agname AGName, 
           @listnername AGListenerName, 
           @primaryserver AGPrimaryServer, 
           @agserverlist AGServerList, 
           @agdblist AGDBList, 
    (
        SELECT COUNT(*)
        FROM #InstanceName
    ) TotalNoOfInstances, 
    (
        SELECT SUBSTRING(
        (
            SELECT ', ' + CONVERT(VARCHAR(10), InstanceName)
            FROM #InstanceName FOR xml PATH('')
        ), 3, 8000)
    ) AllInstancesName,
           --, SERVERPROPERTY('machinename') VirtualServerName 
           SERVERPROPERTY('ComputerNamePhysicalNetBIOS') RunningNode, 
           CONNECTIONPROPERTY('local_net_address') IPAddress, 
           @Domain  + '(' + @domainNames +')' DomainNameList,
           CASE
               WHEN SERVERPROPERTY('IsClustered') = 1
               THEN
    (
        SELECT SUBSTRING(
        (
            SELECT ' ,' + NodeName
            FROM sys.dm_os_cluster_nodes FOR xml PATH('')
        ), 3, 8000)
    )
               WHEN SERVERPROPERTY('IsClustered') = 0
               THEN 'Not Clustered'
           END AllNodes, 
           SERVERPROPERTY(N'Edition') Edition, 
           SERVERPROPERTY('ErrorLogFileName') ErrorLogLocation, 
           @DefaultData AS 'Data Files', 
           @DefaultLog AS 'Log Files',
		   @SQLDataRoot SQLDataRoot,
           @BackupPath DefaultBackup, 
    (
        SELECT COUNT(*)
        FROM sys.sysdatabases
        WHERE dbid > 4
              AND STATUS <> 1073808392
    ) DBCount, 
    (
        SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0))
        FROM sys.master_files
        WHERE is_sparse = 0
              AND database_id <> 2
              AND type_desc = 'ROWS'
    ) TotalDataSizeMB, 
    (
        SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0))
        FROM sys.master_files
        WHERE is_sparse = 0
              AND database_id <> 2
              AND type_desc = 'LOG'
    ) TotalLogSizeMB, 
           SERVERPROPERTY('Collation') ServerCollation, 
    (
        SELECT COUNT(*)
        FROM sys.master_files
        WHERE database_id = 2
              AND type = 0
    ) TempDBDataFileCount, 
           @ProcessorCount ProcessorCount, 
    (
        SELECT value_in_use
        FROM sys.configurations
        WHERE name LIKE 'max degree of parallelism'
    ) MAXDOP,
	(select value from sys.sysconfigures
where comment like 'Cost%') CostThreshold,
           --       @AutoUpdate AutoUpdate,
           @memory TotalMemory, 
    (
        SELECT value_in_use
        FROM sys.configurations
        WHERE name LIKE 'min server memory (MB)'
    ) MinMemory, 
    (
        SELECT value_in_use
        FROM sys.configurations
        WHERE name LIKE 'max server memory (MB)'
    ) MaxMemory,
	(SELECT 
    CASE 
        WHEN sql_memory_model_desc = 'LOCK_PAGES' THEN 'LPIM - Enabled'
        ELSE 'LPIM - Disabled'
    END AS LPIM_Status
FROM sys.dm_os_sys_info) LockPagesInMemory,
(SELECT CONCAT('Total:', @TraceFlagCount, ' (', @TraceFlags, ')') )AS Enabled_Trace_Flags,
(SELECT CONCAT('Total:', @Count, ' (', @NonStandardConfigs, ')') )AS NonStandardConfigurations,
           --, ISNULL(@SystemFamily,'VM') AS SystemFamily 
           @WinName WindowsName, 
           @WindowsRDP WindowsRDPPort,
		  case when @IFIValue = 1 then 
     'Instant file initialization is enabled.'
else 
     'Instant file initialization is disabled.' end As InstantFileInitialization,
           ISNULL(@SystemManufacturer, 'VMware, Inc.') AS SystemManufacturer,
           CASE
               WHEN @SystemManufacturer <> 'VMware, Inc.'
               THEN 'Physical'
               WHEN @SystemManufacturer IS NULL
               THEN 'Virtual'
               WHEN @SystemManufacturer = 'VMware, Inc.'
               THEN 'Virtual'
           END AS [Physica/Virtual], 
           ISNULL(@SystemProductName, 'VMware Virtual Platform') AS SystemProductName, 
           @CPU_0_Desc AS [CPU Description],
           --, @CPU_0_MHz AS [CPU 0 MHz]
           --, @CPU_1_Desc AS [CPU 1 Description]
           --, @CPU_1_MHz AS [CPU 1 MHz]
           CASE
               WHEN SERVERPROPERTY('IsClustered') = 0
               THEN 'No'
               WHEN SERVERPROPERTY('IsClustered') = 1
               THEN 'Yes'
           END IsClustered, 
           ISNULL(@WindowsCluster, 'Not Cluster') WindowsCluster, 
           [DBEngineLogin] = @DBEngineLogin, 
           [AgentLogin] = @AgentLogin, 
    (
        SELECT create_date
        FROM sys.databases
        WHERE name LIKE 'tempdb'
    ) SQLStartTime, 
    (
        SELECT DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE())
        FROM sys.[dm_os_sys_info]
    ) OSRebootTime, 
    (
        SELECT create_date
        FROM sys.server_principals
        WHERE sid = 0x010100000000000512000000
    ) SQLInstallDate,
           --(
           --    SELECT SUBSTRING(
           --                    (
           --                        SELECT ' ,'+QUOTENAME(name)
           --                        FROM sys.sysdatabases
           --                        WHERE dbid > 4 FOR XML PATH('')
           --                    ), 3, 8000)
           --) DBNames,
	   @TimeZone ServerTimeZone,
           GETUTCDATE() RunTimeUTC;

		   WITH RankedRuns AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY RunTimeUTC DESC) AS RowNum
    FROM tblSQLInformation
)
DELETE FROM tblSQLInformation
WHERE RunTimeUTC < (SELECT max(RunTimeUTC) FROM RankedRuns WHERE RowNum = @Retention);

		   END
		   ELSE 
		   BEGIN
		       SELECT SERVERPROPERTY('ServerName') ServerName, 
           CONNECTIONPROPERTY('local_tcp_port') PortNumber,
           CASE
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '8%'
               THEN 'SQL Server 2000'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '9%'
               THEN 'SQL Server 2005'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.0%'
               THEN 'SQL Server 2008'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.5%'
               THEN 'SQL Server 2008 R2'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '11%'
               THEN 'SQL Server 2012'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '12%'
               THEN 'SQL Server 2014'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '13%'
               THEN 'SQL Server 2016'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '14%'
               THEN 'SQL Server 2017'
               WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '15%'
               THEN 'SQL Server 2019'
			   WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '16%'
               THEN 'SQL Server 2022'
           END SQLVersionDesc, 
           SERVERPROPERTY(N'ProductVersion') SQLVersion, 
           SERVERPROPERTY('ProductLevel') ServicePack, 
           @agname AGName, 
           @listnername AGListenerName, 
           @primaryserver AGPrimaryServer, 
           @agserverlist AGServerList, 
           @agdblist AGDBList, 
    (
        SELECT COUNT(*)
        FROM #InstanceName
    ) TotalNoOfInstances, 
    (
        SELECT SUBSTRING(
        (
            SELECT ', ' + CONVERT(VARCHAR(10), InstanceName)
            FROM #InstanceName FOR xml PATH('')
        ), 3, 8000)
    ) AllInstancesName,
           --, SERVERPROPERTY('machinename') VirtualServerName 
           SERVERPROPERTY('ComputerNamePhysicalNetBIOS') RunningNode, 
           CONNECTIONPROPERTY('local_net_address') IPAddress, 
           @Domain  + '(' + @domainNames +')' DomainNameList,
           CASE
               WHEN SERVERPROPERTY('IsClustered') = 1
               THEN
    (
        SELECT SUBSTRING(
        (
            SELECT ' ,' + NodeName
            FROM sys.dm_os_cluster_nodes FOR xml PATH('')
        ), 3, 8000)
    )
               WHEN SERVERPROPERTY('IsClustered') = 0
               THEN 'Not Clustered'
           END AllNodes, 
           SERVERPROPERTY(N'Edition') Edition, 
           SERVERPROPERTY('ErrorLogFileName') ErrorLogLocation, 
           @DefaultData AS 'Data Files', 
           @DefaultLog AS 'Log Files',
		   @SQLDataRoot SQLDataRoot,
           @BackupPath DefaultBackup, 
    (
        SELECT COUNT(*)
        FROM sys.sysdatabases
        WHERE dbid > 4
              AND STATUS <> 1073808392
    ) DBCount, 
    (
        SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0))
        FROM sys.master_files
        WHERE is_sparse = 0
              AND database_id <> 2
              AND type_desc = 'ROWS'
    ) TotalDataSizeMB, 
    (
        SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0))
        FROM sys.master_files
        WHERE is_sparse = 0
              AND database_id <> 2
              AND type_desc = 'LOG'
    ) TotalLogSizeMB, 
           SERVERPROPERTY('Collation') ServerCollation, 
    (
        SELECT COUNT(*)
        FROM sys.master_files
        WHERE database_id = 2
              AND type = 0
    ) TempDBDataFileCount, 
           @ProcessorCount ProcessorCount, 
    (
        SELECT value_in_use
        FROM sys.configurations
        WHERE name LIKE 'max degree of parallelism'
    ) MAXDOP,
	(select value from sys.sysconfigures
where comment like 'Cost%') CostThreshold,
           --       @AutoUpdate AutoUpdate,
           @memory TotalMemory, 
    (
        SELECT value_in_use
        FROM sys.configurations
        WHERE name LIKE 'min server memory (MB)'
    ) MinMemory, 
    (
        SELECT value_in_use
        FROM sys.configurations
        WHERE name LIKE 'max server memory (MB)'
    ) MaxMemory,
	(SELECT 
    CASE 
        WHEN sql_memory_model_desc = 'LOCK_PAGES' THEN 'LPIM - Enabled'
        ELSE 'LPIM - Disabled'
    END AS LPIM_Status
FROM sys.dm_os_sys_info) LockPagesInMemory,
(SELECT CONCAT('Total:', @TraceFlagCount, ' (', @TraceFlags, ')') )AS Enabled_Trace_Flags,
(SELECT CONCAT('Total:', @Count, ' (', @NonStandardConfigs, ')') )AS NonStandardConfigurations,
           --, ISNULL(@SystemFamily,'VM') AS SystemFamily 
           @WinName WindowsName, 
           @WindowsRDP WindowsRDPPort,
		  case when @IFIValue = 1 then 
     'Instant file initialization is enabled.'
else 
     'Instant file initialization is disabled.' end As InstantFileInitialization,
           ISNULL(@SystemManufacturer, 'VMware, Inc.') AS SystemManufacturer,
           CASE
               WHEN @SystemManufacturer <> 'VMware, Inc.'
               THEN 'Physical'
               WHEN @SystemManufacturer IS NULL
               THEN 'Virtual'
               WHEN @SystemManufacturer = 'VMware, Inc.'
               THEN 'Virtual'
           END AS [Physica/Virtual], 
           ISNULL(@SystemProductName, 'VMware Virtual Platform') AS SystemProductName, 
           @CPU_0_Desc AS [CPU Description],
           --, @CPU_0_MHz AS [CPU 0 MHz]
           --, @CPU_1_Desc AS [CPU 1 Description]
           --, @CPU_1_MHz AS [CPU 1 MHz]
           CASE
               WHEN SERVERPROPERTY('IsClustered') = 0
               THEN 'No'
               WHEN SERVERPROPERTY('IsClustered') = 1
               THEN 'Yes'
           END IsClustered, 
           ISNULL(@WindowsCluster, 'Not Cluster') WindowsCluster, 
           [DBEngineLogin] = @DBEngineLogin, 
           [AgentLogin] = @AgentLogin, 
    (
        SELECT create_date
        FROM sys.databases
        WHERE name LIKE 'tempdb'
    ) SQLStartTime, 
    (
        SELECT DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE())
        FROM sys.[dm_os_sys_info]
    ) OSRebootTime, 
    (
        SELECT create_date
        FROM sys.server_principals
        WHERE sid = 0x010100000000000512000000
    ) SQLInstallDate,
           --(
           --    SELECT SUBSTRING(
           --                    (
           --                        SELECT ' ,'+QUOTENAME(name)
           --                        FROM sys.sysdatabases
           --                        WHERE dbid > 4 FOR XML PATH('')
           --                    ), 3, 8000)
           --) DBNames,
	   @TimeZone ServerTimeZone,
           GETUTCDATE() RunTimeUTC;
		   END
END TRY
BEGIN CATCH
    PRINT 'Didn''t work for ' + @@SERVERNAME;
END CATCH;
IF OBJECT_ID('tempdb..#cpudetails') IS NOT NULL
    DROP TABLE #cpudetails;
IF OBJECT_ID('tempdb..#memorydetails') IS NOT NULL
    DROP TABLE #memorydetails;
IF OBJECT_ID('tempdb..#SQLInstances') IS NOT NULL
    DROP TABLE #SQLInstances;
IF OBJECT_ID('tempdb..#WinNames') IS NOT NULL
    DROP TABLE #WinNames;
IF OBJECT_ID('tempdb..#aginfo') IS NOT NULL
    DROP TABLE #aginfo

END
GO
EXEC usp_SQLInformation
GO
SELECT * FROM tblSQLInformation
