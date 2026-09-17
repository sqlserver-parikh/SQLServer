USE tempdb;
GO

SET ANSI_WARNINGS ON;
GO

DROP TABLE IF EXISTS tblSQLInformation;
GO

CREATE OR ALTER PROCEDURE usp_SQLInformation
(
    @LogToTable BIT = 1, 
    @Retention  INT = 26
) 
AS
BEGIN
    SET NOCOUNT ON;

    IF @LogToTable = 1
    BEGIN
        IF NOT EXISTS (
            SELECT *
            FROM sys.objects
            WHERE object_id = OBJECT_ID(N'[dbo].[tblSQLInformation]')
                  AND type IN (N'U')
        )
        CREATE TABLE [dbo].[tblSQLInformation](
            [ServerName] [sql_variant] NULL,
            [PortNumber] [sql_variant] NULL,
            [SQLVersionDesc] [varchar](18) NULL,
            [SQLVersion] [sql_variant] NULL,
            [ServicePack] [sql_variant] NULL,
            [ActiveKB] [varchar](50) NULL,
            [ActiveCU] [varchar](50) NULL,
            [ActivePatchDescription] [varchar](500) NULL,
            [ActivePatchInstallDate] [datetime] NULL,
            [ActivePatchInstalledBy] [varchar](128) NULL,
            [RecentPatchesCount] [int] NULL,
            [RecentPatches] [varchar](max) NULL,
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
            [LockPagesInMemory] [varchar](128) NULL,
            [Enabled_Trace_Flags] [varchar](max) NULL,
            [NonStandardConfigurations] [varchar](max) NULL,
            [WindowsName] [varchar](128) NULL,
            [WindowsRDPPort] [int] NULL,
            [PowerPlan] [varchar](100) NULL,
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
            [ServerTimeZone] [varchar](100) NULL,
            [RunTimeUTC] [datetime] NOT NULL
        ) ON [PRIMARY];
    END;

    DECLARE @DomainNames NVARCHAR(MAX) = '';
    SELECT @DomainNames = STUFF((
        SELECT DISTINCT ', ' + LEFT(name, CHARINDEX('\', name) - 1)
        FROM sys.server_principals
        WHERE type_desc IN ('WINDOWS_LOGIN', 'WINDOWS_GROUP')
          AND name LIKE '%\%' 
          AND name NOT LIKE 'NT %' 
          AND name NOT LIKE 'BUILTIN%'
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '');

    DECLARE @TimeZone VARCHAR(50);
    DECLARE @IsDST BIT;
    DECLARE @UTCOffset VARCHAR(10);

    EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
        'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
        'TimeZoneKeyName',
        @TimeZone OUTPUT;

    DECLARE @PowerPlan VARCHAR(100);
    EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE', 
        'SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes', 
        'ActivePowerScheme', 
        @PowerPlan OUTPUT;

    SET @PowerPlan = CASE 
        WHEN @PowerPlan = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c' THEN 'High Performance'
        WHEN @PowerPlan = '381b4222-f694-41f0-9685-ff5bb260df2e' THEN 'Balanced'
        WHEN @PowerPlan = 'a1841308-3541-4fab-bc81-f71556f20b4a' THEN 'Power Saver'
        ELSE 'Other/Unknown' 
    END;

    EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
        'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
        'ActiveTimeBias',
        @IsDST OUTPUT;

    SET @UTCOffset = (SELECT CONVERT(VARCHAR(30), current_utc_offset) FROM sys.time_zone_info WHERE name = @TimeZone);
    SET @TimeZone = (SELECT name + ' (UTC' + @UTCOffset + '), DST: ' + CASE WHEN @IsDST = 0 THEN 'OFF' ELSE 'ON' END FROM sys.time_zone_info WHERE name = @TimeZone);

    BEGIN TRY
        CREATE TABLE #WinNames
        (
            WinID   VARCHAR(128), 
            WinName VARCHAR(MAX)
        );

        INSERT INTO #WinNames VALUES
        ('5.2 (3790)', 'Windows Server 2003 R2'),
        ('5.2 ()', 'Windows Server 2003 R2'),
        ('6.0 (6002)', 'Windows Server 2008'),
        ('6.1 (7601)', 'Windows Server 2008 R2'),
        ('6.2 (9200)', 'Windows Server 2012'),
        ('6.3 (9600)', 'Windows Server 2012 R2'),
        ('6.3 (14393)', 'Windows Server 2016'),
        ('6.3 (20348)', 'Windows Server 2022'),
        ('10.0 (14393)', 'Windows Server 2016'),
        ('10.0 (17763)', 'Windows Server 2019'),
        ('10.0 (20348)', 'Windows Server 2022'),
        ('6.3 (17763)', 'Windows Server 2022'),
        ('10.0 (10240)', 'Windows 10'),
        ('10.0 (19041)', 'Windows 10'),
        ('10.0 (19042)', 'Windows 10'),
        ('10.0 (19043)', 'Windows 10'),
        ('10.0 (19044)', 'Windows 10'),
        ('10.0 (19045)', 'Windows 10'),
        ('10.0 (22000)', 'Windows 11'),
        ('10.0 (22621)', 'Windows 11');

        DECLARE @config TABLE (
            name NVARCHAR(35),
            default_value SQL_VARIANT
        );

        INSERT INTO @config (name, default_value) VALUES
        ('access check cache bucket count', 0),
        ('access check cache quota', 0),
        ('Ad Hoc Distributed Queries', 0),
        ('affinity I/O mask', 0),
        ('affinity64 I/O mask', 0),
        ('affinity mask', 0),
        ('affinity64 mask', 0),
        ('Agent XPs', 1),
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

        DECLARE @NonStandardConfigs NVARCHAR(MAX);
        DECLARE @Count INT;

        SELECT @NonStandardConfigs = STRING_AGG(
            CONCAT(sc.name, ' (Default: ', CONVERT(NVARCHAR(MAX), c.default_value), ', Current: ', CONVERT(NVARCHAR(MAX), sc.value_in_use), ')'), ', '),
            @Count = COUNT(*)
        FROM sys.configurations sc
        INNER JOIN @config c ON sc.name = c.name
        WHERE sc.value_in_use <> c.default_value;

        DECLARE @TraceFlags VARCHAR(MAX) = '';
        DECLARE @TraceFlagCount INT = 0;

        IF OBJECT_ID('tempdb..#TraceFlags') IS NOT NULL DROP TABLE #TraceFlags;
        CREATE TABLE #TraceFlags (TraceFlag INT, Status INT, Global INT, Session INT);

        INSERT INTO #TraceFlags (TraceFlag, Status, Global, Session)
        EXEC ('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS');

        SELECT @TraceFlags = STRING_AGG(CAST(TraceFlag AS VARCHAR), ', '),
               @TraceFlagCount = COUNT(*)
        FROM #TraceFlags
        WHERE Status = 1;

        DECLARE @Plat TABLE
        (
            Id             INT, 
            Name           VARCHAR(180), 
            InternalValue  VARCHAR(50), 
            Charactervalue VARCHAR(50)
        );
        DECLARE @WinName VARCHAR(128);

        INSERT INTO @Plat EXEC xp_msver WindowsVersion;
        SELECT @WinName = WinName FROM #WinNames A INNER JOIN @Plat B ON A.WinID = B.Charactervalue;
        DELETE FROM @Plat;

        DECLARE @agname SYSNAME, @listnername VARCHAR(128), @primaryserver VARCHAR(128), @agserverlist VARCHAR(MAX), @agdblist VARCHAR(MAX);

        IF (SELECT compatibility_level FROM sys.databases WHERE database_id = 1) >= 110
        BEGIN
            SELECT name AS AGname, 
                   agl.dns_name, 
                   replica_server_name, 
                   ADC.database_name,
                   CASE WHEN (primary_replica = replica_server_name) THEN 1 ELSE 0 END AS IsPrimaryServer, 
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
            END
            ELSE
            BEGIN
                SELECT DISTINCT TOP 1 
                    @agname = a.AGName, 
                    @listnername = a.DNS_Name, 
                    @primaryserver = (
                        SELECT DISTINCT replica_server_name
                        FROM #aginfo b
                        WHERE IsPrimaryServer = 1
                              AND a.agname = b.agname
                              AND ISNULL(a.dns_name,'') = ISNULL(b.dns_name,'')
                    ), 
                    @agserverlist = SUBSTRING((
                        SELECT DISTINCT ', ' + b.replica_server_name + '(' + failover_mode_desc + ', ' + availability_mode_desc + ')'
                        FROM #aginfo b
                        WHERE a.agname = b.agname
                              AND ISNULL(a.dns_name,'') = ISNULL(b.dns_name,'') 
                        FOR XML PATH('')
                    ), 3, 8000), 
                    @agdblist = SUBSTRING((
                        SELECT DISTINCT ', ' + b.database_name
                        FROM #aginfo b
                        WHERE a.agname = b.agname
                              AND ISNULL(a.dns_name,'') = ISNULL(b.dns_name,'')
                        ORDER BY 1 
                        FOR XML PATH('')
                    ), 3, 8000)
                FROM #aginfo a;
            END;
        END
        ELSE
        BEGIN
            SET @agname = 'No AlwaysON';
            SET @listnername = 'No AlwaysON';
            SET @primaryserver = 'No AlwaysON';
            SET @agserverlist = 'No AlwaysON';
            SET @agdblist = 'No AlwaysON';
        END;

        IF OBJECT_ID('tempdb..#InstanceName') IS NOT NULL DROP TABLE #InstanceName;
        CREATE TABLE #InstanceName
        (
            Data1        VARCHAR(128), 
            InstanceName VARCHAR(128), 
            Data3        VARCHAR(128)
        );

        INSERT INTO #InstanceName
        EXECUTE xp_regread 
            @rootkey = 'HKEY_LOCAL_MACHINE', 
            @key = 'SOFTWARE\Microsoft\Microsoft SQL Server', 
            @value_name = 'InstalledInstances';

        UPDATE #InstanceName SET InstanceName = REPLACE(InstanceName, 'MSSQLSERVER', 'Default');

        DECLARE @Ret_Value INT, @CPU_0_Desc VARCHAR(500), @CPU_0_MHz INTEGER, @CPU_1_Desc VARCHAR(500), @CPU_1_MHz INTEGER, @SystemManufacturer VARCHAR(500), @SystemFamily VARCHAR(100), @SystemProductName VARCHAR(100), @AutoUpdate VARCHAR(100);

        CREATE TABLE #memorydetails
        (
            indexs INT, 
            name   VARCHAR(30), 
            Value  NVARCHAR(30), 
            CValue NVARCHAR(30)
        );
        INSERT INTO #memorydetails EXEC xp_msver PhysicalMemory;
        DECLARE @memory NVARCHAR(30);
        SELECT @memory = Value FROM #memorydetails;

        CREATE TABLE #cpudetails
        (
            indexs INT, 
            name   VARCHAR(30), 
            Value  NVARCHAR(30), 
            CValue NVARCHAR(30)
        );

        DECLARE @SQLDataRoot NVARCHAR(512);
        DECLARE @DefaultData NVARCHAR(512);
        DECLARE @DefaultLog NVARCHAR(512);
        DECLARE @BackupPath NVARCHAR(4000);

        EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\Setup', N'SQLDataRoot', @SQLDataRoot OUTPUT;
        EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @DefaultData OUTPUT;
        EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @DefaultLog OUTPUT;
        EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'BackupDirectory', @BackupPath OUTPUT;

        INSERT INTO #cpudetails EXEC xp_msver ProcessorCount;
        DECLARE @ProcessorCount NVARCHAR(30);
        SELECT @ProcessorCount = Value FROM #cpudetails;

        EXEC @Ret_Value = master..xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\BIOS', 'SystemManufacturer', @param = @SystemManufacturer OUTPUT;
        
        DECLARE @WindowsCluster VARCHAR(128);
        EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'CLUSTER', N'CLUSTERNAME', @param = @WindowsCluster OUTPUT;

        DECLARE @WindowsRDP INT;
        EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'System\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp', N'PortNumber', @param = @WindowsRDP OUTPUT;

        EXEC @Ret_Value = master..xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\BIOS', 'SystemFamily', @param = @SystemFamily OUTPUT;
        EXEC @Ret_Value = master..xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\BIOS', 'SystemProductName', @param = @SystemProductName OUTPUT;

        DECLARE @DBEngineLogin VARCHAR(100);
        DECLARE @AgentLogin VARCHAR(100);
        EXECUTE master.dbo.xp_instance_regread @rootkey = N'HKEY_LOCAL_MACHINE', @key = N'SYSTEM\CurrentControlSet\Services\MSSQLServer', @value_name = N'ObjectName', @value = @DBEngineLogin OUTPUT;
        EXECUTE master.dbo.xp_instance_regread @rootkey = N'HKEY_LOCAL_MACHINE', @key = N'SYSTEM\CurrentControlSet\Services\SQLServerAgent', @value_name = N'ObjectName', @value = @AgentLogin OUTPUT;

        DECLARE @Domain VARCHAR(100), @key VARCHAR(100);
        SET @key = 'SYSTEM\ControlSet001\Services\Tcpip\Parameters\';
        EXEC master..xp_regread @rootkey = 'HKEY_LOCAL_MACHINE', @key = @key, @value_name = 'Domain', @value = @Domain OUTPUT;
        EXECUTE master.dbo.xp_instance_regread 'HKEY_LOCAL_MACHINE', 'SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU', 'AUOptions', @param = @AutoUpdate OUTPUT;
        
        EXECUTE @Ret_Value = master.dbo.xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\CentralProcessor\0', 'ProcessorNameString', @param = @CPU_0_Desc OUTPUT;
        EXECUTE @Ret_Value = master.dbo.xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\CentralProcessor\0', '~MHz', @param = @CPU_0_MHz OUTPUT;
        EXECUTE @Ret_Value = master.dbo.xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\CentralProcessor\1', 'ProcessorNameString', @param = @CPU_1_Desc OUTPUT;
        EXECUTE @Ret_Value = master.dbo.xp_instance_regread 'HKEY_LOCAL_MACHINE', 'HARDWARE\DESCRIPTION\System\CentralProcessor\1', '~MHz', @param = @CPU_1_MHz OUTPUT;

        DECLARE @IFIValue INT;
        EXEC master.dbo.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE', @key = 'SYSTEM\CurrentControlSet\Services\SqlServer', @value_name = 'InstantFileInitializationEnabled', @value = @IFIValue OUTPUT;

        -- =========================================================================
        -- PATCH & HOTFIX ENUMERATION (Registry + Active Instance Engine Properties)
        -- =========================================================================
        DECLARE @ActiveKB VARCHAR(50) = CAST(SERVERPROPERTY('ProductUpdateReference') AS VARCHAR(50));
        DECLARE @ActiveCU VARCHAR(50) = CAST(SERVERPROPERTY('ProductUpdateLevel') AS VARCHAR(50));
        DECLARE @ActivePatchDesc VARCHAR(500) = NULL;
        DECLARE @ActivePatchInstallDate DATETIME = NULL;
        DECLARE @ActivePatchInstalledBy VARCHAR(128) = NULL;
        DECLARE @RecentPatchesCount INT = 0;
        DECLARE @RecentPatches VARCHAR(MAX) = '';

        IF OBJECT_ID('tempdb..#RegKeys') IS NOT NULL DROP TABLE #RegKeys;
        IF OBJECT_ID('tempdb..#RawRegistryData') IS NOT NULL DROP TABLE #RawRegistryData;
        IF OBJECT_ID('tempdb..#Patches') IS NOT NULL DROP TABLE #Patches;

        CREATE TABLE #RegKeys (
            SubKey NVARCHAR(512),
            SourcePath NVARCHAR(1024)
        );

        CREATE TABLE #RawRegistryData (
            SubKey NVARCHAR(512),
            DisplayName NVARCHAR(512),
            InstallDate NVARCHAR(100),
            InstalledBy NVARCHAR(512)
        );

        CREATE TABLE #Patches (
            PSComputerName NVARCHAR(255),
            ParsedInstallDate DATETIME,
            [Description] NVARCHAR(512),
            HotFixID NVARCHAR(100),
            InstalledBy NVARCHAR(512),
            IsActiveInstancePatch INT
        );

        DECLARE @PathNormal NVARCHAR(1024) = 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall';
        DECLARE @PathWow6432 NVARCHAR(1024) = 'SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall';
        CREATE TABLE #TempKeys (SubKey NVARCHAR(512));

        BEGIN TRY
            INSERT INTO #TempKeys EXEC master.dbo.xp_instance_regenumkeys 'HKEY_LOCAL_MACHINE', @PathNormal;
            INSERT INTO #RegKeys (SubKey, SourcePath) SELECT SubKey, @PathNormal FROM #TempKeys;
            TRUNCATE TABLE #TempKeys;
        END TRY BEGIN CATCH END CATCH;

        BEGIN TRY
            INSERT INTO #TempKeys EXEC master.dbo.xp_instance_regenumkeys 'HKEY_LOCAL_MACHINE', @PathWow6432;
            INSERT INTO #RegKeys (SubKey, SourcePath) SELECT SubKey, @PathWow6432 FROM #TempKeys;
        END TRY BEGIN CATCH END CATCH;

        DROP TABLE #TempKeys;

        DECLARE @SubKey NVARCHAR(512);
        DECLARE @SourcePath NVARCHAR(1024);
        DECLARE @CurrentPath NVARCHAR(1024);
        DECLARE @DisplayName NVARCHAR(512);
        DECLARE @InstallDate NVARCHAR(100);
        DECLARE @InstalledBy NVARCHAR(512);

        DECLARE reg_cursor CURSOR LOCAL FAST_FORWARD FOR 
        SELECT SubKey, SourcePath FROM #RegKeys;

        OPEN reg_cursor;
        FETCH NEXT FROM reg_cursor INTO @SubKey, @SourcePath;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @CurrentPath = @SourcePath + '\' + @SubKey;
            SET @DisplayName = NULL;
            SET @InstallDate = NULL;
            SET @InstalledBy = NULL;

            BEGIN TRY
                EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE', @CurrentPath, 'DisplayName', @DisplayName OUTPUT;
            END TRY BEGIN CATCH END CATCH;

            IF @DisplayName IS NOT NULL
            BEGIN
                BEGIN TRY
                    EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE', @CurrentPath, 'InstallDate', @InstallDate OUTPUT;
                END TRY BEGIN CATCH END CATCH;

                BEGIN TRY
                    EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE', @CurrentPath, 'InstalledBy', @InstalledBy OUTPUT;
                END TRY BEGIN CATCH END CATCH;

                INSERT INTO #RawRegistryData (SubKey, DisplayName, InstallDate, InstalledBy)
                VALUES (@SubKey, @DisplayName, @InstallDate, @InstalledBy);
            END

            FETCH NEXT FROM reg_cursor INTO @SubKey, @SourcePath;
        END

        CLOSE reg_cursor;
        DEALLOCATE reg_cursor;

        ;WITH RawFiltered AS (
            SELECT 
                DisplayName,
                SubKey,
                InstalledBy,
                LTRIM(RTRIM(DisplayName)) AS CleanDisplayName,
                LTRIM(RTRIM(InstallDate)) AS RawInstallDate,
                CASE 
                    WHEN DisplayName LIKE '%KB[0-9]%' THEN 
                        SUBSTRING(
                            DisplayName, 
                            PATINDEX('%KB[0-9]%', DisplayName), 
                            PATINDEX('%[^0-9]%', SUBSTRING(DisplayName, PATINDEX('%KB[0-9]%', DisplayName) + 2, LEN(DisplayName)) + ' ') + 1
                        )
                    WHEN SubKey LIKE '%KB[0-9]%' THEN 
                        SUBSTRING(
                            SubKey, 
                            PATINDEX('%KB[0-9]%', SubKey), 
                            PATINDEX('%[^0-9]%', SUBSTRING(SubKey, PATINDEX('%KB[0-9]%', SubKey) + 2, LEN(SubKey)) + ' ') + 1
                        )
                    ELSE 'SQL-Patch'
                END AS HotFixID
            FROM #RawRegistryData
            WHERE (DisplayName LIKE '%SQL Server%' OR DisplayName LIKE '%KB[0-9]%')
              AND (
                  DisplayName LIKE '%Update%' 
                  OR DisplayName LIKE '%Hotfix%' 
                  OR DisplayName LIKE '%CU[0-9]%' 
                  OR DisplayName LIKE '%Cumulative Update%' 
                  OR DisplayName LIKE '%GDR%' 
                  OR DisplayName LIKE '%Service Pack%'
              )
        ),
        ParsedPatches AS (
            SELECT 
                CleanDisplayName AS [Description],
                HotFixID,
                ISNULL(NULLIF(InstalledBy, ''), 'NT AUTHORITY\SYSTEM') AS InstalledBy,
                CASE 
                    WHEN RawInstallDate LIKE '[1-2][0-9][0-9][0-9][0-1][0-9][0-3][0-9]' THEN 
                        TRY_CONVERT(DATETIME, 
                            SUBSTRING(RawInstallDate, 1, 4) + '-' + 
                            SUBSTRING(RawInstallDate, 5, 2) + '-' + 
                            SUBSTRING(RawInstallDate, 7, 2)
                        )
                    ELSE TRY_CONVERT(DATETIME, RawInstallDate)
                END AS ParsedInstallDate
            FROM RawFiltered
        ),
        ActiveInstanceProperty AS (
            SELECT 
                CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(255)) AS PSComputerName,
                @ActiveKB AS ActiveKB,
                @ActiveCU AS ActiveCU,
                CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(50)) AS ProductVersion
        ),
        CombinedPatchSet AS (
            SELECT 
                HOST_NAME() AS PSComputerName,
                ParsedInstallDate,
                [Description],
                HotFixID,
                InstalledBy,
                CASE 
                    WHEN @ActiveKB IS NOT NULL AND HotFixID = @ActiveKB THEN 1 
                    ELSE 0 
                END AS IsActiveInstancePatch
            FROM ParsedPatches
            WHERE ParsedInstallDate IS NULL 
               OR ParsedInstallDate >= DATEADD(YEAR, -1, GETDATE())
            UNION ALL
            SELECT 
                a.PSComputerName,
                NULL AS ParsedInstallDate,
                'SQL Server Active Engine ' + ISNULL(a.ActiveCU, 'RTM') + ' (' + a.ProductVersion + ')' AS [Description],
                a.ActiveKB AS HotFixID,
                'SQL Engine Setup' AS InstalledBy,
                1 AS IsActiveInstancePatch
            FROM ActiveInstanceProperty a
            WHERE a.ActiveKB IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM ParsedPatches p WHERE p.HotFixID = a.ActiveKB
              )
        ),
        RankedPatches AS (
            SELECT 
                PSComputerName,
                ParsedInstallDate,
                [Description],
                HotFixID,
                InstalledBy,
                IsActiveInstancePatch,
                ROW_NUMBER() OVER (
                    PARTITION BY PSComputerName, HotFixID 
                    ORDER BY ParsedInstallDate DESC
                ) AS RowNum
            FROM CombinedPatchSet
        )
        INSERT INTO #Patches (PSComputerName, ParsedInstallDate, [Description], HotFixID, InstalledBy, IsActiveInstancePatch)
        SELECT 
            PSComputerName,
            ParsedInstallDate,
            [Description],
            HotFixID,
            InstalledBy,
            IsActiveInstancePatch
        FROM RankedPatches
        WHERE RowNum = 1;

        -- Extract active patch metadata
        SELECT TOP 1
            @ActivePatchDesc = [Description],
            @ActivePatchInstallDate = ParsedInstallDate,
            @ActivePatchInstalledBy = InstalledBy
        FROM #Patches
        WHERE IsActiveInstancePatch = 1
        ORDER BY ParsedInstallDate DESC;

        IF @ActivePatchDesc IS NULL AND (@ActiveKB IS NOT NULL OR @ActiveCU IS NOT NULL)
        BEGIN
            SET @ActivePatchDesc = 'SQL Server Active Engine ' + ISNULL(@ActiveCU, 'RTM') + ' (' + CONVERT(VARCHAR(50), SERVERPROPERTY('ProductVersion')) + ')';
            SET @ActivePatchInstalledBy = 'SQL Engine Setup';
        END;

        -- Aggregate recent patches applied within the last 1 year
        SELECT 
            @RecentPatches = STRING_AGG(
                CONCAT(
                    HotFixID, 
                    ' (', 
                    ISNULL(CONVERT(VARCHAR(10), ParsedInstallDate, 120), 'Unknown Date'), 
                    CASE WHEN IsActiveInstancePatch = 1 THEN ' [ACTIVE]' ELSE '' END, 
                    ')'
                ), 
                ', '
            ),
            @RecentPatchesCount = COUNT(*)
        FROM (
            SELECT TOP 100 HotFixID, ParsedInstallDate, IsActiveInstancePatch
            FROM #Patches
            ORDER BY IsActiveInstancePatch DESC, ParsedInstallDate DESC
        ) p;

        IF @RecentPatches IS NOT NULL
            SET @RecentPatches = CONCAT('Total: ', @RecentPatchesCount, ' (', @RecentPatches, ')');
        ELSE
            SET @RecentPatches = 'None';

        -- =========================================================================
        -- INSERT OR SELECT RESULT SET
        -- =========================================================================
        IF @LogToTable = 1
        BEGIN
            INSERT INTO tblSQLInformation
            SELECT 
                SERVERPROPERTY('ServerName') AS ServerName, 
                CONNECTIONPROPERTY('local_tcp_port') AS PortNumber,
                CASE
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '8%' THEN 'SQL Server 2000'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '9%' THEN 'SQL Server 2005'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.0%' THEN 'SQL Server 2008'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.5%' THEN 'SQL Server 2008 R2'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '11%' THEN 'SQL Server 2012'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '12%' THEN 'SQL Server 2014'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '13%' THEN 'SQL Server 2016'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '14%' THEN 'SQL Server 2017'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '15%' THEN 'SQL Server 2019'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '16%' THEN 'SQL Server 2022'
                END AS SQLVersionDesc, 
                SERVERPROPERTY(N'ProductVersion') AS SQLVersion, 
                SERVERPROPERTY('ProductLevel') AS ServicePack, 
                @ActiveKB AS ActiveKB,
                @ActiveCU AS ActiveCU,
                @ActivePatchDesc AS ActivePatchDescription,
                @ActivePatchInstallDate AS ActivePatchInstallDate,
                @ActivePatchInstalledBy AS ActivePatchInstalledBy,
                @RecentPatchesCount AS RecentPatchesCount,
                @RecentPatches AS RecentPatches,
                @agname AS AGName, 
                @listnername AS AGListenerName, 
                @primaryserver AS AGPrimaryServer, 
                @agserverlist AS AGServerList, 
                @agdblist AS AGDBList, 
                (SELECT COUNT(*) FROM #InstanceName) AS TotalNoOfInstances, 
                (SELECT SUBSTRING((SELECT ', ' + CONVERT(VARCHAR(10), InstanceName) FROM #InstanceName FOR XML PATH('')), 3, 8000)) AS AllInstancesName,
                SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS RunningNode, 
                CONNECTIONPROPERTY('local_net_address') AS IPAddress, 
                @Domain + '(' + @domainNames + ')' AS DomainNameList,
                CASE
                    WHEN SERVERPROPERTY('IsClustered') = 1 THEN (SELECT SUBSTRING((SELECT ' ,' + NodeName FROM sys.dm_os_cluster_nodes FOR XML PATH('')), 3, 8000))
                    WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'Not Clustered'
                END AS AllNodes, 
                SERVERPROPERTY(N'Edition') AS Edition, 
                SERVERPROPERTY('ErrorLogFileName') AS ErrorLogLocation, 
                @DefaultData AS [Data Files], 
                @DefaultLog AS [Log Files],
                @SQLDataRoot AS SQLDataRoot,
                @BackupPath AS DefaultBackup, 
                (SELECT COUNT(*) FROM sys.sysdatabases WHERE dbid > 4 AND status <> 1073808392) AS DBCount, 
                (SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0)) FROM sys.master_files WHERE is_sparse = 0 AND database_id <> 2 AND type_desc = 'ROWS') AS TotalDataSizeMB, 
                (SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0)) FROM sys.master_files WHERE is_sparse = 0 AND database_id <> 2 AND type_desc = 'LOG') AS TotalLogSizeMB, 
                SERVERPROPERTY('Collation') AS ServerCollation, 
                (SELECT COUNT(*) FROM sys.master_files WHERE database_id = 2 AND type = 0) AS TempDBDataFileCount, 
                @ProcessorCount AS ProcessorCount, 
                (SELECT value_in_use FROM sys.configurations WHERE name LIKE 'max degree of parallelism') AS MAXDOP,
                (SELECT value FROM sys.sysconfigures WHERE comment LIKE 'Cost%') AS CostThreshold,
                @memory AS TotalMemory, 
                (SELECT value_in_use FROM sys.configurations WHERE name LIKE 'min server memory (MB)') AS MinMemory, 
                (SELECT value_in_use FROM sys.configurations WHERE name LIKE 'max server memory (MB)') AS MaxMemory,
                (SELECT CASE WHEN sql_memory_model_desc = 'LOCK_PAGES' THEN 'LPIM - Enabled' ELSE 'LPIM - Disabled' END FROM sys.dm_os_sys_info) AS LockPagesInMemory,
                CONCAT('Total:', @TraceFlagCount, ' (', @TraceFlags, ')') AS Enabled_Trace_Flags,
                CONCAT('Total:', @Count, ' (', @NonStandardConfigs, ')') AS NonStandardConfigurations,
                @WinName AS WindowsName, 
                @WindowsRDP AS WindowsRDPPort,
                @PowerPlan AS PowerPlan,
                CASE WHEN @IFIValue = 1 THEN 'Instant file initialization is enabled.' ELSE 'Instant file initialization is disabled.' END AS InstantFileInitialization,
                ISNULL(@SystemManufacturer, 'VMware, Inc.') AS SystemManufacturer,
                CASE
                    WHEN @SystemManufacturer <> 'VMware, Inc.' THEN 'Physical'
                    WHEN @SystemManufacturer IS NULL OR @SystemManufacturer = 'VMware, Inc.' THEN 'Virtual'
                END AS [Physica/Virtual], 
                ISNULL(@SystemProductName, 'VMware Virtual Platform') AS SystemProductName, 
                @CPU_0_Desc AS [CPU Description],
                CASE
                    WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'No'
                    WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'Yes'
                END AS IsClustered, 
                ISNULL(@WindowsCluster, 'Not Cluster') AS WindowsCluster, 
                @DBEngineLogin AS [DBEngineLogin], 
                @AgentLogin AS [AgentLogin], 
                (SELECT create_date FROM sys.databases WHERE name LIKE 'tempdb') AS SQLStartTime, 
                (SELECT DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE()) FROM sys.[dm_os_sys_info]) AS OSRebootTime, 
                (SELECT create_date FROM sys.server_principals WHERE sid = 0x010100000000000512000000) AS SQLInstallDate,
                @TimeZone AS ServerTimeZone,
                GETUTCDATE() AS RunTimeUTC;

            -- Retention Cleanup
            WITH RankedRuns AS (
                SELECT *,
                       ROW_NUMBER() OVER (ORDER BY RunTimeUTC DESC) AS RowNum
                FROM tblSQLInformation
            )
            DELETE FROM tblSQLInformation
            WHERE RunTimeUTC < (SELECT MAX(RunTimeUTC) FROM RankedRuns WHERE RowNum = @Retention);
        END
        ELSE 
        BEGIN
            SELECT 
                SERVERPROPERTY('ServerName') AS ServerName, 
                CONNECTIONPROPERTY('local_tcp_port') AS PortNumber,
                CASE
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '8%' THEN 'SQL Server 2000'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '9%' THEN 'SQL Server 2005'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.0%' THEN 'SQL Server 2008'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '10.5%' THEN 'SQL Server 2008 R2'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '11%' THEN 'SQL Server 2012'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '12%' THEN 'SQL Server 2014'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '13%' THEN 'SQL Server 2016'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '14%' THEN 'SQL Server 2017'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '15%' THEN 'SQL Server 2019'
                    WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) LIKE '16%' THEN 'SQL Server 2022'
                END AS SQLVersionDesc, 
                SERVERPROPERTY(N'ProductVersion') AS SQLVersion, 
                SERVERPROPERTY('ProductLevel') AS ServicePack, 
                @ActiveKB AS ActiveKB,
                @ActiveCU AS ActiveCU,
                @ActivePatchDesc AS ActivePatchDescription,
                @ActivePatchInstallDate AS ActivePatchInstallDate,
                @ActivePatchInstalledBy AS ActivePatchInstalledBy,
                @RecentPatchesCount AS RecentPatchesCount,
                @RecentPatches AS RecentPatches,
                @agname AS AGName, 
                @listnername AS AGListenerName, 
                @primaryserver AS AGPrimaryServer, 
                @agserverlist AS AGServerList, 
                @agdblist AS AGDBList, 
                (SELECT COUNT(*) FROM #InstanceName) AS TotalNoOfInstances, 
                (SELECT SUBSTRING((SELECT ', ' + CONVERT(VARCHAR(10), InstanceName) FROM #InstanceName FOR XML PATH('')), 3, 8000)) AS AllInstancesName,
                SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS RunningNode, 
                CONNECTIONPROPERTY('local_net_address') AS IPAddress, 
                @Domain + '(' + @domainNames + ')' AS DomainNameList,
                CASE
                    WHEN SERVERPROPERTY('IsClustered') = 1 THEN (SELECT SUBSTRING((SELECT ' ,' + NodeName FROM sys.dm_os_cluster_nodes FOR XML PATH('')), 3, 8000))
                    WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'Not Clustered'
                END AS AllNodes, 
                SERVERPROPERTY(N'Edition') AS Edition, 
                SERVERPROPERTY('ErrorLogFileName') AS ErrorLogLocation, 
                @DefaultData AS [Data Files], 
                @DefaultLog AS [Log Files],
                @SQLDataRoot AS SQLDataRoot,
                @BackupPath AS DefaultBackup, 
                (SELECT COUNT(*) FROM sys.sysdatabases WHERE dbid > 4 AND status <> 1073808392) AS DBCount, 
                (SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0)) FROM sys.master_files WHERE is_sparse = 0 AND database_id <> 2 AND type_desc = 'ROWS') AS TotalDataSizeMB, 
                (SELECT CONVERT(DECIMAL(25, 0), SUM(size / 128.0)) FROM sys.master_files WHERE is_sparse = 0 AND database_id <> 2 AND type_desc = 'LOG') AS TotalLogSizeMB, 
                SERVERPROPERTY('Collation') AS ServerCollation, 
                (SELECT COUNT(*) FROM sys.master_files WHERE database_id = 2 AND type = 0) AS TempDBDataFileCount, 
                @ProcessorCount AS ProcessorCount, 
                (SELECT value_in_use FROM sys.configurations WHERE name LIKE 'max degree of parallelism') AS MAXDOP,
                (SELECT value FROM sys.sysconfigures WHERE comment LIKE 'Cost%') AS CostThreshold,
                @memory AS TotalMemory, 
                (SELECT value_in_use FROM sys.configurations WHERE name LIKE 'min server memory (MB)') AS MinMemory, 
                (SELECT value_in_use FROM sys.configurations WHERE name LIKE 'max server memory (MB)') AS MaxMemory,
                (SELECT CASE WHEN sql_memory_model_desc = 'LOCK_PAGES' THEN 'LPIM - Enabled' ELSE 'LPIM - Disabled' END FROM sys.dm_os_sys_info) AS LockPagesInMemory,
                CONCAT('Total:', @TraceFlagCount, ' (', @TraceFlags, ')') AS Enabled_Trace_Flags,
                CONCAT('Total:', @Count, ' (', @NonStandardConfigs, ')') AS NonStandardConfigurations,
                @WinName AS WindowsName, 
                @WindowsRDP AS WindowsRDPPort,
                @PowerPlan AS PowerPlan,
                CASE WHEN @IFIValue = 1 THEN 'Instant file initialization is enabled.' ELSE 'Instant file initialization is disabled.' END AS InstantFileInitialization,
                ISNULL(@SystemManufacturer, 'VMware, Inc.') AS SystemManufacturer,
                CASE
                    WHEN @SystemManufacturer <> 'VMware, Inc.' THEN 'Physical'
                    WHEN @SystemManufacturer IS NULL OR @SystemManufacturer = 'VMware, Inc.' THEN 'Virtual'
                END AS [Physica/Virtual], 
                ISNULL(@SystemProductName, 'VMware Virtual Platform') AS SystemProductName, 
                @CPU_0_Desc AS [CPU Description],
                CASE
                    WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'No'
                    WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'Yes'
                END AS IsClustered, 
                ISNULL(@WindowsCluster, 'Not Cluster') AS WindowsCluster, 
                @DBEngineLogin AS [DBEngineLogin], 
                @AgentLogin AS [AgentLogin], 
                (SELECT create_date FROM sys.databases WHERE name LIKE 'tempdb') AS SQLStartTime, 
                (SELECT DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE()) FROM sys.[dm_os_sys_info]) AS OSRebootTime, 
                (SELECT create_date FROM sys.server_principals WHERE sid = 0x010100000000000512000000) AS SQLInstallDate,
                @TimeZone AS ServerTimeZone,
                GETUTCDATE() AS RunTimeUTC;
        END;

    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'reg_cursor') >= 0
        BEGIN
            CLOSE reg_cursor;
            DEALLOCATE reg_cursor;
        END;
        PRINT 'Execution error on ' + @@SERVERNAME + ': ' + ERROR_MESSAGE();
    END CATCH;

    -- Cleanup Temporary Tables
    IF OBJECT_ID('tempdb..#cpudetails') IS NOT NULL DROP TABLE #cpudetails;
    IF OBJECT_ID('tempdb..#memorydetails') IS NOT NULL DROP TABLE #memorydetails;
    IF OBJECT_ID('tempdb..#InstanceName') IS NOT NULL DROP TABLE #InstanceName;
    IF OBJECT_ID('tempdb..#WinNames') IS NOT NULL DROP TABLE #WinNames;
    IF OBJECT_ID('tempdb..#aginfo') IS NOT NULL DROP TABLE #aginfo;
    IF OBJECT_ID('tempdb..#TraceFlags') IS NOT NULL DROP TABLE #TraceFlags;
    IF OBJECT_ID('tempdb..#RegKeys') IS NOT NULL DROP TABLE #RegKeys;
    IF OBJECT_ID('tempdb..#RawRegistryData') IS NOT NULL DROP TABLE #RawRegistryData;
    IF OBJECT_ID('tempdb..#Patches') IS NOT NULL DROP TABLE #Patches;
END;
GO

-- Execute and verify
EXEC usp_SQLInformation @LogToTable = 1, @Retention = 26;
GO

SELECT 
  *
FROM tblSQLInformation;
GO
