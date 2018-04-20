USE DBATasks;
GO
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[spMemoryConfig]')
          AND type IN(N'P', N'PC')
)
    BEGIN
        DROP PROCEDURE [dbo].[spMemoryConfig];
    END;
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
IF NOT EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[spMemoryConfig]')
          AND type IN(N'P', N'PC')
)
    BEGIN
        EXEC dbo.sp_executesql
             @statement = N'CREATE PROCEDURE [dbo].[spMemoryConfig] AS';
    END;
GO
ALTER PROCEDURE [dbo].[spMemoryConfig]
(@InstanceCount INT = 1,
 @MinMemoryPCT  INT = 25,
 @PrintOnly     BIT = 1
)
AS
         BEGIN
             DECLARE @max_memory INT, @min_memory INT, @os_memory INT, @total_memory INT, @current_value SQL_VARIANT;
             SELECT @current_value = value_in_use
             FROM sys.configurations
             WHERE name LIKE 'show advanced options';
             IF
(
    SELECT total_physical_memory_kb / 1024
    FROM sys.dm_os_sys_memory
) <= 8192
                 BEGIN
                     SET @os_memory = 2048;
                 END;
                 ELSE
                 BEGIN
                     IF
(
    SELECT total_physical_memory_kb / 1024
    FROM sys.dm_os_sys_memory
) <= 32768
                         BEGIN
                             SET @os_memory = 4096;
                         END;
                         ELSE
                         BEGIN
                             IF
(
    SELECT total_physical_memory_kb / 1024
    FROM sys.dm_os_sys_memory
) <= 65536
                                 BEGIN
                                     SET @os_memory = 8192;
                                 END;
                                 ELSE
                                 BEGIN
                                     SET @os_memory = 12288;
                                 END;
                         END;
                 END;
             SET @max_memory = (
(
    SELECT total_physical_memory_kb / 1024
    FROM sys.dm_os_sys_memory
) - @os_memory) / @InstanceCount;
             DECLARE @Version NUMERIC(18, 10);
             SET @Version = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)))-1)+'.'+REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)))-CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(MAX)))), '.', '') AS NUMERIC(18, 10));
             IF(@max_memory > 128 * 1024
                AND SERVERPROPERTY('EDITIONID') NOT IN(1804890536, 1872460670, 610778273, -2117995310)
                AND @Version > 12)
                 SET @max_memory = 128 * 1024;
             IF(@max_memory > 64 * 1024
                AND SERVERPROPERTY('EDITIONID') NOT IN(1804890536, 1872460670, 610778273, -2117995310)
                AND @Version < 12)
                 SET @max_memory = 64 * 1024;
             SET @min_memory = FLOOR(@max_memory * @MinMemoryPCT / 100 / 1024.0) * 1024;
             SELECT @total_memory = CONVERT(VARCHAR(8), total_physical_memory_kb / 1024)
             FROM sys.dm_os_sys_memory;
             PRINT '--Total Server Memory: '+CONVERT(VARCHAR(8), @total_memory)+' MB';
             PRINT '--Max SQL Memory: '+CONVERT(VARCHAR(8), @max_memory)+' MB';
             PRINT '--Min SQL Memory: '+CONVERT(VARCHAR(8), @min_memory)+' MB';
             PRINT '--OS Memory: '+CONVERT(VARCHAR(8), @os_memory)+' MB';
             IF(@PrintOnly = 0)
                 BEGIN
                     IF @current_value = 1
                         BEGIN
                             IF
(
    SELECT value_in_use
    FROM sys.configurations
    WHERE NAME LIKE 'min server memory (MB)'
) <> @min_memory
                                 BEGIN
                                     EXEC sys.sp_configure
                                          N'min server memory (MB)',
                                          @min_memory;
                                     RECONFIGURE WITH OVERRIDE;
                                 END;
                             IF
(
    SELECT value_in_use
    FROM sys.configurations
    WHERE NAME LIKE 'max server memory (MB)'
) <> @max_memory
                                 BEGIN
                                     EXEC sys.sp_configure
                                          N'max server memory (MB)',
                                          @max_memory;
                                     RECONFIGURE WITH OVERRIDE;
                                 END;
                         END;
                         ELSE
                         BEGIN
                             EXEC sp_configure
                                  'show advanced options',
                                  1
                             RECONFIGURE WITH OVERRIDE
                             IF
(
    SELECT value_in_use
    FROM sys.configurations
    WHERE NAME LIKE 'min server memory (MB)'
) <> @min_memory
                                 BEGIN
                                     EXEC sys.sp_configure
                                          N'min server memory (MB)',
                                          @min_memory;
                                     RECONFIGURE WITH OVERRIDE;
                                 END;
                             IF
(
    SELECT value_in_use
    FROM sys.configurations
    WHERE NAME LIKE 'max server memory (MB)'
) <> @max_memory
                                 BEGIN
                                     EXEC sys.sp_configure
                                          N'max server memory (MB)',
                                          @max_memory;
                                     RECONFIGURE WITH OVERRIDE;
                                 END;
                             EXEC sp_configure
                                  'show advanced options',
                                  0;
                             RECONFIGURE WITH OVERRIDE;
                         END;
                 END;
         END;
GO
