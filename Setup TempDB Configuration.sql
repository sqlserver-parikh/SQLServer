USE tempdb;
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_TempDBConfiguration]') AND type IN (N'P', N'PC'))
    DROP PROCEDURE [dbo].[usp_TempDBConfiguration];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [dbo].[usp_TempDBConfiguration]
(
    -- File Sizing & Growth Configuration
    @DataFileCount          TINYINT      = NULL,
    @InitialDataFileSizeMB  INT          = 2048,
    @DataFileGrowthMB       INT          = 512,
    @InitialLogFileSizeMB   INT          = 1024,
    @LogFileGrowthMB        INT          = 512,

    -- Safety and Execution Control
    @PercentFreeOnDrive     TINYINT      = 20,
    @Print                  BIT          = 1,
    @Execute                BIT          = 0
)
AS
BEGIN
    SET NOCOUNT ON;

    -- =================================================================
    -- VALIDATION
    -- =================================================================
    IF @Print = 0 AND @Execute = 0
    BEGIN
        PRINT 'Warning: Both @Print and @Execute are set to 0. No action will be taken.';
        RETURN;
    END

    -- =================================================================
    -- VARIABLE DECLARATION (and other declarations from previous script)
    -- =================================================================
    DECLARE @cpu_count INT, @current_data_file_count INT, @target_data_file_count TINYINT, @alter_command NVARCHAR(MAX),
            @logical_name SYSNAME, @file_type_desc NVARCHAR(60), @file_path NVARCHAR(520), @current_physical_name NVARCHAR(520),
            @drive_total_mb BIGINT, @drive_free_mb BIGINT, @existing_tempdb_size_mb BIGINT, @required_tempdb_size_mb BIGINT,
            @additional_space_needed_mb BIGINT, @projected_free_space_mb BIGINT, @current_size_mb INT;

    -- =================================================================
    -- GATHER SYSTEM INFORMATION (same as before)
    -- =================================================================
    SELECT @cpu_count = cpu_count FROM sys.dm_os_sys_info;
    SELECT @current_data_file_count = COUNT(*) FROM tempdb.sys.database_files WHERE type_desc = 'ROWS';
    SELECT @existing_tempdb_size_mb = SUM(size) * 8 / 1024 FROM tempdb.sys.database_files;
    SELECT TOP 1 @file_path = physical_name FROM tempdb.sys.database_files WHERE name = 'tempdev';
    SET @file_path = LEFT(@file_path, LEN(@file_path) - CHARINDEX('\', REVERSE(@file_path))) + '\';
    SELECT @drive_total_mb = total_bytes / 1048576, @drive_free_mb = available_bytes / 1048576
    FROM sys.dm_os_volume_stats(DB_ID('tempdb'), 1);

    -- =================================================================
    -- DETERMINE TARGET FILE COUNT & REQUIRED SPACE (same as before)
    -- =================================================================
    SET @target_data_file_count = ISNULL(@DataFileCount, CASE WHEN @cpu_count <= 8 THEN @cpu_count ELSE 8 END);
    SET @required_tempdb_size_mb = (@target_data_file_count * @InitialDataFileSizeMB) + @InitialLogFileSizeMB;

    -- =================================================================
    -- SAFETY CHECK: VALIDATE DISK SPACE (same as before, with corrected PRINT syntax)
    -- =================================================================
    SET @additional_space_needed_mb = @required_tempdb_size_mb - @existing_tempdb_size_mb;
    IF @additional_space_needed_mb < 0 SET @additional_space_needed_mb = 0;
    SET @projected_free_space_mb = @drive_free_mb - @additional_space_needed_mb;

    IF (@projected_free_space_mb * 100.0 / @drive_total_mb) < @PercentFreeOnDrive
    BEGIN
        RAISERROR('SAFETY CHECK FAILED: The proposed configuration would leave less than the specified @PercentFreeOnDrive.', 16, 1);
        PRINT '---------------------------------------------------------------------------------------------------';
        PRINT 'Drive Total Size:                     %s MB'+ CONVERT(VARCHAR(20), @drive_total_mb);
        PRINT 'Drive Free Space (Current):           %s MB'+ CONVERT(VARCHAR(20), @drive_free_mb);
        PRINT 'Required TempDB Size:                 %s MB'+ CONVERT(VARCHAR(20), @required_tempdb_size_mb);
        PRINT 'Projected Drive Free Space (After):   %s MB'+ CONVERT(VARCHAR(20), @projected_free_space_mb);
        PRINT 'Projected Free Percentage:            %.2f%%'+ (@projected_free_space_mb * 100.0 / @drive_total_mb);
        PRINT 'Required Minimum Free Percentage:     %d%%'+ @PercentFreeOnDrive;
        PRINT '---------------------------------------------------------------------------------------------------';
        PRINT 'ACTION HALTED. To proceed, reduce file sizes or lower the @PercentFreeOnDrive parameter.';
        RETURN;
    END;

    -- ... (Information printing and ADD FILES logic remains the same) ...
	-- The following sections are updated.

    -- =================================================================
    -- MODIFY EXISTING FILES (DATA and LOG) TO ENSURE UNIFORMITY
    -- =================================================================
    PRINT CHAR(10) + '--- Verifying/Modifying All Existing TempDB Files ---';

    DECLARE file_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT name, type_desc, (size * 8 / 1024) FROM tempdb.sys.database_files;

    OPEN file_cursor;
    FETCH NEXT FROM file_cursor INTO @logical_name, @file_type_desc, @current_size_mb;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @target_size_mb INT, @target_growth_mb INT;

        IF @file_type_desc = 'ROWS' -- It's a data file
        BEGIN
            SET @target_size_mb = @InitialDataFileSizeMB;
            SET @target_growth_mb = @DataFileGrowthMB;
        END
        ELSE -- It's a log file
        BEGIN
            SET @target_size_mb = @InitialLogFileSizeMB;
            SET @target_growth_mb = @LogFileGrowthMB;
        END

        -- *** NEW LOGIC TO DETECT SHRINK SCENARIO ***
        IF @current_size_mb > @target_size_mb
        BEGIN
            PRINT '---> WARNING: File ''' + @logical_name + ''' needs to be shrunk from ' + CAST(@current_size_mb AS VARCHAR(10)) + 'MB to ' + CAST(@target_size_mb AS VARCHAR(10)) + 'MB.';
            PRINT '     This script will NOT perform the shrink. It will only set the target size in metadata.';
            PRINT '     To complete the shrink, you MUST RESTART the SQL Server service during a maintenance window.';
        END

        SET @alter_command = N'ALTER DATABASE [tempdb] MODIFY FILE (NAME = N''' + @logical_name + ''', SIZE = ' + CAST(@target_size_mb AS NVARCHAR(10)) + N'MB, FILEGROWTH = ' + CAST(@target_growth_mb AS NVARCHAR(10)) + N'MB);';

        IF @Print = 1 PRINT @alter_command;
        IF @Execute = 1 EXEC sp_executesql @statement = @alter_command;

        FETCH NEXT FROM file_cursor INTO @logical_name, @file_type_desc, @current_size_mb;
    END

    CLOSE file_cursor;
    DEALLOCATE file_cursor;

    PRINT '---------------------------------------------------------';
    PRINT 'Configuration script generation complete.';
    IF @Execute = 1 PRINT 'Commands have been executed.';

END;
GO
