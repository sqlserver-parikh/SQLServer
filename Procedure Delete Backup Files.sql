USE tempdb; -- Usually kept in master or a dedicated Admin DB
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_DeleteBackupFiles]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[usp_DeleteBackupFiles] AS RETURN 0;';
END
GO

ALTER PROCEDURE [dbo].[usp_DeleteBackupFiles]
    @DatabaseName          NVARCHAR(128) = NULL,
    @RetainDays            INT           = 2,
    @MaxBackupFilesToKeep  INT           = NULL,
    @BackupTypeToDelete    CHAR(1)       = NULL, -- D (Full), I (Diff), L (Log)
    @LookBackDays          INT           = 60,
    @LocalDrive            BIT           = 1,    -- 1 = Exclude UNC paths (\\Server\Share)
    @Print                 BIT           = 1,    -- Display commands/info
    @Execute               BIT           = 0     -- Physically delete files
AS
/*******************************************************************************
Description:
    Deletes physical backup files based on msdb history. 
    Can filter by local drives only and provides post-deletion verification.

Parameters:
    @DatabaseName:         Name of the database to process. NULL = All.
    @RetainDays:           Delete files older than this many days.
    @MaxBackupFilesToKeep: Keep the most recent N files per DB/Type.
    @BackupTypeToDelete:   D = Full, I = Differential, L = Log. NULL = All.
    @LookBackDays:         How far back in msdb history to search (Default 30).
    @LocalDrive:           If 1, only processes files NOT starting with '\\'.
    @Print:                1 = Select results and print commands.
    @Execute:              1 = Execute the xp_delete_file command.

Usage:
    EXEC dbo.usp_DeleteBackupFiles @RetainDays = 7, @LocalDrive = 1, @Print = 1, @Execute = 0;

Author: Modified for Production Standard
Version: 1.1
*******************************************************************************/
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ErrorMsg NVARCHAR(2048);
    DECLARE @CurrentFile NVARCHAR(512);
    DECLARE @DeleteCmd NVARCHAR(1000);
    DECLARE @FileExists INT;

    ----------------------------------------------------------------------------
    -- 1. Parameter Validation
    ----------------------------------------------------------------------------
    IF (@RetainDays IS NULL AND @MaxBackupFilesToKeep IS NULL)
    BEGIN
        SET @ErrorMsg = 'Validation Failed: You must provide either @RetainDays or @MaxBackupFilesToKeep.';
        RAISERROR(@ErrorMsg, 16, 1);
        RETURN;
    END

    IF (@RetainDays IS NOT NULL AND @MaxBackupFilesToKeep IS NOT NULL)
    BEGIN
        SET @ErrorMsg = 'Validation Failed: You cannot provide both @RetainDays and @MaxBackupFilesToKeep.';
        RAISERROR(@ErrorMsg, 16, 1);
        RETURN;
    END

    IF @BackupTypeToDelete NOT IN ('D', 'I', 'L', NULL)
    BEGIN
        SET @ErrorMsg = 'Validation Failed: @BackupTypeToDelete must be ''D'', ''I'', ''L'', or NULL.';
        RAISERROR(@ErrorMsg, 16, 1);
        RETURN;
    END

    ----------------------------------------------------------------------------
    -- 2. Setup Temp Table
    ----------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#BackupFiles') IS NOT NULL DROP TABLE #BackupFiles;
    
    CREATE TABLE #BackupFiles (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        PhysicalPath NVARCHAR(512),
        DBName NVARCHAR(128),
        BackupDate DATETIME,
        BkpType CHAR(1),
        VerifiedExists BIT DEFAULT 0,
        DeleteAttempted BIT DEFAULT 0,
        DeleteSuccessful BIT DEFAULT 0,
        ErrorMessage NVARCHAR(MAX)
    );

    ----------------------------------------------------------------------------
    -- 3. Gather Initial Data from MSDB
    ----------------------------------------------------------------------------
    INSERT INTO #BackupFiles (PhysicalPath, DBName, BackupDate, BkpType)
    SELECT DISTINCT 
           f.physical_device_name, 
           s.database_name, 
           s.backup_start_date, 
           s.type
    FROM msdb.dbo.backupset s
    JOIN msdb.dbo.backupmediafamily f ON s.media_set_id = f.media_set_id
    WHERE s.backup_start_date >= DATEADD(DAY, -@LookBackDays, GETDATE())
      AND (@DatabaseName IS NULL OR s.database_name = @DatabaseName)
      AND (@BackupTypeToDelete IS NULL OR s.type = @BackupTypeToDelete)
      AND f.device_type IN (2, 7) -- Disk devices
      AND f.physical_device_name NOT LIKE '{%' -- Exclude internal GUIDs
      AND (@LocalDrive = 0 OR (@LocalDrive = 1 AND f.physical_device_name NOT LIKE '\\%'));

    ----------------------------------------------------------------------------
    -- 4. Apply Retention Logic
    ----------------------------------------------------------------------------
    IF @RetainDays IS NOT NULL
    BEGIN
        -- Remove files that are still within the "Keep" window
        DELETE FROM #BackupFiles WHERE BackupDate >= DATEADD(DAY, -@RetainDays, GETDATE());
    END
    ELSE IF @MaxBackupFilesToKeep IS NOT NULL
    BEGIN
        -- Remove the N newest files from the "To Delete" list
        ;WITH RankedFiles AS (
            SELECT ID, ROW_NUMBER() OVER (PARTITION BY DBName, BkpType ORDER BY BackupDate DESC) AS RowNum
            FROM #BackupFiles
        )
        DELETE FROM #BackupFiles WHERE ID IN (SELECT ID FROM RankedFiles WHERE RowNum <= @MaxBackupFilesToKeep);
    END

    ----------------------------------------------------------------------------
    -- 5. Verify File Existence (Pre-check)
    ----------------------------------------------------------------------------
    DECLARE verify_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT PhysicalPath FROM #BackupFiles;
    OPEN verify_cursor;
    FETCH NEXT FROM verify_cursor INTO @CurrentFile;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC master.dbo.xp_fileexist @CurrentFile, @FileExists OUTPUT;
        IF @FileExists = 1 UPDATE #BackupFiles SET VerifiedExists = 1 WHERE PhysicalPath = @CurrentFile;
        FETCH NEXT FROM verify_cursor INTO @CurrentFile;
    END
    CLOSE verify_cursor;
    DEALLOCATE verify_cursor;

    -- Clean list to only files that actually exist on disk
    DELETE FROM #BackupFiles WHERE VerifiedExists = 0;

    ----------------------------------------------------------------------------
    -- 6. Execution and Error Handling
    ----------------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM #BackupFiles)
    BEGIN
        IF @Execute = 1
        BEGIN
            DECLARE delete_cursor CURSOR LOCAL FAST_FORWARD FOR 
                SELECT PhysicalPath FROM #BackupFiles;

            OPEN delete_cursor;
            FETCH NEXT FROM delete_cursor INTO @CurrentFile;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @DeleteCmd = 'EXEC master.dbo.xp_delete_file 0, ''' + @CurrentFile + '''';
                
                BEGIN TRY
                    UPDATE #BackupFiles SET DeleteAttempted = 1 WHERE PhysicalPath = @CurrentFile;
                    
                    EXEC sp_executesql @DeleteCmd;

                    -- Post-Deletion Verification
                    EXEC master.dbo.xp_fileexist @CurrentFile, @FileExists OUTPUT;
                    IF @FileExists = 0
                        UPDATE #BackupFiles SET DeleteSuccessful = 1 WHERE PhysicalPath = @CurrentFile;
                    ELSE
                        UPDATE #BackupFiles SET ErrorMessage = 'File still exists after xp_delete_file (Permission or Lock issue)' WHERE PhysicalPath = @CurrentFile;

                END TRY
                BEGIN CATCH
                    UPDATE #BackupFiles 
                    SET ErrorMessage = ERROR_MESSAGE(), DeleteSuccessful = 0 
                    WHERE PhysicalPath = @CurrentFile;
                END CATCH

                FETCH NEXT FROM delete_cursor INTO @CurrentFile;
            END

            CLOSE delete_cursor;
            DEALLOCATE delete_cursor;
        END

        ----------------------------------------------------------------------------
        -- 7. Reporting (@Print)
        ----------------------------------------------------------------------------
        IF @Print = 1
        BEGIN
            SELECT 
                DBName,
                BkpType AS [Type],
                BackupDate,
                PhysicalPath,
                CASE WHEN @Execute = 1 THEN DeleteSuccessful ELSE 0 END AS [IsDeleted],
                ISNULL(ErrorMessage, CASE WHEN @Execute = 0 THEN 'Ready to Delete' ELSE 'Success' END) AS [Status],
                'EXEC master.dbo.xp_delete_file 0, ''' + PhysicalPath + '''' AS [GeneratedCommand]
            FROM #BackupFiles
            ORDER BY DBName, BackupDate;
        END
    END
    ELSE
    BEGIN
        PRINT 'No files found matching the criteria.';
    END
END
GO
exec [usp_DeleteBackupFiles]
