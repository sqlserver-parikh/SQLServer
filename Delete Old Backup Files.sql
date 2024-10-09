USE tempdb;
GO

CREATE OR ALTER PROCEDURE usp_DeleteBackupHistory
(
    @RetainDays INT = 2,
    @BackupTypeToDelete CHAR(1) = NULL, -- 'D' for full, 'I' for incremental, 'L' for log
    @BackupLocation VARCHAR(MAX) = NULL,
    @PrintOnly BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @BackupTypeToDelete IS NULL OR @BackupTypeToDelete = ''
        SET @BackupTypeToDelete = '%';

    DECLARE @DefaultBackupDirectory VARCHAR(200);
    EXECUTE master..xp_instance_regread
        N'HKEY_LOCAL_MACHINE',
        N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer',
        N'BackupDirectory',
        @DefaultBackupDirectory OUTPUT;

    IF @BackupLocation IS NULL OR @BackupLocation = ''
        SET @DefaultBackupDirectory = SUBSTRING(@DefaultBackupDirectory, 1, 3);
    ELSE 
        SET @DefaultBackupDirectory = @BackupLocation;

    IF OBJECT_ID('tempdb..#DirectoryTree') IS NOT NULL
        DROP TABLE #DirectoryTree;

    CREATE TABLE #DirectoryTree
    (
        id INT IDENTITY(1, 1),
        subdirectory NVARCHAR(512),
        depth INT,
        isfile BIT
    );

    INSERT INTO #DirectoryTree (subdirectory, depth, isfile)
    EXEC master.sys.xp_dirtree @DefaultBackupDirectory, 0, 1;

    SELECT 
        CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server,
        'exec xp_delete_file 0,''' + a.physical_device_name + '''' AS DeleteFileCommand,
        b.database_name,
        b.backup_start_date,
        b.backup_finish_date,
        b.expiration_date,
        CASE b.type
            WHEN 'D' THEN 'Database'
            WHEN 'L' THEN 'Log'
            WHEN 'I' THEN 'Incremental'
        END AS backup_type,
        b.backup_size,
        a.logical_device_name,
        b.name AS backupset_name,
        b.description
    INTO #deletetable
    FROM msdb..backupmediafamily a
    INNER JOIN msdb..backupset b ON a.media_set_id = b.media_set_id
    INNER JOIN #DirectoryTree c ON c.subdirectory = REVERSE(LEFT(REVERSE(a.physical_device_name), CHARINDEX('\', REVERSE(a.physical_device_name))-1))
    WHERE CONVERT(DATETIME, b.backup_start_date, 102) < GETDATE() - @RetainDays
    AND b.type LIKE '%' + @BackupTypeToDelete + '%'
    ORDER BY b.backup_finish_date;

    IF @PrintOnly = 1
    BEGIN
        SELECT * FROM #deletetable;
    END
    ELSE
    BEGIN
        DECLARE @DeleteCmd NVARCHAR(MAX);
        DECLARE delete_cursor CURSOR FOR 
        SELECT DeleteFileCommand FROM #deletetable;

        OPEN delete_cursor;
        FETCH NEXT FROM delete_cursor INTO @DeleteCmd;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC(@DeleteCmd);
            FETCH NEXT FROM delete_cursor INTO @DeleteCmd;
        END;

        CLOSE delete_cursor;
        DEALLOCATE delete_cursor;

        -- Display results after deletion
        SELECT * FROM #deletetable;
    END
END
GO

-- Execute the stored procedure
EXEC usp_DeleteBackupHistory;
