USE tempdb;
GO

CREATE OR ALTER PROCEDURE usp_DeleteBackupFiles
(
    @RetainDays INT = 2,
    @BackupTypeToDelete CHAR(1) = NULL, -- 'D' for full, 'I' for incremental, 'L' for log
    @BackupLocation VARCHAR(MAX) = NULL,
    @DatabaseName NVARCHAR(128) = NULL, -- Specific database name
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

    IF OBJECT_ID('tempdb..#BackupFiles') IS NOT NULL
        DROP TABLE #BackupFiles;

    CREATE TABLE #BackupFiles
    (
        id INT IDENTITY(1, 1),
        physical_device_name NVARCHAR(512),
        database_name NVARCHAR(128),
        backup_start_date DATETIME,
        backup_finish_date DATETIME,
        expiration_date DATETIME,
        backup_type NVARCHAR(20),
        backup_size BIGINT,
        logical_device_name NVARCHAR(128),
        backupset_name NVARCHAR(128),
        description NVARCHAR(512),
        file_exists BIT
    );

    INSERT INTO #BackupFiles (physical_device_name, database_name, backup_start_date, backup_finish_date, expiration_date, backup_type, backup_size, logical_device_name, backupset_name, description)
    SELECT a.physical_device_name, 
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
    FROM msdb..backupmediafamily a
    INNER JOIN msdb..backupset b ON a.media_set_id = b.media_set_id
    WHERE CONVERT(DATETIME, b.backup_start_date, 102) < GETDATE() - @RetainDays
    AND b.type LIKE '%' + @BackupTypeToDelete + '%'
    AND (@DatabaseName IS NULL OR b.database_name = @DatabaseName);

    DECLARE @physical_device_name NVARCHAR(512);
    DECLARE @file_exists INT;
    DECLARE file_cursor CURSOR FOR 
    SELECT physical_device_name FROM #BackupFiles;

    OPEN file_cursor;
    FETCH NEXT FROM file_cursor INTO @physical_device_name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC master.dbo.xp_fileexist @physical_device_name, @file_exists OUTPUT;
        UPDATE #BackupFiles SET file_exists = @file_exists WHERE physical_device_name = @physical_device_name;
        FETCH NEXT FROM file_cursor INTO @physical_device_name;
    END;

    CLOSE file_cursor;
    DEALLOCATE file_cursor;

    IF @PrintOnly = 1
    BEGIN
        SELECT 
            CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server,
            'exec xp_delete_file 0,''' + physical_device_name + '''' AS DeleteFileCommand,
            database_name,
            backup_start_date,
            backup_finish_date,
            expiration_date,
            backup_type,
            backup_size,
            logical_device_name,
            backupset_name,
            description
        FROM #BackupFiles
        WHERE file_exists = 1;
    END
    ELSE
    BEGIN
        DECLARE @DeleteCmd NVARCHAR(MAX);
        DECLARE delete_cursor CURSOR FOR 
        SELECT 'exec xp_delete_file 0,''' + physical_device_name + ''''
        FROM #BackupFiles
        WHERE file_exists = 1;

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
        SELECT 
            CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server,
            'exec xp_delete_file 0,''' + physical_device_name + '''' AS DeleteFileCommand,
            database_name,
            backup_start_date,
            backup_finish_date,
            expiration_date,
            backup_type,
            backup_size,
            logical_device_name,
            backupset_name,
            description
        FROM #BackupFiles
        WHERE file_exists = 1;
    END
END
GO

-- Execute the stored procedure
EXEC usp_DeleteBackupFiles;
