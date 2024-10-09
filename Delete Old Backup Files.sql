USE tempdb;
GO

CREATE OR ALTER PROCEDURE usp_DeleteBackupFiles
(
    @DatabaseName NVARCHAR(128) = null, -- Specific database name
    @RetainDays INT = null,
    @BackupTypeToDelete CHAR(1) = NULL, -- 'D' for full, 'I' for incremental, 'L' for log
	@LookBackDays INT = 90 ,
	@MaxBackupFilesToKeep INT = 0,
    @PrintOnly BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
 -- Ensure that either @RetainDays or @MaxBackupFilesToKeep is provided but not both
IF ( (@RetainDays IS NULL AND @MaxBackupFilesToKeep IS NULL) OR 
     (@RetainDays IS NOT NULL AND @MaxBackupFilesToKeep IS NOT NULL))
BEGIN
    RAISERROR('Either @RetainDays or @MaxBackupFilesToKeep must be provided, but not both.', 16, 1);
    RETURN;
END

   IF @LookBackDays < 0
    BEGIN
        RAISERROR('LookBackDays must be positive number.', 16, 1);
        RETURN;
    END
    IF @BackupTypeToDelete IS NULL OR @BackupTypeToDelete = ''
        SET @BackupTypeToDelete = '%';

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
    WHERE b.type LIKE '%' + @BackupTypeToDelete + '%'
	AND b.backup_start_date >= DATEADD(DAY, -@LookBackDays, GETDATE())
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

	IF (@MaxBackupFilesToKeep IS NOT NULL) OR (@MaxBackupFilesToKeep <> '')
	BEGIN
	  ;WITH RankedFiles AS
    (
        SELECT 
            physical_device_name,
            database_name,
            backup_type,
            ROW_NUMBER() OVER (PARTITION BY database_name, backup_type ORDER BY backup_start_date DESC) AS RowNum
        FROM #BackupFiles
    )
    DELETE FROM #BackupFiles
    WHERE physical_device_name IN 
    (
        SELECT physical_device_name 
        FROM RankedFiles 
        WHERE RowNum <= @MaxBackupFilesToKeep
    );

	END

	IF (@RetainDays IS NOT NULL ) OR (@RetainDays <> '')
	BEGIN 
		DELETE FROM #BackupFiles
		WHERE backup_start_date > DATEADD(DD,-@RetainDays,GETDATE())
	END

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
            description, file_exists 
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
