USE tempdb;
GO

CREATE OR ALTER PROCEDURE dbo.usp_ManageTDE
    @DatabaseName NVARCHAR(128) = 'TDETest',
    @TDEAction NVARCHAR(10) = 'enable', -- Valid Actions: 'Enable', 'Disable', 'Status', 'DropDEK'
    @CertificateName NVARCHAR(128) = 'TDE Certificate',
    
    -- TDE supports: AES_128, AES_192, AES_256, TRIPLE_DES_3KEY. AES_256 is recommended.
    @EncryptionAlgorithm NVARCHAR(32) = 'AES_256',
    
    -- Passwords for creating or backing up crypto objects.
    @MasterKeyPassword NVARCHAR(128) = 'M0$tS3curedP@s5w0rD',
    @CertificateBackupPassword NVARCHAR(128) = 'M0$tS3curedP@s5w0rD', -- Password to protect the private key file on backup.

    -- Actions for creating/managing crypto objects
    @CreateMasterKey BIT = 1,
    @CreateCertificate BIT = 1,
    @BackupCertificate BIT = 1,
    @RestoreCertificate BIT = 0,
    @DropCertificate BIT = 0,

    -- File paths for backup/restore operations
    @BackupDirectory NVARCHAR(256) = 'F:\data', -- If NULL, will use a valid default location.
    @RestoreCertFilePath NVARCHAR(256) = '',
    @RestorePrivateKeyFilePath NVARCHAR(256) = '',
    @RestoreDecryptionPrivateKeyPassword NVARCHAR(128) = '',

    -- Control flags
    @Print BIT = 1,
    @Execute BIT = 1,
    @WaitForCompletion BIT = 0,
    @MaxWaitMinutes INT = 60
AS
BEGIN
    SET NOCOUNT ON;

    -- ====================================================================================
    -- Initial Validation and Variable Declaration
    -- ====================================================================================
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ErrorMessage NVARCHAR(500);
    DECLARE @MajorVersion INT = CAST(PARSENAME(CONVERT(VARCHAR(32), SERVERPROPERTY('ProductVersion')), 4) AS INT);
    DECLARE @Edition VARCHAR(128) = CAST(SERVERPROPERTY('Edition') AS VARCHAR(128));

    IF @TDEAction <> 'Status' OR @DropCertificate = 1 OR @CreateCertificate = 1 OR @CreateMasterKey = 1 OR @BackupCertificate = 1 OR @RestoreCertificate = 1
    BEGIN
        IF @MajorVersion < 10 BEGIN RAISERROR('TDE is not supported on SQL Server versions prior to 2008.', 16, 1); RETURN; END
        IF @MajorVersion < 15 AND (@Edition NOT LIKE '%Enterprise%' AND @Edition NOT LIKE '%Developer%') BEGIN RAISERROR('On this version of SQL Server, TDE is only supported on Enterprise or Developer editions.', 16, 1); RETURN; END
        IF @Edition LIKE '%Express%' OR @Edition LIKE '%Web%' BEGIN RAISERROR('TDE is not supported on the Express or Web editions of SQL Server.', 16, 1); RETURN; END
    END

    DECLARE @WaitStart DATETIME, @CurrentPercent FLOAT;
    DECLARE @ServerName NVARCHAR(128), @Timestamp NVARCHAR(20);
    DECLARE @BackupFileCert NVARCHAR(256), @BackupFileKey NVARCHAR(256);

    SET @DatabaseName = LTRIM(RTRIM(ISNULL(@DatabaseName, '')));
    SET @TDEAction = UPPER(LTRIM(RTRIM(ISNULL(@TDEAction, 'STATUS'))));
    SET @CertificateName = LTRIM(RTRIM(ISNULL(@CertificateName, 'TDE Certificate')));

    BEGIN TRY
        -- ====================================================================================
        -- STEP 1: Process Crypto Management Actions FIRST
        -- ====================================================================================
        
        IF @CreateMasterKey = 1
        BEGIN
            IF (SELECT COUNT(*) FROM master.sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##') > 0 BEGIN PRINT '--Database Master Key already exists.'; END
            ELSE
            BEGIN
                IF @MasterKeyPassword IS NULL BEGIN RAISERROR('A password is required to create a master key (@MasterKeyPassword).', 16, 1); RETURN; END
                SET @SQL = 'USE master; CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + REPLACE(@MasterKeyPassword, '''', '''''') + ''';';
                IF @Print = 1 PRINT 'Creating Database Master Key:' + CHAR(10) + @SQL + CHAR(10);
                IF @Execute = 1 BEGIN EXEC sp_executesql @SQL; PRINT 'Database Master Key created successfully.'; END
            END
        END

        IF @RestoreCertificate = 1
        BEGIN
            IF (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName) > 0 BEGIN PRINT '--Certificate ''' + @CertificateName + ''' already exists.'; END
            ELSE
            BEGIN
                IF @RestoreCertFilePath = '' OR @RestorePrivateKeyFilePath = '' OR @RestoreDecryptionPrivateKeyPassword = '' BEGIN RAISERROR('CertFilePath, PrivateKeyFilePath, and PrivateKeyPassword are required for restore.', 16, 1); RETURN; END
                SET @SQL = 'USE master; CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) + ' FROM FILE = ''' + REPLACE(@RestoreCertFilePath, '''', '''''') + ''' WITH PRIVATE KEY (FILE = ''' + REPLACE(@RestorePrivateKeyFilePath, '''', '''''') + ''', DECRYPTION BY PASSWORD = ''' + REPLACE(@RestoreDecryptionPrivateKeyPassword, '''', '''''') + ''');';
                IF @Print = 1 PRINT 'Restoring Certificate:' + CHAR(10) + @SQL + CHAR(10);
                IF @Execute = 1 BEGIN EXEC sp_executesql @SQL; PRINT 'Certificate ''' + @CertificateName + ''' restored successfully.'; END
            END
        END

        IF @CreateCertificate = 1
        BEGIN
            IF (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName) > 0 BEGIN PRINT '--Certificate ''' + @CertificateName + ''' already exists.'; END
            ELSE
            BEGIN
                IF (SELECT COUNT(*) FROM master.sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##') = 0 BEGIN RAISERROR('Master Key does not exist. Use @CreateMasterKey=1 to create it first.', 16, 1); RETURN; END
                SET @SQL = 'USE master; CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) + ' WITH SUBJECT = ''TDE Certificate'';';
                IF @Print = 1 PRINT 'Creating Certificate:' + CHAR(10) + @SQL + CHAR(10);
                IF @Execute = 1 BEGIN EXEC sp_executesql @SQL; PRINT 'Certificate ''' + @CertificateName + ''' created successfully.'; END
            END
        END
        
        IF @BackupCertificate = 1
        BEGIN
            IF (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName) = 0 BEGIN RAISERROR('Certificate ''%s'' does not exist to be backed up.', 16, 1, @CertificateName); RETURN; END
            IF @CertificateBackupPassword IS NULL BEGIN RAISERROR('@CertificateBackupPassword is required for backup.', 16, 1); RETURN; END
            
            DECLARE @FinalBackupDirectory NVARCHAR(256) = @BackupDirectory;
            DECLARE @DirExists INT = 0;

            IF @FinalBackupDirectory IS NOT NULL AND @FinalBackupDirectory <> ''
            BEGIN
                PRINT '--' + @FinalBackupDirectory
            END
            ELSE
            BEGIN
                PRINT '--@BackupDirectory not specified, attempting to find a valid location.';
                EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'BackupDirectory', @FinalBackupDirectory OUTPUT;
                
                IF @FinalBackupDirectory IS NOT NULL AND @FinalBackupDirectory <> ''
                BEGIN
                    EXEC master.dbo.xp_fileexist @FinalBackupDirectory, @DirExists OUTPUT;
                END

                IF @DirExists = 0
                BEGIN
                    PRINT '--Default backup directory is invalid or not found. Falling back to the MASTER database data directory.';
                    SELECT @FinalBackupDirectory = SUBSTRING(physical_name, 1, LEN(physical_name) - CHARINDEX('\', REVERSE(physical_name))) 
                    FROM master.sys.master_files WHERE database_id = 1 AND file_id = 1;
                END
            END

            PRINT '--Using backup directory: ' + @FinalBackupDirectory;

            IF RIGHT(@FinalBackupDirectory, 1) != '\' SET @FinalBackupDirectory = @FinalBackupDirectory + '\';
            SET @ServerName = REPLACE(REPLACE(@@SERVERNAME, '\', '_'), ':', '_');
            SET @Timestamp = REPLACE(REPLACE(REPLACE(CONVERT(NVARCHAR(19), GETDATE(), 120), '-', ''), ':', ''), ' ', '_');
            SET @BackupFileCert = @FinalBackupDirectory + @CertificateName + '_' + @ServerName + '_' + @Timestamp + '.cer';
            SET @BackupFileKey = @FinalBackupDirectory + @CertificateName + '_' + @ServerName + '_' + @Timestamp + '.pvk';
            SET @SQL = 'USE master; BACKUP CERTIFICATE ' + QUOTENAME(@CertificateName) + ' TO FILE = ''' + @BackupFileCert + ''' WITH PRIVATE KEY (FILE = ''' + @BackupFileKey + ''', ENCRYPTION BY PASSWORD = ''' + REPLACE(@CertificateBackupPassword, '''', '''''') + ''');';
            
            IF @Print = 1 PRINT '--Backing up Certificate:' + CHAR(10) + @SQL + CHAR(10);
            IF @Execute = 1 
            BEGIN
                EXEC sp_executesql @SQL;
                PRINT '--Certificate backed up successfully:' + CHAR(10) + '  --Certificate: ' + @BackupFileCert + CHAR(10) + '  --Private Key: ' + @BackupFileKey;
                PRINT '--*** CRITICAL: Store these files and the password in a secure, off-server location! ***';
            END
        END
        
        IF @DropCertificate = 1
        BEGIN
            IF (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName) = 0 BEGIN PRINT 'Certificate ''' + @CertificateName + ''' does not exist.'; END
            ELSE
            BEGIN
                DECLARE @UsedByDBs NVARCHAR(MAX);
                SELECT @UsedByDBs = STUFF((
                    SELECT ', ' + d.name
                    FROM sys.dm_database_encryption_keys dek 
                    JOIN master.sys.certificates c ON dek.encryptor_thumbprint = c.thumbprint 
                    JOIN sys.databases d ON dek.database_id = d.database_id
                    WHERE c.name = @CertificateName AND dek.encryption_state NOT IN (0, 1)
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '');

                IF @UsedByDBs IS NOT NULL BEGIN RAISERROR('Cannot drop certificate ''%s'' because it is in use by: %s.', 16, 1, @CertificateName, @UsedByDBs); RETURN; END

                SET @SQL = 'USE master; DROP CERTIFICATE ' + QUOTENAME(@CertificateName) + ';';
                IF @Print = 1 PRINT '-- Dropping Certificate (Safety Check Passed):' + CHAR(10) + @SQL + CHAR(10);
                IF @Execute = 1 BEGIN EXEC sp_executesql @SQL; PRINT 'Certificate ''' + @CertificateName + ''' dropped successfully.'; END
            END
        END

        -- ====================================================================================
        -- STEP 2: Process the Main TDE Action
        -- ====================================================================================
        IF @TDEAction = 'STATUS'
        BEGIN
            SET @SQL = '
                SELECT
                    d.name AS [DatabaseName],
                    ISNULL(dek.encryption_state_desc, ''NOT ENCRYPTED'') AS [TDE_Status],
                    ISNULL(dek.percent_complete, 0) AS [Progress_%],
                    dek.key_algorithm AS [Algorithm],
                    c.name AS [Certificate_Name]
                FROM sys.databases d
                LEFT JOIN sys.dm_database_encryption_keys dek ON d.database_id = dek.database_id
                LEFT JOIN master.sys.certificates c ON dek.encryptor_thumbprint = c.thumbprint
                WHERE d.database_id > 4
                ORDER BY d.name;';
            IF @Print = 1 PRINT '-- TDE Status Report Query:' + CHAR(10) + @SQL + CHAR(10);
            IF @Execute = 1 EXEC sp_executesql @SQL;
        END
        ELSE IF @TDEAction = 'ENABLE'
        BEGIN
            IF @DatabaseName = '' BEGIN RAISERROR('DatabaseName is required for the ENABLE action.', 16, 1); RETURN; END
            IF (SELECT COUNT(*) FROM master.sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##') = 0 RAISERROR('Database Master Key does not exist. Use @CreateMasterKey=1 to create it first.', 16, 1);
            IF (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName) = 0 RAISERROR('Certificate ''%s'' does not exist. Use @CreateCertificate=1 or @RestoreCertificate=1 first.', 16, 1, @CertificateName);

            DECLARE @EnableEncryptionState INT;
            SELECT @EnableEncryptionState = encryption_state FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName);
            IF @EnableEncryptionState = 3 BEGIN PRINT '--TDE is already enabled on database ''' + @DatabaseName + '''.'; RETURN; END
            IF @EnableEncryptionState IN (2, 4, 5, 6) BEGIN PRINT 'TDE operation already in progress on database ''' + @DatabaseName + '''.'; RETURN; END

            SET @SQL = 'USE ' + QUOTENAME(@DatabaseName) + ';' + CHAR(10) +
                       'IF NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(''' + REPLACE(@DatabaseName, '''', '''''') + '''))' + CHAR(10) +
                       'BEGIN' + CHAR(10) +
                       '    PRINT ''--Creating Database Encryption Key for ' + @DatabaseName + '...'';' + CHAR(10) +
                       '    CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = ' + @EncryptionAlgorithm + CHAR(10) +
                       '    ENCRYPTION BY SERVER CERTIFICATE ' + QUOTENAME(@CertificateName) + ';' + CHAR(10) +
                       'END;' + CHAR(10) +
                       'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET ENCRYPTION ON;';

            IF @Print = 1 PRINT '--Enabling TDE on database ''' + @DatabaseName + ''':' + CHAR(10) + @SQL + CHAR(10);
            IF @Execute = 1 
            BEGIN
                EXEC sp_executesql @SQL;
                PRINT '--TDE enablement initiated for database ''' + @DatabaseName + '''.';
                IF @WaitForCompletion = 1
                BEGIN
                    PRINT 'Waiting for TDE encryption to complete...';
                    SET @WaitStart = GETDATE();
                    WHILE DATEDIFF(MINUTE, @WaitStart, GETDATE()) < @MaxWaitMinutes
                    BEGIN
                        SELECT @EnableEncryptionState = encryption_state, @CurrentPercent = percent_complete FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName);
                        IF @EnableEncryptionState = 3 BEGIN PRINT 'TDE encryption completed successfully.'; BREAK; END
                        PRINT 'Encryption progress: ' + CAST(ISNULL(@CurrentPercent, 0) AS VARCHAR(10)) + '%';
                        WAITFOR DELAY '00:00:30';
                    END
                    IF @EnableEncryptionState != 3 PRINT 'TDE encryption is still in progress. Monitor using the Status action.';
                END
            END
        END
        ELSE IF @TDEAction = 'DISABLE'
        BEGIN
            IF @DatabaseName = '' BEGIN RAISERROR('DatabaseName is required for the DISABLE action.', 16, 1); RETURN; END
            DECLARE @DisableEncryptionState INT;
            SELECT @DisableEncryptionState = encryption_state FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName);
            IF @DisableEncryptionState IS NULL OR @DisableEncryptionState = 1 BEGIN PRINT '--TDE is not enabled on database ''' + @DatabaseName + '''.'; RETURN; END
            IF @DisableEncryptionState IN (2, 4, 5, 6) BEGIN PRINT 'TDE operation is in progress on database ''' + @DatabaseName + '''. Please wait for completion before disabling.'; RETURN; END

            SET @SQL = 'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET ENCRYPTION OFF;';
            
            IF @Print = 1 PRINT '--Disabling TDE on database ''' + @DatabaseName + ''':' + CHAR(10) + @SQL + CHAR(10);
            IF @Execute = 1
            BEGIN
                EXEC sp_executesql @SQL;
                PRINT '--TDE decryption initiated for database ''' + @DatabaseName + '''.';
                IF @WaitForCompletion = 1
                BEGIN
                    PRINT 'Waiting for TDE decryption to complete...';
                    SET @WaitStart = GETDATE();
                    WHILE DATEDIFF(MINUTE, @WaitStart, GETDATE()) < @MaxWaitMinutes
                    BEGIN
                        SELECT @DisableEncryptionState = encryption_state, @CurrentPercent = percent_complete FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName);
                        IF @DisableEncryptionState = 1 BEGIN PRINT 'TDE decryption completed. You may now use @TDEAction=''DropDEK''.'; BREAK; END
                        PRINT 'Decryption progress: ' + CAST(ISNULL(@CurrentPercent, 0) AS VARCHAR(10)) + '%';
                        WAITFOR DELAY '00:00:30';
                    END
                    IF @DisableEncryptionState != 1 PRINT '--TDE decryption is still in progress. Monitor using the Status action.';
                END
            END
        END
        ELSE IF @TDEAction = 'DROPDEK'
        BEGIN
            IF @DatabaseName = '' BEGIN RAISERROR('DatabaseName is required for the DROPDEK action.', 16, 1); RETURN; END
            DECLARE @DropDEKEncryptionState INT;
            SELECT @DropDEKEncryptionState = encryption_state FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName);
            IF @DropDEKEncryptionState IS NULL BEGIN PRINT '--Database Encryption Key does not exist for database ''' + @DatabaseName + '''.'; RETURN; END
            IF @DropDEKEncryptionState <> 1 BEGIN RAISERROR('Cannot drop the DEK. The database ''%s'' is not fully decrypted.', 16, 1, @DatabaseName); RETURN; END

            SET @SQL = 'USE ' + QUOTENAME(@DatabaseName) + '; DROP DATABASE ENCRYPTION KEY;';
            IF @Print = 1 PRINT '--Dropping Database Encryption Key for ''' + @DatabaseName + ''':' + CHAR(10) + @SQL + CHAR(10);
            IF @Execute = 1
            BEGIN
                EXEC sp_executesql @SQL;
                PRINT '--Database Encryption Key for ''' + @DatabaseName + ''' dropped successfully.';
            END
        END

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = 'Error: ' + ERROR_MESSAGE() + ' (Line ' + CAST(ERROR_LINE() AS VARCHAR(10)) + ')';
        RAISERROR(@ErrorMessage, 16, 1);
        RETURN -1;
    END CATCH
END;
GO
