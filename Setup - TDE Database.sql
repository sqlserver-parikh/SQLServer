USE tempdb;
GO

CREATE OR ALTER PROCEDURE dbo.usp_EnableTDE
    @DatabaseName NVARCHAR(128) = NULL,                -- Target database for TDE (optional for some actions)
    @CertificateName NVARCHAR(128) = 'TDE Certificate', -- Name of the TDE certificate
    @EncryptionAlgorithm NVARCHAR(32) = 'AES_256',      -- Algorithm for TDE
    @MasterKeyPassword NVARCHAR(128) = 'NotE@syToHack2025!',            -- Password for creating master key
    @CertificatePassword NVARCHAR(128) = NULL,          -- Password for creating or backing up certificate
    @CreateMasterKey BIT = 0,                           -- 1 = Create master key if not exists
    @CreateCertificate BIT = 0,                         -- 1 = Create certificate if not exists
    @BackupCertificate BIT = 0,                         -- 1 = Backup certificate
    @BackupDirectory NVARCHAR(256) = NULL,              -- Directory for backup files
    @RestoreCertificate BIT = 0,                        -- 1 = Restore certificate
    @CertFilePath NVARCHAR(256) = '',                 -- Path to .cer file for restore
    @PrivateKeyFilePath NVARCHAR(256) = '',           -- Path to .pvk file for restore
    @PrivateKeyPassword NVARCHAR(128) =  '',           -- Password for .pvk file (restore)
    @EnableTDE BIT = 0,                                 -- 1 = Enable TDE on database
    @PrintOnly BIT = 1                                  -- 1 = Print SQL, 0 = Execute
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ErrorMessage NVARCHAR(500);
    DECLARE @CertificateExists INT;
    DECLARE @MasterKeyExists INT;
    DECLARE @BackupFileCert NVARCHAR(256);
    DECLARE @BackupFileKey NVARCHAR(256);
    DECLARE @ServerName NVARCHAR(128);
    DECLARE @Timestamp NVARCHAR(20);

    BEGIN TRY
        -- Check for master key existence
        SET @MasterKeyExists = (SELECT COUNT(*) FROM master.sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##');
        -- Check for certificate existence
        SET @CertificateExists = (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName);

        -- 1. Create Master Key (if requested or required for restore/cert creation)
        IF @CreateMasterKey = 1 OR ((@RestoreCertificate = 1 OR @CreateCertificate = 1) AND @MasterKeyExists = 0)
        BEGIN
            IF @MasterKeyExists = 0
            BEGIN
                IF @MasterKeyPassword IS NULL OR @MasterKeyPassword = ''
                BEGIN
                    RAISERROR('MasterKeyPassword is required to create the master key.', 16, 1);
                    RETURN;
                END
                -- Idempotent master key creation
                SET @SQL = 'IF NOT EXISTS (SELECT 1 FROM master.sys.symmetric_keys WHERE name = ''##MS_DatabaseMasterKey##'') ' +
                           'BEGIN ' +
                           'USE master; CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + @MasterKeyPassword + '''; ' +
                           'WAITFOR DELAY ''00:00:05''; END';
                IF @PrintOnly = 1
                    PRINT @SQL + CHAR(10);
                ELSE
                    EXEC sp_executesql @SQL;
                SET @MasterKeyExists = 1;
            END
            ELSE
            BEGIN
                PRINT 'Master key already exists in master.' + CHAR(10);
            END
        END

        -- 2. Restore Certificate (if requested)
        IF @RestoreCertificate = 1
        BEGIN
            IF @CertificateName IS NULL OR @CertificateName = ''
                OR @CertFilePath IS NULL OR @CertFilePath = ''
                OR @PrivateKeyFilePath IS NULL OR @PrivateKeyFilePath = ''
                OR @PrivateKeyPassword IS NULL OR @PrivateKeyPassword = ''
            BEGIN
                RAISERROR('For certificate restore, provide CertificateName, CertFilePath, PrivateKeyFilePath, and PrivateKeyPassword.', 16, 1);
                RETURN;
            END
            -- Ensure master key exists (already handled above)
            IF @CertificateExists = 0
            BEGIN
                -- Idempotent certificate restore
                SET @SQL = 'IF NOT EXISTS (SELECT 1 FROM master.sys.certificates WHERE name = ''' + @CertificateName + ''') ' +
                           'BEGIN ' +
                           'USE master; CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) + CHAR(10) +
                           'FROM FILE = ''' + @CertFilePath + '''' + CHAR(10) +
                           'WITH PRIVATE KEY (FILE = ''' + @PrivateKeyFilePath + ''', ' + CHAR(10) +
                           'DECRYPTION BY PASSWORD = ''' + @PrivateKeyPassword + '''); ' + CHAR(10) +
                           'WAITFOR DELAY ''00:00:05''; END';
                IF @PrintOnly = 1
                    PRINT @SQL + CHAR(10);
                ELSE
                    EXEC sp_executesql @SQL;
                SET @CertificateExists = 1;
            END
            ELSE
            BEGIN
                PRINT 'Certificate ' + @CertificateName + ' already exists in master.' + CHAR(10);
            END
        END

        -- 3. Create Certificate (if requested)
        IF @CreateCertificate = 1
        BEGIN
            IF @CertificateName IS NULL OR @CertificateName = ''
            BEGIN
                RAISERROR('CertificateName is required to create certificate.', 16, 1);
                RETURN;
            END
            -- Ensure master key exists (already handled above)
            IF @CertificateExists = 0
            BEGIN
                -- Idempotent certificate creation
                SET @SQL = 'IF NOT EXISTS (SELECT 1 FROM master.sys.certificates WHERE name = ''' + @CertificateName + ''') ' +
                           'BEGIN ' +
                           'USE master; CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) +
                           ' WITH SUBJECT = ''TDE Certificate'', ' + CHAR(10) +
                           'EXPIRY_DATE = ''' + CONVERT(NVARCHAR(10), DATEADD(YEAR, 10, GETDATE()), 120) + ''';' + CHAR(10) +
                           'WAITFOR DELAY ''00:00:05''; END';
                IF @PrintOnly = 1
                    PRINT @SQL + CHAR(10);
                ELSE
                    EXEC sp_executesql @SQL;
                SET @CertificateExists = 1;
            END
            ELSE
            BEGIN
                PRINT 'Certificate ' + @CertificateName + ' already exists in master.' + CHAR(10);
            END
        END

        -- 4. Backup Certificate (if requested)
        IF @BackupCertificate = 1
        BEGIN
            IF @CertificateExists = 0
            BEGIN
                RAISERROR('Certificate does not exist. Cannot backup.', 16, 1);
                RETURN;
            END
            IF @CertificatePassword IS NULL OR @CertificatePassword = ''
            BEGIN
                RAISERROR('CertificatePassword is required for certificate backup.', 16, 1);
                RETURN;
            END
            IF @BackupDirectory IS NULL OR @BackupDirectory = ''
            BEGIN
                EXEC master.dbo.xp_instance_regread 
                    N'HKEY_LOCAL_MACHINE', 
                    N'Software\Microsoft\MSSQLServer\MSSQLServer', 
                    N'BackupDirectory', 
                    @BackupDirectory OUTPUT;
            END
            IF RIGHT(@BackupDirectory, 1) != '\'
                SET @BackupDirectory = @BackupDirectory + '\';
            SET @ServerName = REPLACE(@@SERVERNAME, '\\', '_');
            SET @Timestamp = REPLACE(REPLACE(CONVERT(NVARCHAR(19), GETDATE(), 120), ' ', '_'), ':', '');
            SET @BackupFileCert = @BackupDirectory + @CertificateName + '_' + @ServerName + '_' + @Timestamp + '.cer';
            SET @BackupFileKey = @BackupDirectory + @CertificateName + '_' + @ServerName + '_' + @Timestamp + '.pvk';
            SET @SQL = 'USE master; BACKUP CERTIFICATE ' + QUOTENAME(@CertificateName) + CHAR(10) +
                       'TO FILE = ''' + @BackupFileCert + ''' ' + CHAR(10) +
                       'WITH PRIVATE KEY (FILE = ''' + @BackupFileKey + ''', ' + CHAR(10) +
                       'ENCRYPTION BY PASSWORD = ''' + @CertificatePassword + ''');' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';';
            IF @PrintOnly = 1
                PRINT @SQL + CHAR(10);
            ELSE
                EXEC sp_executesql @SQL;
        END

        -- 5. Enable TDE (if requested)
        IF @EnableTDE = 1
        BEGIN
            IF @DatabaseName IS NULL OR @DatabaseName = ''
            BEGIN
                RAISERROR('DatabaseName is required to enable TDE.', 16, 1);
                RETURN;
            END
            IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName)
            BEGIN
                RAISERROR('Specified database does not exist.', 16, 1);
                RETURN;
            END
            IF @EncryptionAlgorithm NOT IN ('AES_128', 'AES_192', 'AES_256', 'TRIPLE_DES_3KEY')
            BEGIN
                RAISERROR('Invalid encryption algorithm.', 16, 1);
                RETURN;
            END
            IF @CertificateExists = 0
            BEGIN
                RAISERROR('Certificate does not exist. Cannot enable TDE.', 16, 1);
                RETURN;
            END
            -- Idempotent DEK creation
            IF NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName))
            BEGIN
                SET @SQL = 'USE ' + QUOTENAME(@DatabaseName) + ';' + CHAR(10) +
                           'WAITFOR DELAY ''00:00:05'';' + CHAR(10) +
                           'IF NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(''' + @DatabaseName + ''')) ' +
                           'BEGIN ' +
                           'CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = ' + @EncryptionAlgorithm + CHAR(10) +
                           'ENCRYPTION BY SERVER CERTIFICATE ' + QUOTENAME(@CertificateName) + ';' + CHAR(10) +
                           'WAITFOR DELAY ''00:00:05'';' + CHAR(10) +
                           'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET ENCRYPTION ON;' + CHAR(10) +
                           'WAITFOR DELAY ''00:00:05''; END';
                IF @PrintOnly = 1
                    PRINT @SQL + CHAR(10);
                ELSE
                    EXEC sp_executesql @SQL;
            END
            ELSE
            BEGIN
                PRINT 'TDE is already enabled on database ' + @DatabaseName + '.' + CHAR(10);
            END
        END
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        RAISERROR('Error in usp_EnableTDE: %s', 16, 1, @ErrorMessage);
        RETURN;
    END CATCH
END;
GO


--
-- Parameter summary:
-- @DatabaseName: Target database for TDE (optional for some actions)
-- @CertificateName: Name of the TDE certificate
-- @EncryptionAlgorithm: Algorithm for TDE (only used if enabling TDE)
-- @MasterKeyPassword: Password for creating master key (required if creating)
-- @CertificatePassword: Password for creating or backing up certificate (required for backup, creation)
-- @CreateMasterKey: 1 = Create master key if not exists
-- @CreateCertificate: 1 = Create certificate if not exists
-- @BackupCertificate: 1 = Backup certificate
-- @BackupDirectory: Directory for backup files (optional, will use default if not provided)
-- @RestoreCertificate: 1 = Restore certificate
-- @CertFilePath: Path to .cer file for restore
-- @PrivateKeyFilePath: Path to .pvk file for restore
-- @PrivateKeyPassword: Password for .pvk file (restore)
-- @EnableTDE: 1 = Enable TDE on database
-- @PrintOnly: 1 = Print SQL, 0 = Execute 
