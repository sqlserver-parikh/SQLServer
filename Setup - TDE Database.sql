USE tempdb;
GO

CREATE OR ALTER PROCEDURE dbo.usp_EnableTDE
    @DatabaseName NVARCHAR(128) = NULL,
    @CertificateName NVARCHAR(128) = 'TDE_Certificate',
    @EncryptionAlgorithm NVARCHAR(32) = 'AES_256',
    @MasterKeyPassword NVARCHAR(128) = NULL,
    @BackupCertificate BIT = 0,
    @BackupPath NVARCHAR(256) = NULL,
    @PrintOnly BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ErrorMessage NVARCHAR(500);
    DECLARE @CertificateExists INT;
    DECLARE @MasterKeyExists INT;
    DECLARE @BackupFileCert NVARCHAR(256);
    DECLARE @BackupFileKey NVARCHAR(256);
    DECLARE @DefaultBackupPath NVARCHAR(256);
    DECLARE @RegKey NVARCHAR(512);
    DECLARE @ValueName NVARCHAR(512) = 'BackupDirectory';
    DECLARE @Value NVARCHAR(512);
    DECLARE @IsSMKEncrypted BIT = 0;
    DECLARE @SQLMasterKey NVARCHAR(MAX) = '';
    DECLARE @ServerName NVARCHAR(128);
    DECLARE @Timestamp NVARCHAR(20);

    BEGIN TRY
        -- Validate input parameters
        IF @BackupCertificate = 0
        BEGIN
            IF @DatabaseName IS NULL OR @DatabaseName = ''
            BEGIN
                RAISERROR('DatabaseName parameter is required when not performing backup operation.', 16, 1);
                RETURN;
            END

            IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName)
            BEGIN
                RAISERROR('Specified database does not exist.', 16, 1);
                RETURN;
            END
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

        -- Check if master key exists
        SET @MasterKeyExists = (SELECT COUNT(*) FROM master.sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##');

        -- Check if certificate exists
        SET @CertificateExists = (SELECT COUNT(*) FROM master.sys.certificates WHERE name = @CertificateName);

        -- Validate MasterKeyPassword when required
        IF (@MasterKeyExists = 0 OR @CertificateExists = 0 OR @BackupCertificate = 1)
            AND (@MasterKeyPassword IS NULL OR @MasterKeyPassword = '')
        BEGIN
            RAISERROR('MasterKeyPassword is required when creating a master key, certificate, or backing up the certificate.', 16, 1);
            RETURN;
        END

        -- Create master key if it doesn't exist
        IF @MasterKeyExists = 0
        BEGIN
            SET @SQL = 'USE master;' + CHAR(10) +
                       'CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + @MasterKeyPassword + ''';' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';';
            IF @PrintOnly = 1
                PRINT @SQL + CHAR(10);
            ELSE
                EXEC sp_executesql @SQL;
        END

        -- Create certificate if it doesn't exist
        IF @CertificateExists = 0
        BEGIN
            SET @SQL = 'USE master;' + CHAR(10) +
                       'CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) +
                       ' WITH SUBJECT = ''TDE Certificate'', ' + CHAR(10) +
                       'EXPIRY_DATE = ''' + CONVERT(NVARCHAR(10), DATEADD(YEAR, 10, GETDATE()), 120) + ''';' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';';
            IF @PrintOnly = 1
                PRINT @SQL + CHAR(10);
            ELSE
                EXEC sp_executesql @SQL;
        END

        -- Backup certificate if requested
        IF @BackupCertificate = 1
        BEGIN
            IF @BackupPath IS NULL
            BEGIN
                EXEC master.dbo.xp_instance_regread 
                    N'HKEY_LOCAL_MACHINE', 
                    N'Software\Microsoft\MSSQLServer\MSSQLServer', 
                    N'BackupDirectory', 
                    @BackupPath OUTPUT;
            END

            IF RIGHT(@BackupPath, 1) != '\'
                SET @BackupPath = @BackupPath + '\';

            -- Get server name and timestamp
            SET @ServerName = REPLACE(@@SERVERNAME, '\', '_'); -- Replace backslash with underscore for valid filename
            SET @Timestamp = REPLACE(REPLACE(CONVERT(NVARCHAR(19), GETDATE(), 120), ' ', '_'), ':', '');

            SET @BackupFileCert = @BackupPath + @CertificateName + '_' + @ServerName + '_' + @Timestamp + '.cer';
            SET @BackupFileKey = @BackupPath + @CertificateName + '_' + @ServerName + '_' + @Timestamp + '.pvk';

            SET @SQL = 'USE master; BACKUP CERTIFICATE ' + QUOTENAME(@CertificateName) + CHAR(10) +
                       'TO FILE = ''' + @BackupFileCert + ''' ' + CHAR(10) +
                       'WITH PRIVATE KEY (FILE = ''' + @BackupFileKey + ''', ' + CHAR(10) +
                       'ENCRYPTION BY PASSWORD = ''' + @MasterKeyPassword + ''');' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';';
            IF @PrintOnly = 1
                PRINT @SQL + CHAR(10);
            ELSE
                EXEC sp_executesql @SQL;
        END

        -- Create DEK and enable TDE
        IF NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(@DatabaseName))
        BEGIN
            SET @SQL = 'USE ' + QUOTENAME(@DatabaseName) + ';' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';' + CHAR(10) +
                       'CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = ' + @EncryptionAlgorithm + CHAR(10) +
                       'ENCRYPTION BY SERVER CERTIFICATE ' + QUOTENAME(@CertificateName) + ';' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';' + CHAR(10) +
                       'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET ENCRYPTION ON;' + CHAR(10) +
                       'WAITFOR DELAY ''00:00:05'';';
            IF @PrintOnly = 1
                PRINT @SQL + CHAR(10);
            ELSE
                EXEC sp_executesql @SQL;
        END
        ELSE
        BEGIN
            PRINT 'TDE is already enabled on database ' + @DatabaseName + '.' + CHAR(10);
        END
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        RAISERROR('Error enabling TDE: %s', 16, 1, @ErrorMessage);
        RETURN;
    END CATCH
END;
GO
