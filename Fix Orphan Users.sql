USE [tempdb]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_FixOrphanUsers]') AND type in (N'P', N'PC'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[usp_FixOrphanUsers] AS' 
END
GO

ALTER PROCEDURE [dbo].[usp_FixOrphanUsers]
    @DatabaseName NVARCHAR(255) = NULL,
    @Print        BIT = 0,
    @Execute      BIT = 1,
    @RaiseError   BIT = 0 -- 1: Fails the batch on FIRST error. 0: Rolls back the failed DB, logs it, and moves to the next.
AS
BEGIN
    SET NOCOUNT ON;

    IF @DatabaseName IS NOT NULL AND @DatabaseName <> ''
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName AND state_desc = 'ONLINE')
        BEGIN
            RAISERROR('The database %s does not exist or is not online.', 16, 1, @DatabaseName);
            RETURN;
        END
    END

    DECLARE @CurrentDBName NVARCHAR(128);
    DECLARE @UserName      NVARCHAR(128);
    DECLARE @DynamicSQL    NVARCHAR(MAX);
    DECLARE @ErrorMessage  NVARCHAR(2048);

    IF OBJECT_ID('tempdb..#OrphanUsers') IS NOT NULL 
        DROP TABLE #OrphanUsers;

    CREATE TABLE #OrphanUsers (
        DBName   NVARCHAR(128),
        UserName NVARCHAR(128)
    );

    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT name
    FROM sys.databases
    WHERE state_desc = 'ONLINE'
      AND is_read_only = 0
      AND name NOT IN ('master', 'model', 'msdb', 'tempdb')
      AND (name = @DatabaseName OR @DatabaseName IS NULL OR @DatabaseName = '');

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @CurrentDBName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- ADDED 'G' to catch orphaned Windows Groups
        SET @DynamicSQL = N'
            INSERT INTO #OrphanUsers (DBName, UserName)
            SELECT
                @DBName AS DBName,
                name AS UserName
            FROM ' + QUOTENAME(@CurrentDBName) + N'.sys.database_principals
            WHERE type IN (''S'', ''U'', ''G'') 
              AND authentication_type IN (1, 2, 3) 
              AND sid IS NOT NULL AND sid <> 0x0
              AND SUSER_SNAME(sid) IS NULL
              AND name NOT IN (''dbo'', ''guest'', ''INFORMATION_SCHEMA'', ''sys'')';
        
        EXEC sp_executesql @DynamicSQL, N'@DBName NVARCHAR(128)', @DBName = @CurrentDBName;
        
        FETCH NEXT FROM db_cursor INTO @CurrentDBName;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    DECLARE user_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT UserName, DBName FROM #OrphanUsers;

    OPEN user_cursor;
    FETCH NEXT FROM user_cursor INTO @UserName, @CurrentDBName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @UserName AND type IN ('S', 'U', 'G'))
        BEGIN
            SET @DynamicSQL = N'ALTER USER ' + QUOTENAME(@UserName) + N' WITH LOGIN = ' + QUOTENAME(@UserName) + N';';
        END
        ELSE
        BEGIN
            -- Transaction wrapping added inside the dynamic execution context
            SET @DynamicSQL = N'
                DECLARE @SchemaFixes NVARCHAR(MAX) = '''';
                DECLARE @RoleFixes   NVARCHAR(MAX) = '''';
                DECLARE @FinalDrop   NVARCHAR(MAX) = N''DROP USER ' + QUOTENAME(@UserName) + N';'';

                SELECT @SchemaFixes = ISNULL(STUFF((
                    SELECT CHAR(13) + N''ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(name) + N'' TO [dbo];''
                    FROM sys.schemas 
                    WHERE principal_id = USER_ID(@TargetUser)
                    FOR XML PATH(N''''), TYPE).value(N''.'', N''NVARCHAR(MAX)''), 1, 1, N''''), '''');

                SELECT @RoleFixes = ISNULL(STUFF((
                    SELECT CHAR(13) + N''ALTER AUTHORIZATION ON ROLE::'' + QUOTENAME(name) + N'' TO [dbo];''
                    FROM sys.database_principals 
                    WHERE type = ''R'' AND owning_principal_id = USER_ID(@TargetUser)
                    FOR XML PATH(N''''), TYPE).value(N''.'', N''NVARCHAR(MAX)''), 1, 1, N''''), '''');

                DECLARE @FullTask NVARCHAR(MAX) = ISNULL(@SchemaFixes, '''') + CHAR(13) + ISNULL(@RoleFixes, '''') + CHAR(13) + @FinalDrop;
                
                IF @ExecuteInner = 1
                BEGIN
                    BEGIN TRY
                        BEGIN TRAN;
                            EXEC sp_executesql @FullTask;
                        COMMIT TRAN;
                    END TRY
                    BEGIN CATCH
                        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
                        THROW; -- Re-throw to be caught by the outer loop
                    END CATCH
                END
                ELSE
                BEGIN
                    PRINT @FullTask; 
                END';
        END

        IF @Print = 1
        BEGIN
            PRINT '-- =================================================='
            PRINT '-- Database: ' + @CurrentDBName + ' | User: ' + @UserName;
            PRINT '-- =================================================='
            PRINT @DynamicSQL;
        END

        IF @Execute = 1 OR @Print = 1
        BEGIN
            BEGIN TRY
                DECLARE @ExecContext NVARCHAR(500) = QUOTENAME(@CurrentDBName) + N'..sp_executesql';
                
                EXEC @ExecContext @DynamicSQL, 
                                  N'@TargetUser NVARCHAR(128), @ExecuteInner BIT', 
                                  @TargetUser = @UserName, 
                                  @ExecuteInner = @Execute;
            END TRY
            BEGIN CATCH
                -- Custom error payload so SQL Agent actually tells you what failed
                SET @ErrorMessage = ERROR_MESSAGE();
                DECLARE @CustomError NVARCHAR(2048) = N'Failed to process User [' + @UserName + N'] in DB [' + @CurrentDBName + N']. Original Error: ' + @ErrorMessage;
                
                PRINT @CustomError;
                
                IF @RaiseError = 1
                BEGIN
                    -- Throws the CUSTOM error, breaking the batch.
                    THROW 50000, @CustomError, 1;
                END
            END CATCH
        END

        FETCH NEXT FROM user_cursor INTO @UserName, @CurrentDBName;
    END

    CLOSE user_cursor;
    DEALLOCATE user_cursor;
    DROP TABLE #OrphanUsers;
END
GO
