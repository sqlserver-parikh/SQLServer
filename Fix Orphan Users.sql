USE TEMPDB
GO
CREATE OR ALTER PROCEDURE usp_FixOrphanUsers
    @DatabaseName NVARCHAR(255) = '',
    @PrintOnly BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @userid VARCHAR(255);
    DECLARE @dbname VARCHAR(128);
    DECLARE @script NVARCHAR(MAX);

    CREATE TABLE #OrphanUsers
    (
        DBName   VARCHAR(128),
        UserName VARCHAR(128),
        UserSID  NVARCHAR(255)
    );

  IF @DatabaseName IS NULL OR @DatabaseName = ''
  BEGIN
        INSERT INTO #OrphanUsers
		EXEC sp_MSforeachdb
        'IF DATABASEPROPERTYEX(''?'', ''IsReadOnly'') = 0 AND DATABASEPROPERTYEX(''?'', ''Status'') = ''ONLINE''
         BEGIN
             SELECT ''?'' AS DBName, name, sid 
             FROM [?]..sysusers
             WHERE issqluser = 1
               AND (sid IS NOT NULL AND sid <> 0x0)
               AND (LEN(sid) <= 16)
               AND SUSER_SNAME(sid) IS NULL
             ORDER BY name
         END';
    END
    ELSE
    BEGIN
        SET @script = 'USE ' + QUOTENAME(@DatabaseName) + '; ' +
                      'INSERT INTO #OrphanUsers (DBName, UserName, UserSID) ' +
                      'SELECT ''' + @DatabaseName + ''', name, sid ' +
                      'FROM sysusers ' +
                      'WHERE issqluser = 1 ' +
                      'AND (sid IS NOT NULL AND sid <> 0x0) ' +
                      'AND (LEN(sid) <= 16) ' +
                      'AND SUSER_SNAME(sid) IS NULL ' +
                      'ORDER BY name;';
        EXEC sp_executesql @script;
    END

    DECLARE FixUser CURSOR FOR
    SELECT UserName, DBName
    FROM #OrphanUsers;

    OPEN FixUser;
    FETCH NEXT FROM FixUser INTO @userid, @dbname;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @userid)
        BEGIN
            SET @script = 'USE ' + QUOTENAME(@dbname) + ';' + CHAR(10) +
                          'EXEC sp_change_users_login ''update_one'', ''' + @userid + ''', ''' + @userid + ''';';
            IF @PrintOnly = 1
            BEGIN
                PRINT @script;
            END
            ELSE
            BEGIN
                EXEC sp_executesql @script;
                PRINT @script;
            END
        END
        ELSE
        BEGIN
            IF EXISTS (SELECT name FROM sys.schemas WHERE principal_id = USER_ID(@userid))
            BEGIN
                SET @script = 'USE ' + QUOTENAME(@dbname) + ';' + CHAR(10) +
                              'DROP USER ' + QUOTENAME(@userid) + ';' + CHAR(10);
                IF @PrintOnly = 1
                BEGIN
                    PRINT @script;
                END
                ELSE
                BEGIN
                    EXEC sp_executesql @script;
                    PRINT @script;
                END
            END
        END

        FETCH NEXT FROM FixUser INTO @userid, @dbname;
    END

    CLOSE FixUser;
    DEALLOCATE FixUser;
    DROP TABLE #OrphanUsers;
END
