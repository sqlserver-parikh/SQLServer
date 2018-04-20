--Below script will auto fix orphan users and create script to drop user if no associated login found for particular database.
SET NOCOUNT ON;
DECLARE @userid VARCHAR(255);
CREATE TABLE #OrphanUsers
(UserName VARCHAR(100),
 USID     NVARCHAR(255)
);
INSERT INTO #OrphanUsers
EXEC sp_change_users_login
     'report';
DECLARE FixUser CURSOR
FOR SELECT UserName
    FROM #OrphanUsers;
OPEN FixUser;
FETCH NEXT FROM FixUser INTO @userid;
WHILE @@FETCH_STATUS = 0
    BEGIN TRY
        EXEC sp_change_users_login
             'update_one',
             @userid,
             @userid;
        PRINT '--User '+@userid+' is mapped;';
        FETCH NEXT FROM FixUser INTO @userid;
    END TRY
    BEGIN CATCH
        PRINT 'DROP user '+@userid+';';
        FETCH NEXT FROM FixUser INTO @userid;
    END CATCH;
CLOSE FixUser;
DEALLOCATE FixUser;
DROP TABLE #OrphanUsers;

/*
--Below script will loop through all database and generate script to map user and drop user.
SET NOCOUNT ON;
DECLARE @userid VARCHAR(255);
DECLARE @dbname VARCHAR(128);
DECLARE @script NVARCHAR(MAX);
CREATE TABLE #OrphanUsers
(DBName   VARCHAR(128),
 UserName VARCHAR(128),
 UserSID  NVARCHAR(255)
);
INSERT INTO #OrphanUsers
EXEC sp_MSforeachdb
'select "?" DBName,name, sid from [?]..sysusers
            where issqluser = 1
            and   (sid is not null and sid <> 0x0)
            and   (len(sid) <= 16)
            and   suser_sname(sid) is null
            order by name';
DECLARE FixUser CURSOR
FOR SELECT UserName,
           DBName
    FROM #OrphanUsers;
OPEN FixUser;
FETCH NEXT FROM FixUser INTO @userid, @DBName;
WHILE @@FETCH_STATUS = 0
    IF EXISTS
(
    SELECT 1
    FROM sys.server_principals
    WHERE name = @userid
)
        BEGIN
            SET @script = 'USE '+QUOTENAME(@dbname)+';'+CHAR(10)+'EXECUTE sp_change_users_login ''update_one'', '''+@userid+''', '''+@userid+'''';
            EXEC sp_executesql
                 @script;
            PRINT @script;
            FETCH NEXT FROM FixUser INTO @userid, @DBName;
        END;
        ELSE
        BEGIN
            IF EXISTS
(
    SELECT name
    FROM sys.schemas
    WHERE principal_id = USER_ID(@userid)
)
                BEGIN
                    SET @script = 'USE '+QUOTENAME(@dbname)+';'+CHAR(10)+'DROP USER '+QUOTENAME(@userid)+';'+CHAR(10);
                    EXEC sp_executesql
                         @script;
                    PRINT @script;
                END;
            FETCH NEXT FROM FixUser INTO @userid, @DBName;
        END;
CLOSE FixUser;
DEALLOCATE FixUser;
DROP TABLE #OrphanUsers;
*/
