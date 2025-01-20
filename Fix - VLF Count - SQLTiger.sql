USE tempdb
GO
CREATE OR ALTER PROCEDURE usp_FixVLFIssues
    @DBName NVARCHAR(255) = NULL,
    @PerformLogBackup BIT = 0,
    @Print BIT = 1,
    @OnlySetGrowth BIT = 0,
    @MinVLF INT = 500
AS
BEGIN
--majority code is taken from SQLTiger team
    SET NOCOUNT ON;

    DECLARE @query VARCHAR(1000), @dbname_loop VARCHAR(255), @count int, @usedlogsize bigint, @logsize bigint;
    DECLARE @sqlcmd NVARCHAR(1000), @sqlparam NVARCHAR(100), @filename VARCHAR(255), @i int, @recmodel NVARCHAR(128);
    DECLARE @potsize int, @n_iter int, @n_iter_final int, @initgrow int, @n_init_iter int, @bckpath NVARCHAR(255);
    DECLARE @majorver smallint, @minorver smallint, @build smallint;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @sqlShrink NVARCHAR(MAX), @sqlBackup NVARCHAR(MAX), @sqlGrowth NVARCHAR(MAX);

    CREATE TABLE #loginfo (dbname varchar(100), num_of_rows int, used_logsize_MB DECIMAL(20,1));
    CREATE TABLE #databases (dbname varchar(100));

    DECLARE @tblvlf TABLE (
        dbname varchar(100), 
        Actual_log_size_MB DECIMAL(20,1), 
        Potential_log_size_MB DECIMAL(20,1), 
        Actual_VLFs int, 
        Potential_VLFs int, 
        Growth_iterations int,
        Log_Initial_size_MB DECIMAL(20,1), 
        File_autogrow_MB DECIMAL(20,1)
    );
        
    SELECT TOP 1 @bckpath = REVERSE(RIGHT(REVERSE(physical_device_name), 
        LEN(physical_device_name)-CHARINDEX('\',REVERSE(physical_device_name),0))) 
    FROM msdb.dbo.backupmediafamily 
    WHERE device_type = 2;

    SELECT @majorver = (@@microsoftversion / 0x1000000) & 0xff, 
           @minorver = (@@microsoftversion / 0x10000) & 0xff, 
           @build = @@microsoftversion & 0xffff;
     
    IF @DBName IS NULL
        INSERT INTO #databases
        SELECT name 
        FROM master.sys.databases 
        WHERE is_read_only = 0 
        AND state = 0 
        AND database_id <> 2;
    ELSE
        INSERT INTO #databases
        SELECT name 
        FROM master.sys.databases 
        WHERE is_read_only = 0 
        AND state = 0 
        AND database_id <> 2
        AND name = @DBName;

    DECLARE db_cursor CURSOR FAST_FORWARD FOR 
        SELECT dbname FROM #databases;

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @dbname_loop;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Check for active operations
            IF EXISTS (
                SELECT 1 
                FROM sys.dm_exec_requests r
                JOIN sys.databases d ON r.database_id = d.database_id
                WHERE d.name = @dbname_loop
                AND (
                    r.command LIKE '%DBCC%'
                    OR r.command LIKE '%BACKUP%'
                    OR r.command LIKE '%INDEX%'
                    OR r.command LIKE '%CREATE%INDEX%'
                    OR r.command LIKE '%ALTER%INDEX%'
                    OR r.command LIKE '%REBUILD%'
                    OR r.command LIKE '%REORGANIZE%'
                )
            )
            BEGIN
                PRINT 'Skipping database ' + @dbname_loop + ' due to active maintenance operations.';
                FETCH NEXT FROM db_cursor INTO @dbname_loop;
                CONTINUE;
            END

            CREATE TABLE #log_info (
                recoveryunitid int NULL,
                fileid tinyint,
                file_size bigint,
                start_offset bigint,
                FSeqNo int,
                [status] tinyint,
                parity tinyint,
                create_lsn numeric(25,0)
            );

            SET @query = 'DBCC LOGINFO (' + QUOTENAME(@dbname_loop, '''') + ') WITH NO_INFOMSGS';

            IF @majorver < 11
            BEGIN
                INSERT INTO #log_info (fileid, file_size, start_offset, FSeqNo, [status], parity, create_lsn)
                EXEC (@query);
            END
            ELSE
            BEGIN
                INSERT INTO #log_info (recoveryunitid, fileid, file_size, start_offset, FSeqNo, [status], parity, create_lsn)
                EXEC (@query);
            END;

            SET @count = @@ROWCOUNT;
            SET @usedlogsize = (
                SELECT (MIN(l.start_offset) + SUM(CASE WHEN l.status <> 0 THEN l.file_size ELSE 0 END))/1024.00/1024.00 
                FROM #log_info l
            );

            DROP TABLE #log_info;

            INSERT INTO #loginfo (dbname, num_of_rows, used_logsize_MB)
            VALUES (@dbname_loop, @count, @usedlogsize);

            FETCH NEXT FROM db_cursor INTO @dbname_loop;
        END TRY
        BEGIN CATCH
            PRINT 'Error checking database ' + @dbname_loop + ': ' + ERROR_MESSAGE();
            FETCH NEXT FROM db_cursor INTO @dbname_loop;
        END CATCH
    END;

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    DECLARE shrink_cursor CURSOR FAST_FORWARD FOR 
    SELECT dbname, num_of_rows 
    FROM #loginfo 
    WHERE num_of_rows >= @MinVLF
    ORDER BY dbname;

    OPEN shrink_cursor;
    FETCH NEXT FROM shrink_cursor INTO @dbname_loop, @count;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Check for active operations again
            IF EXISTS (
                SELECT 1 
                FROM sys.dm_exec_requests r
                JOIN sys.databases d ON r.database_id = d.database_id
                WHERE d.name = @dbname_loop
                AND (
                    r.command LIKE '%DBCC%'
                    OR r.command LIKE '%BACKUP%'
                    OR r.command LIKE '%INDEX%'
                    OR r.command LIKE '%CREATE%INDEX%'
                    OR r.command LIKE '%ALTER%INDEX%'
                    OR r.command LIKE '%REBUILD%'
                    OR r.command LIKE '%REORGANIZE%'
                )
            )
            BEGIN
                PRINT 'Skipping database ' + @dbname_loop + ' due to active maintenance operations.';
                FETCH NEXT FROM shrink_cursor INTO @dbname_loop, @count;
                CONTINUE;
            END

            SET @sqlcmd = N'SELECT @nameout = name, @logsizeout = (size*8)/1024 
                           FROM [' + @dbname_loop + '].dbo.sysfiles 
                           WHERE (64 & status) = 64';
            SET @sqlparam = N'@nameout NVARCHAR(100) OUTPUT, @logsizeout bigint OUTPUT';
            
            DECLARE @nameout NVARCHAR(100), @logsizeout bigint;
            EXEC sp_executesql @sqlcmd, @sqlparam, 
                 @nameout = @nameout OUTPUT, 
                 @logsizeout = @logsizeout OUTPUT;

            SET @filename = @nameout;
            SET @logsize = @logsizeout;

            -- Initialize command strings
            SET @sqlShrink = N'';
            SET @sqlBackup = N'';
            SET @sqlGrowth = N'';

            -- Build shrink and backup commands if not in OnlySetGrowth mode
            IF @OnlySetGrowth = 0
            BEGIN
                SET @sqlShrink = N'USE [' + @dbname_loop + N']; DBCC SHRINKFILE (N''' + @filename + ''', 1, TRUNCATEONLY);';

                IF @PerformLogBackup = 1
                BEGIN
                    SET @sqlBackup = N'BACKUP LOG [' + @dbname_loop + N'] TO DISK = ''' + @bckpath + 
                        '\' + @dbname_loop + '_' + CONVERT(VARCHAR(8), GETDATE(), 112) + '_' + 
                        REPLACE(CONVERT(VARCHAR(8), GETDATE(), 108), ':', '') + '.trn'';';
                END
            END

            -- Calculate size settings
            IF @majorver >= 11
            BEGIN
                SET @n_iter = 
                    CASE 
                        WHEN @logsize <= 64 THEN 1
                        WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)
                        WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)
                        WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)
                        WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)
                        WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4096, 0)
                        WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8192, 0)
                    END;

                SET @potsize = 
                    CASE 
                        WHEN @logsize <= 64 THEN 1*64
                        WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)*256
                        WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)*512
                        WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)*1024
                        WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)*2048
                        WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4096, 0)*4096
                        WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8192, 0)*8192
                    END;
            END
            ELSE
            BEGIN
                SET @n_iter = 
                    CASE 
                        WHEN @logsize <= 64 THEN 1
                        WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)
                        WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)
                        WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)
                        WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)
                        WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4000, 0)
                        WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8000, 0)
                    END;

                SET @potsize = 
                    CASE 
                        WHEN @logsize <= 64 THEN 1*64
                        WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)*256
                        WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)*512
                        WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)*1024
                        WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)*2048
                        WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4000, 0)*4000
                        WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8000, 0)*8000
                    END;
            END;

            SET @n_iter_final = @n_iter;
            IF @logsize > @potsize AND @potsize <= 4096 AND ABS(@logsize - @potsize) < 512
                SET @n_iter_final = @n_iter + 1;
            ELSE IF @logsize < @potsize AND @potsize <= 51200 AND ABS(@logsize - @potsize) > 1024
                SET @n_iter_final = @n_iter - 1;

            IF @potsize = 0 
                SET @potsize = 64;
            IF @n_iter = 0 
                SET @n_iter = 1;

            SET @potsize = 
                CASE 
                    WHEN @n_iter < @n_iter_final THEN @potsize + (@potsize/@n_iter)
                    WHEN @n_iter > @n_iter_final THEN @potsize - (@potsize/@n_iter)
                    ELSE @potsize 
                END;

            SET @n_init_iter = @n_iter_final;
            IF @potsize >= 8192
                SET @initgrow = @potsize/@n_iter_final;

            IF @potsize >= 64 AND @potsize <= 512
            BEGIN
                SET @n_init_iter = 1;
                SET @initgrow = 512;
            END;

            IF @potsize > 512 AND @potsize <= 1024
            BEGIN
                SET @n_init_iter = 1;
                SET @initgrow = 1023;
            END;

            IF @potsize > 1024 AND @potsize < 8192
            BEGIN
                SET @n_init_iter = 1;
                SET @initgrow = @potsize;
            END;

            INSERT INTO @tblvlf
            SELECT @dbname_loop, @logsize, @potsize, @count,
                CASE 
                    WHEN @potsize <= 64 THEN (@potsize/(@potsize/@n_init_iter))*4
                    WHEN @potsize > 64 AND @potsize < 1024 THEN (@potsize/(@potsize/@n_init_iter))*8
                    WHEN @potsize >= 1024 THEN (@potsize/(@potsize/@n_init_iter))*16
                END,
                @n_init_iter, @initgrow, 
                CASE WHEN (@potsize/@n_iter_final) <= 1024 THEN (@potsize/@n_iter_final) ELSE 1024 END;

            IF @n_init_iter > 4 
                SET @n_init_iter = 4;

            -- Build growth commands
            SET @sqlGrowth = N'USE [master]; ' + CHAR(13);

            -- Add initial growth setting
            SET @sqlGrowth = @sqlGrowth + 
                N'ALTER DATABASE [' + @dbname_loop + N'] MODIFY FILE ( NAME = N''' + @filename + 
                ''', FILEGROWTH = ' + 
                CASE WHEN (@potsize/@n_iter_final) <= 1024 
                     THEN CONVERT(VARCHAR, (@potsize/@n_iter_final)) 
                     ELSE '1024' 
                END + N'MB );' + CHAR(13);

            IF @OnlySetGrowth = 0
            BEGIN
                -- Add size modifications
                SET @i = 1;
                WHILE @i <= @n_init_iter
                BEGIN
                    SET @sqlGrowth = @sqlGrowth + 
                        N'ALTER DATABASE [' + @dbname_loop + N'] MODIFY FILE ( NAME = N''' + @filename + 
                        ''', SIZE = ' + CONVERT(VARCHAR, @initgrow*@i) + N'MB );' + CHAR(13);
                    SET @i = @i + 1;
                END;
            END;

            -- Execute or print commands
            IF @Print = 1
            BEGIN
                IF LEN(@sqlShrink) > 0 PRINT @sqlShrink;
                IF LEN(@sqlBackup) > 0 PRINT @sqlBackup;
                IF LEN(@sqlGrowth) > 0 PRINT @sqlGrowth;
            END
            ELSE
            BEGIN
                IF LEN(@sqlShrink) > 0 
                BEGIN
                    BEGIN TRY
                        EXEC sp_executesql @sqlShrink;
                    END TRY
                    BEGIN CATCH
                        PRINT 'Error executing shrink for database ' + @dbname_loop + ': ' + ERROR_MESSAGE();
                    END CATCH
                END

                IF LEN(@sqlBackup) > 0 
                BEGIN
                    BEGIN TRY
                        EXEC sp_executesql @sqlBackup;
                    END TRY
                    BEGIN CATCH
                        PRINT 'Error executing backup for database ' + @dbname_loop + ': ' + ERROR_MESSAGE();
                    END CATCH
                END

                IF LEN(@sqlGrowth) > 0 
                BEGIN
                    BEGIN TRY
                        EXEC sp_executesql @sqlGrowth;
                        PRINT 'Successfully processed database: ' + @dbname_loop;
                    END TRY
                    BEGIN CATCH
                        PRINT 'Error executing growth settings for database ' + @dbname_loop + ': ' + ERROR_MESSAGE();
                    END CATCH
                END
            END

        END TRY
        BEGIN CATCH
            PRINT 'Error processing database ' + @dbname_loop + ':';
            PRINT 'Error Message: ' + ERROR_MESSAGE();
            PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
            PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
            PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
            PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
        END CATCH

        FETCH NEXT FROM shrink_cursor INTO @dbname_loop, @count;
    END;

    CLOSE shrink_cursor;
    DEALLOCATE shrink_cursor;

    -- Return final results
    SELECT 
        dbname AS [Database_Name], 
        Actual_log_size_MB, 
        Potential_log_size_MB, 
        Actual_VLFs, 
        Potential_VLFs, 
        Growth_iterations, 
        Log_Initial_size_MB, 
        File_autogrow_MB,
		B.log_reuse_wait_desc LogReuseWaitDesc
    FROM @tblvlf A
			INNER JOIN sys.databases B ON A.dbname = B.name

    ORDER BY Database_Name;

    -- Cleanup
    DROP TABLE #loginfo;
    DROP TABLE #databases;
END;
GO
EXEC usp_FixVLFIssues
