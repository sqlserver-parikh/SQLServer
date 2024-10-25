USE tempdb
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblFileStats]') AND type in (N'U'))
DROP TABLE tblFileStats
GO
CREATE PROCEDURE [dbo].[usp_FileStats](@retentiondays INT = 4, @waitfordelaysec int = 30)
AS
     SET NOCOUNT ON;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tblFileStats]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[tblFileStats](
	[ServerName] sysname NULL,
	[DBName] [nvarchar](128) NULL,
	[FileID] [smallint] NOT NULL,
	[NumOfReads] [bigint] NULL,
	[NumOfWrites] [bigint] NULL,
	[IOStallReadMS] [bigint] NULL,
	[IOStallWriteMS] [bigint] NULL,
	[NumOfBytesRead] [bigint] NULL,
	[NumOfBytesWritten] [bigint] NULL,
	[IOStall] [bigint] NULL,
	[SizeOnDiskBytes] [bigint] NULL,
	[ReadLatency] [bigint] NULL,
	[WriteLatency] [bigint] NULL,
	[FileType] [nvarchar](60) NULL,
	[FileLocation] [nvarchar](1080) NOT NULL,
	[RunTimeUTC] [datetime] NOT NULL
) ON [PRIMARY]
END


	DECLARE @lastruntime DATETIME;
     SET @lastruntime = ISNULL(
                              (
                                  SELECT MAX(RunTime)
                                  FROM tblFileStats
                              ), GETDATE());
     IF OBJECT_ID('tempdb..#io') IS NOT NULL
         DROP TABLE #io;
     SELECT *
     INTO #io
     FROM sys.dm_io_virtual_file_stats(NULL, NULL);
     DECLARE @delaytime CHAR(8);
     SET @delaytime = CONVERT(CHAR(8), DATEADD(SECOND, @waitfordelaysec, 0), 108);
     WAITFOR DELAY @delaytime;
     INSERT INTO tblFileStats
            SELECT @@SERVERNAME,DB_NAME(a.database_id) DBName,
                   a.file_id FileID,
                   a.num_of_reads - b.num_of_reads AS NumOfReads,
                   a.num_of_writes - b.num_of_writes AS NumOfWrites,
                   a.io_stall_read_ms - b.io_stall_read_ms IOStallReadMS,
                   a.io_stall_write_ms - b.io_stall_write_ms IOStallWriteMS,
                   a.num_of_bytes_read - b.num_of_bytes_read NumOfBytesRead,
                   a.num_of_bytes_written - b.num_of_bytes_written NumOfBytesWritten,
                   a.io_stall - b.io_stall IOStall,
                   a.size_on_disk_bytes - b.size_on_disk_bytes SizeOnDiskBytes,
                   CASE
                       WHEN a.num_of_reads - b.num_of_reads > 0
                       THEN(a.io_stall_read_ms - b.io_stall_read_ms) / (a.num_of_reads - b.num_of_reads)
                       ELSE 0
                   END AS ReadLatency,
                   CASE
                       WHEN a.num_of_writes - b.num_of_writes > 0
                       THEN(a.io_stall_write_ms - b.io_stall_write_ms) / (a.num_of_writes - b.num_of_writes)
                       ELSE 0
                   END AS WriteLatency,
                   c.type_desc FileType,
                   c.physical_name FileLocation,
                   GETUTCDATE() RunTime
            FROM #io b
                 INNER JOIN sys.dm_io_virtual_file_stats(NULL, NULL) a ON a.database_id = b.database_id
                                                                          AND a.file_id = b.file_id
                 INNER JOIN sys.master_files c ON a.database_id = c.database_id
                                                  AND a.file_id = c.file_id
            ORDER BY DB_NAME(a.database_id);
     DELETE FROM tblFileStats
     WHERE NumOfBytesRead = 0
           AND NumOfBytesWritten = 0
           AND RunTime > @lastruntime;
     DELETE FROM tblFileStats
     WHERE RunTime < DATEADD(DD, -@retentiondays, GETUTCDATE());
GO

