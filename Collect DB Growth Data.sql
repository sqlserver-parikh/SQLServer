USE DBATasks;
GO
IF OBJECT_ID(N'vwDBList') IS NOT NULL
DROP VIEW vwDBList
GO
CREATE VIEW vwDBList
AS
     SELECT sd.[name] AS 'DBName',
            SUBSTRING(SUSER_SNAME(sd.[owner_sid]), 1, 24) AS 'Owner'
     FROM master.sys.databases sd
            WHERE HAS_DBACCESS(sd.[name]) = 1
                  AND sd.[is_read_only] = 0
                  AND sd.[state_desc] = 'ONLINE'
                  AND sd.[user_access_desc] = 'MULTI_USER'
                  AND sd.[is_in_standby] = 0;
GO
IF OBJECT_ID(N'tblDBGrowthDetail', N'U') IS NULL
BEGIN   
CREATE TABLE [dbo].[tblDBGrowthDetail]
([DBID]          [INT] NOT NULL,
 [FileID]        [INT] NOT NULL,
 [FileType]      [INT] NOT NULL,
 [SizeMB]        [INT] NOT NULL,
 [UsedMB]        [INT] NULL,
 [DBNAME]        [NVARCHAR](200) NULL,
 [FileNAME]      [NVARCHAR](128) NULL,
 [FileGroupName] [SYSNAME] NULL,
 DataSpaceID     SMALLINT,
 SnapDate        [DATETIME2](7)
);
END
GO
IF OBJECT_ID(N'spDBGrowth') IS NOT NULL
DROP PROCEDURE dbo.spDBGrowth
GO
CREATE PROCEDURE dbo.spDBGrowth
(@retentiondays int = 730)
AS
SET NOCOUNT ON
     BEGIN
         INSERT INTO DBATasks..tblDBGrowthDetail
                SELECT mf.database_id,
                       mf.file_id,
                       mf.type,
                       --To convert size in MB
                       CEILING(mf.size * 1.0 / 128),
                       NULL,
                       DB_NAME(mf.database_id),
                       mf.name,
                       NULL,
                       NULL,
                       SYSDATETIME()
                FROM sys.master_files mf
                       ORDER BY mf.database_id,
                                mf.file_id;
         DECLARE @DBNAME NVARCHAR(255);
         DECLARE @DBID INT;
         DECLARE @SQL NVARCHAR(2000);
         DECLARE cDatabases CURSOR
         FOR SELECT UPPER(name),
                    database_id
             FROM sys.databases sd(NOLOCK)
                  INNER JOIN vwDBList vw ON sd.name = vw.DBName;
         OPEN cDatabases;
         FETCH NEXT FROM cDatabases INTO @DBNAME, @DBID;
         WHILE @@FETCH_STATUS = 0
             BEGIN
                 SET @SQL = N'use ['+@DBNAME+']
          UPDATE A
             SET  UsedMB = (ceiling(S.size/128) - ceiling(S.size/128.0 - CAST(FILEPROPERTY(S.name, ''SpaceUsed'') AS int )/128.0))
			 , FileGroupName = ISNULL(fg.name,''LogFile'')
			 , DataSpaceID = fg.data_space_id
			FROM DBATasks..tblDBGrowthDetail A (NOLOCK) 
			JOIN sys.database_files S ON
               A.DBID = '+CONVERT(NVARCHAR(10), @DBID)+'
               AND A.FileID = S.file_id LEFT JOIN sys.filegroups fg on fg.data_space_id = S.data_space_id;';
                 EXEC sp_executesql
                      @SQL,
                      N'@DBNAME nvarchar(128),  @DBID int',
                      @DBNAME,
                      @DBID;
                 FETCH NEXT FROM cDatabases INTO @DBNAME, @DBID;
             END;
         CLOSE cDatabases;
         DEALLOCATE cDatabases;
		 DELETE tblDBGrowthDetail
		 WHERE SnapDate < DATEADD(DD,-@retentiondays,GETDATE())
     END;
GO
USE [msdb]
GO

IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Collect DB Growth Data')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Collect DB Growth Data', @delete_unused_schedule=1
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Collect DB Growth Data')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Collect DB Growth Data', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'DBGrowth', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DBATasks..spDBGrowth', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20010820, 
		@active_end_date=99991231, 
		@active_start_time=223000, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

--ROLLBACK
--USE DBATasks
--DROP VIEW [dbo].[vwDBList]
--DROP TABLE [dbo].[tblDBGrowthDetail]
--DROP PROCEDURE dbo.spDBGrowth
--EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Collect DB Growth Data', @delete_unused_schedule=1
					  
USE DBATasks
GO
CREATE VIEW vwDBGrowthData
AS
SELECT PVT.DatabaseName 
      , PVT.[0], PVT.[-1], PVT.[-2], PVT.[-3],  PVT.[-4],  PVT.[-5],  PVT.[-6] 
               , PVT.[-7], PVT.[-8], PVT.[-9], PVT.[-10], PVT.[-11], PVT.[-12] 
FROM 
   (SELECT DGD.DBNAME AS DatabaseName 
          ,DATEDIFF(mm, GETDATE(), DGD.SnapDate) AS MonthsAgo 
          ,CONVERT(numeric(10, 1), AVG(DGD.UsedMB)) AS AvgSizeMB 
    FROM [DBATasks].[dbo].[tblDBGrowthDetail] as DGD 
    WHERE NOT DGD.DBNAME IN 
              ('master', 'msdb', 'model', 'tempdb') 
          AND DGD.FileGroupName NOT LIKE  'LogFile' 
          AND DGD.SnapDate BETWEEN DATEADD(yy, -1, GETDATE()) AND GETDATE() 
    GROUP BY DGD.DBNAME 
            ,DATEDIFF(mm, GETDATE(), DGD.SnapDate) 
    ) AS BCKSTAT 
PIVOT (SUM(BCKSTAT.AvgSizeMB) 
       FOR BCKSTAT.MonthsAgo IN ([0], [-1], [-2], [-3], [-4], [-5], [-6], [-7], [-8], [-9], [-10], [-11], [-12]) 
      ) AS PVT 

