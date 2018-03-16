USE msdb;
SELECT collection_time,
       [dd hh:mm:ss.mss],
 --     ,Left(cast(sql_text as varchar(max)),50) SQLText 
       sql_text,
       sql_command,
     -- , Left(cast(sql_command as varchar(max)),50)SQLCommand 
       [session_id],
       [login_name],
       [wait_info],
       [tran_log_writes],
       [CPU],
       [tempdb_allocations],
       [tempdb_current],
       [blocking_session_id],
       [blocked_session_count],
       [reads],
       [writes],
       [physical_reads],
       [query_plan],
       [used_memory],
       [status],
       [tran_start_time],
       [open_tran_count],
       [percent_complete],
       [host_name],
       [database_name],
       [program_name],
       [start_time],
       [login_time]
FROM dbo.tblWhoIsActive WITH (nolock)
WHERE 1 = 1
       --AND database_name LIKE 'DBName'
       --AND CAST(sql_text AS VARCHAR(MAX)) LIKE '%QueryText%'
       --AND CAST(sql_command AS VARCHAR(MAX)) LIKE '%QueryText%'
       --AND (blocking_session_id IS NOT NULL OR blocked_session_count > 0)  --Just find blocking details
	--AND host_name LIKE 'ServerName'
	--AND percent_complete > 20
	--AND login_name LIKE 'LoginName'
	--AND program_name LIKE '%ProgramName'
	--and query_plan is not null
	--and session_id = 55
      AND collection_time > DATEADD(MINUTE, -15, GETDATE())
	--AND collection_time > '2018-03-16 11:14:01.090'
	--AND collection_time < '2018-03-16 11:14:01.090'
ORDER BY 1 DESC;
--ORDER BY tempdb_current DESC
