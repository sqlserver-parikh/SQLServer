USE DBATasks;
SELECT DISTINCT
       MAX(collection_time) CollectionTime,
       LEFT(CAST(sql_text AS VARCHAR(MAX)), 100) SQLText,
       [session_id],
       MAX([tempdb_current]) TempDB,
       [host_name],
       [database_name],
       [program_name]
FROM dbo.tblWhoIsActive WITH (nolock)
WHERE 1 = 1
      --AND database_name LIKE 'arg_epw_pdb'
	 --AND CAST(sql_text AS VARCHAR(MAX)) LIKE '%QueryText%'
      --AND CAST(sql_command AS VARCHAR(MAX)) LIKE '%QueryText%'
      --AND (blocking_session_id IS NOT NULL OR blocked_session_count > 0)
	 --AND host_name LIKE 'ServerName'
	 --AND convert(decimal(10,2), percent_complete) > 20
	 --AND login_name LIKE 'LoginName'
	 --AND program_name LIKE '%ProgramName'
	 --and query_plan is not null
	 --and session_id = 55
      --AND CONVERT(INT, RTRIM(LTRIM((REPLACE(tempdb_current, ',', ''))))) > 10000
      AND collection_time > DATEADD(MI, -12000, GETDATE())
	 --AND collection_time > '2018-03-16 11:14:01.090'
	 --AND collection_time < '2018-03-16 11:14:01.090'
GROUP BY LEFT(CAST(sql_text AS VARCHAR(MAX)), 100),
         [session_id],
         [host_name],
         [database_name],
         [program_name]
ORDER BY MAX([tempdb_current]) DESC;
