SELECT I.NTUserName,
       I.loginname,
       I.HostName,
       I.ApplicationName,
       I.SessionLoginName,
       I.databasename,
       MIN(I.StartTime) AS first_used,
       MAX(I.StartTime) AS last_used,
       S.principal_id,
       S.sid,
       S.type_desc,
       S.name
FROM sys.traces T
     CROSS APPLY ::fn_trace_gettable
(CASE
     WHEN CHARINDEX('_', T.[path]) <> 0
     THEN SUBSTRING(T.PATH, 1, CHARINDEX('_', T.[path])-1)+'.trc'
     ELSE T.[path]
 END, T.max_files
) I
     LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.loginsid) = S.sid
WHERE T.id = 1
      AND I.LoginSid IS NOT NULL
GROUP BY I.NTUserName,
         I.loginname,
         I.SessionLoginName,
         I.databasename,
         S.principal_id,
         S.sid,
         S.type_desc,
         S.name,
         I.HostName,
         I.ApplicationName
ORDER BY I.NTUserName;
