USE distribution 
GO 
SELECT DISTINCT  
srv.srvname publication_server  
, a.publisher_db 
, p.immediate_sync 
, p.publication publication_name 
, CASE CAST(P.retention AS NVARCHAR(30)) 
         WHEN 0 THEN 'Never expire' 
         ELSE CAST(P.retention AS nvarchar(30))  END AS 'retention(hour)' 
, a.article 
, CASE p.publication_type 
         WHEN 0 THEN 'Transactional' 
         WHEN 1 THEN 'Snapshot' 
         WHEN 2 THEN 'Merge' END AS publication_type 
, a.destination_object 
, ss.srvname subscription_server 
, s.subscriber_db 
, CASE s.subscription_type 
         WHEN 0 THEN 'Push' 
         WHEN 1 THEN 'Pull' END AS subscription_type 
, CASE s.status 
         WHEN 0 THEN 'Inactive' 
         WHEN 1 THEN 'Subscribed' 
         WHEN 2 THEN 'Active' END AS status 
--, da.name AS distribution_agent_job_name 
FROM MSpublications p 
JOIN MSarticles a ON a.publication_id = p.publication_id 
JOIN MSsubscriptions s ON p.publication_id = s.publication_id 
JOIN master..sysservers ss ON s.subscriber_id = ss.srvid 
JOIN master..sysservers srv ON srv.srvid = p.publisher_id 
JOIN MSdistribution_agents da ON da.publisher_id = p.publisher_id  
     AND da.subscriber_id = s.subscriber_id 
ORDER BY 1,2,3
