$a = "<style>"
$a = $a + "TABLE{border-width: 1px;border-style: solid;border-color: black;border-collapse: collapse;}"
$a = $a + "TH{border-width: 1px;padding: 0px;border-style: solid;border-color: black;background-color:thistle}"
$a = $a + "TD{border-width: 1px;padding: 0px;border-style: solid;border-color: black;background-color:palegoldenrod}"
$a = $a + "</style>"
 
$instancepath= "C:\temp\SQLServerList.txt"
$outputfilefull = "C:\temp\JobFailures.htm"
  
$filePath = ""
  
$dt = new-object "System.Data.DataTable"
foreach ($instance in get-content $instancepath)
{
$instance
#$query = Get-Content "c:\temp\JobFailures.sql"
$cn = new-object System.Data.SqlClient.SqlConnection "server=$instance;database=msdb;Integrated Security=sspi"
$cn.Open()
$sql = $cn.CreateCommand()
$sql.CommandText = "Select
        @@SERVERNAME ServerName
        ,sj.name as [JobName]
,       suser_sname(sj.owner_sid ) OwnerName
,CASE   WHEN run_status = 0 THEN 'Failed'
                WHEN run_status = 1 THEN 'Success'
                WHEN run_status = 2 THEN 'Retry'
                WHEN run_status = 3 THEN 'Cancelled'
                WHEN run_status = 5 THEN 'In Progress'
        END Outcome
,       jh.message 
,       (Select count(*) from msdb..sysjobschedules js1 where js1.job_id = sj.job_id ) as [SchedulesCount]
,       (Select count(*) from msdb..sysjobsteps js2 where js2.job_id = sj.job_id ) as [StepsCount]
,       (Select count(*) from msdb..sysjobhistory jh1 where jh1.job_id=sj.job_id and jh1.step_id = 0 and datediff( HH, convert(datetime, convert( varchar, jh1.run_date) ), getdate()) < 24 ) as [ExecutionCount]
,       (Select avg(((run_duration/10000*3600) + ((run_duration%10000)/100*60) + (run_duration%100))+0.0) from msdb..sysjobhistory jh2 where jh2.job_id=sj.job_id and jh2.step_id = 0 and datediff( HH, convert(datetime, convert( varchar, jh2.run_date) ), getdate()) < 24  ) as [AverageRunDuration]
,       (Select avg(retries_attempted+0.0) from msdb..sysjobhistory jh2 where jh2.job_id=sj.job_id and jh2.step_id = 0 and datediff( HH, convert(datetime, convert( varchar, jh2.run_date) ), getdate()) < 24 ) as [AverageRetriesAttempted]
,       Count(*) as [FailureCount]
from msdb..sysjobhistory jh
inner join msdb..sysjobs sj on ( jh.job_id = sj.job_id )
where jh.step_id = 0 and datediff( HH, convert(datetime, convert( varchar, jh.run_date) ), getdate()) < 24 and jh.run_status = 0 
        and sj.name not like 'VZBScomTest%'
group by sj.job_id, sj.name  ,sj.owner_sid ,CASE    WHEN run_status = 0 THEN 'Failed'
                WHEN run_status = 1 THEN 'Success'
                WHEN run_status = 2 THEN 'Retry'
                WHEN run_status = 3 THEN 'Cancelled'
                WHEN run_status = 5 THEN 'In Progress'
        END , jh.message"
$rdr = $sql.ExecuteReader()
$dt.Load($rdr)
$cn.Close()
}
  
$dt | select * -ExcludeProperty RowError, RowState, HasErrors, Name, Table, ItemArray | ConvertTo-Html -head $a | Set-Content $outputfilefull 
 
$filepath = $outputfilefull  
 
$EmailFrom = "MyTeamDL@domain.com"
$emailto = "MyTeamDL@domain.com"
$Subject = "Job Failure report"
$Body = Get-Content ($filepath)
$counting = get-content $filepath | measure -Character -Line -Word
$Body = New-Object System.Net.Mail.MailMessage $Emailfrom, $emailto, $subject, $body
$Body.isBodyhtml = $true
$SMTPServer = "mailrelay.domain.com"
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer) 
if ($counting.words -gt 40)
{
$SMTPClient.Send($Body)
}
