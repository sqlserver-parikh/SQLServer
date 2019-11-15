----===============================================================================================
----Start: Database Mail Setup 
----===============================================================================================
USE [master]
DECLARE @emailaddress varchar(30)
DECLARE @smtpservername varchar(128)
DECLARE @operatoremailaddress varchar(1000)
set @emailaddress = 'DBMail@company.com' --Email will come from this email address, change it as appropriate, i keep it DBPMail@domain.com so I can easily filter DBMail alerts.
set @smtpservername = 'mail.company.com'  -- Change smtp server name
set @operatoremailaddress = 'DBA-SQLServer@company.com' -- Change This to SQL DBA Team Distribution List.

--Crate new Database Mail Profile
IF EXISTS (SELECT name FROM msdb..sysmail_profile WHERE name = N'Default Profile')
EXECUTE msdb.dbo.sysmail_delete_profile_sp
@profile_name = N'Default Profile' 

EXECUTE msdb.dbo.sysmail_add_profile_sp
@profile_name = 'Default Profile',
@description = 'Profile for sending Automated DBA Notifications'


--Set the New Profile as the Default
EXECUTE msdb.dbo.sysmail_add_principalprofile_sp
@profile_name = 'Default Profile',
@principal_name = 'public',
@is_default = 1 ;

--Create an Account for the Notifications
IF EXISTS (SELECT name FROM msdb..sysmail_account WHERE name = N'SQL Alerts')
EXECUTE msdb.dbo.sysmail_delete_account_sp
@account_name = 'SQL Alerts' 

EXECUTE msdb.dbo.sysmail_add_account_sp
@account_name = 'SQL Alerts',
@description = 'SQL Alerts',
@email_address = @emailaddress, -- Change This
@display_name = 'SQLDatabase-eMail',
@mailserver_name = @smtpservername, -- Change This to Exchange Name
@port = 25, --Use Port 25 for Exchange
--,@username = @username, --Please comment this out for Exchange
--@password = @password, --Please comment this out for Exchange
@enable_ssl = 0 --This should be 0 for Exchange

-- Add the Account to the Profile
EXECUTE msdb.dbo.sysmail_add_profileaccount_sp
@profile_name = 'Default Profile',
@account_name = 'SQL Alerts',
@sequence_number = 1

----===============================================================================================
----Complete: Database Mail Setup
----===============================================================================================


----===============================================================================================
----Start: Modify SQL Agent Property to send email
----===============================================================================================
USE [msdb]

EXEC msdb.dbo.sp_set_sqlagent_properties @email_save_in_sent_folder=1, 
@alert_replace_runtime_tokens=1
EXEC master.dbo.xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', N'UseDatabaseMail', N'REG_DWORD', 1

EXEC master.dbo.xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', N'DatabaseMailProfile', N'REG_SZ', N'Default Profile'
----===============================================================================================
---- Complete: Modify SQL Agent Property to send email
----===============================================================================================


----===============================================================================================
----Start: Add New Mail Operator SQLDBATeam
----===============================================================================================
USE [msdb]

/****** Object: Operator [SQLDBATeam] Script Date: 08/29/2010 10:47:10 ******/
IF EXISTS (SELECT name FROM msdb.dbo.sysoperators WHERE name = N'SQLDBATeam')
EXEC msdb.dbo.sp_delete_operator @name=N'SQLDBATeam'

USE [msdb]

/****** Object: Operator [SQLDBATeam] Script Date: 08/29/2010 10:47:10 ******/
EXEC msdb.dbo.sp_add_operator @name=N'SQLDBATeam',
@enabled=1, 
@weekday_pager_start_time=90000, 
@weekday_pager_end_time=180000, 
@saturday_pager_start_time=90000, 
@saturday_pager_end_time=180000, 
@sunday_pager_start_time=90000, 
@sunday_pager_end_time=180000, 
@pager_days=0, 
@email_address=@operatoremailaddress, 
@category_name=N'[Uncategorized]'
GO
----===============================================================================================
----Complete: Adding New Mail Operator SQLDBATeam
----===============================================================================================


----===============================================================================================
----Start: Add Alerts from Severity 16 to Severity 25
----===============================================================================================
USE [msdb]
GO

-- Set here the Operator name to receive notifications
DECLARE @customoper sysname
SET @customoper = 'SQLDBATeam'

IF EXISTS (SELECT name FROM msdb.dbo.sysoperators WHERE name = @customoper)
BEGIN
	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 10' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 10'
	END

	----------------------------------------
		
	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 825')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 825'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 825)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 825', 
			@message_id=825, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 825', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 833')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 833'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 833)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 833', 
			@message_id=833, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 833', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 855')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 855'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 855)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 855', 
			@message_id=855, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 855', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 856')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 856'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 856)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 856', 
			@message_id=856, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 856', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 3452')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 3452'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 3452)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 3452', 
			@message_id=3452, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 3452', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 3619')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 3619'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 3619)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 3619', 
			@message_id=3619, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 3619', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17179')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17179'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17179)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17179', 
			@message_id=17179, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17179', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17883')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17883'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17883)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17883', 
			@message_id=17883, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17883', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17884')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17884'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17884)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17884', 
			@message_id=17884, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17884', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17887')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17887'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17887)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17887', 
			@message_id=17887, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17887', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17888')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17888'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17888)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17888', 
			@message_id=17888, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17888', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17890')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17890'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17890)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17890', 
			@message_id=17890, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17890', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 28036')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 28036'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 28036)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 28036', 
			@message_id=28036, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 10'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 28036', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 16' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 16'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 2508')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 2508'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 2508)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 2508', 
			@message_id=2508, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 2508', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 2511')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 2511'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 2511)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 2511', 
			@message_id=2511, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 2511', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 3271')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 3271'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 3271)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 3271', 
			@message_id=3271, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 3271', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5228')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5228'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5228)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5228', 
			@message_id=5228, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5228', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5229')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5229'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5229)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5229', 
			@message_id=5229, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5229', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5242')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5242'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5242)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5242', 
			@message_id=5242, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5242', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5243')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5243'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5243)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5243', 
			@message_id=5243, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5243', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5250')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5250'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5250)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5250', 
			@message_id=5250, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5250', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5901')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5901'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5901)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5901', 
			@message_id=5901, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5901', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17130')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17130'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17130)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17130', 
			@message_id=17130, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17130', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 17300')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 17300'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 17300)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 17300', 
			@message_id=17300, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 16'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 17300', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 17' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 17'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 802')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 802'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 802)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 802', 
			@message_id=802, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 802', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 845')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 845'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 845)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 845', 
			@message_id=845, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 845', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 1101')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 1101'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 1101)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 1101', 
			@message_id=1101, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 1101', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 1105')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 1105'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 1105)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 1105', 
			@message_id=1105, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 1105', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 1121')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 1121'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 1121)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 1121', 
			@message_id=1121, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 1121', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 1214')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 1214'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 1214)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 1214', 
			@message_id=1214, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 1214', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 9002')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 9002'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 9002)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 9002', 
			@message_id=9002, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 17'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 9002', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 19' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 19'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 701')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 701'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 701)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 701', 
			@message_id=701, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 19'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 701', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 20' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 20'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 3624')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 3624'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 3624)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 3624', 
			@message_id=3624, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 20'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 3624', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 21' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 21'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 605')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 605'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 605)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 605', 
			@message_id=605, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 21'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 605', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 22' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 22'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5180')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5180'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5180)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5180', 
			@message_id=5180, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 22'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5180', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 8966')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 8966'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 8966)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 8966', 
			@message_id=8966, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 22'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 8966', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 23' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 23'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 5572')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 5572'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 5572)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 5572', 
			@message_id=5572, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 23'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 5572', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 9100')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 9100'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 9100)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 9100', 
			@message_id=9100, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 23'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 9100', @operator_name=@customoper, @notification_method = 1
	END

	----------------------------------------

	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Agent Alerts Sev 24' AND category_class=2)
	BEGIN
	EXEC msdb.dbo.sp_add_category @class=N'ALERT', @type=N'NONE', @name=N'Agent Alerts Sev 24'
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 823')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 823'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 823)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 823', 
			@message_id=823, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 24'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 823', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 824')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 824'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 824)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 824', 
			@message_id=824, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 24'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 824', @operator_name=@customoper, @notification_method = 1
	END

	IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error 832')
	EXEC msdb.dbo.sp_delete_alert @name=N'Error 832'

	IF EXISTS (SELECT message_id FROM msdb.sys.messages WHERE message_id = 832)
	BEGIN
		EXEC msdb.dbo.sp_add_alert @name=N'Error 832', 
			@message_id=832, 
			@severity=0, 
			@enabled=1, 
			@delay_between_responses=0, 
			@include_event_description_in=1, 
			@category_name=N'Agent Alerts Sev 24'
		EXEC msdb.dbo.sp_add_notification @alert_name=N'Error 832', @operator_name=@customoper, @notification_method = 1
	END
	PRINT 'Agent alerts created';
END
ELSE
BEGIN
	PRINT 'Operator does not exist. Alerts were not created.';
END
GO

GO
----===============================================================================================
----Complete: Adding Alerts from Severity 16 to Severity 25
----===============================================================================================


----===============================================================================================
----Start: Restart SQL Server Agent
----Attention: Below Command will Restart SQL Server Agent 
----===============================================================================================
/*
WAITFOR DELAY '00:00:05'
GO
EXEC master.dbo.xp_servicecontrol 'STOP', 'SQLServerAgent'
GO
WAITFOR DELAY '00:00:05'
GO
EXEC master.dbo.xp_servicecontrol 'START', 'SQLServerAgent'
GO
WAITFOR DELAY '00:00:05'
GO
*/
----===============================================================================================
----Complete: Stop and Start SQL Server Agent
----===============================================================================================

----===============================================================================================
----Start: Enable Database Mail Advanced option
----===============================================================================================
IF EXISTS (SELECT * FROM sys.configurations WHERE name = 'Database Mail XPs' and value_in_use = 0)
BEGIN 
IF EXISTS (SELECT * FROM sys.configurations WHERE name = 'show advanced options' and value_in_use = 0)
BEGIN
EXEC sp_configure 'show advanced options',1
RECONFIGURE WITH OVERRIDE
END
EXEC sp_configure 'Database Mail XPs',1
RECONFIGURE WITH OVERRIDE
EXEC sp_configure 'show advanced options',0
RECONFIGURE WITH OVERRIDE
END
----===============================================================================================
----End: Enable Database Mail Advanced option
----===============================================================================================


----===============================================================================================
---- Start: Test Database Mail Configuration
----===============================================================================================
/*
EXECUTE msdb.dbo.sp_send_dbmail
@recipients = 'DBATEAM@domain.com', -- Change This
@Subject = 'Test Message generated from SQL Server DatabaseMail',
@Body = 'This is a test message from SQL Server DatabaseMail'
*/
----===============================================================================================
---- Complete: test Database Mail Configuration
----===============================================================================================


----===============================================================================================
----Start: Job Failed Alert Stored Procedure
----===============================================================================================
USE master;
GO
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID('[dbo].[spJobFailedAlert]')
          AND type IN('p', 'pc')
)
    DROP PROCEDURE dbo.spJobFailedAlert;
GO
CREATE PROCEDURE dbo.spJobFailedAlert @job_id UNIQUEIDENTIFIER
WITH ENCRYPTION
AS
     SET NOCOUNT ON;
     DECLARE @today DATETIME, @crlf VARCHAR(10), @stat_failed TINYINT, @stat_succeeded TINYINT, @stat_retry TINYINT, @stat_canceled TINYINT, @stat_in_progress TINYINT, @email_to NVARCHAR(100), @subject VARCHAR(200), @body VARCHAR(8000), @job_name SYSNAME, @step_name SYSNAME, @err_severity INT, @run_datetime DATETIME, @rundurationsec VARCHAR(10), @command VARCHAR(3200), @errmessage VARCHAR(2048);
     SET @body = '';
     SET @crlf = CHAR(10);
     SET @stat_failed = 0;
     SET @stat_succeeded = 1;
     SET @stat_retry = 2;
     SET @stat_canceled = 3;
     SET @stat_in_progress = 4;
     SET @today = GETDATE();
     SELECT @email_to = email_address
     FROM msdb..sysoperators
     WHERE NAME LIKE 'SQLDBATeam';
     SELECT TOP 1 @job_name = sj.name,
                  @step_name = CONVERT(VARCHAR(2), sjh.step_id)+'.'+sjh.step_name,
                  @rundurationsec = run_duration / 10000 * 3600 + run_duration / 100 % 100 * 69 + run_duration % 100,
                  @run_datetime = CONVERT(VARCHAR, sjh.run_date)+' '+STUFF(STUFF(RIGHT('000000'+CONVERT(VARCHAR, sjh.run_time), 6), 5, 0, ':'), 3, 0, ':'),
                  @command = sjs.command,
                  @errmessage = sjh.message
     FROM msdb.dbo.sysjobs sj
          INNER JOIN msdb..sysjobhistory sjh ON sj.job_id = sjh.job_id
          INNER JOIN msdb..sysjobsteps sjs ON sj.job_id = sjs.job_id
     WHERE sj.job_id = @job_id
           AND sjh.step_id = 0 --exclude the job outcome step
           AND sjh.run_status IN(@stat_failed) --filter for only failed status
     ORDER BY sjh.run_date DESC,
              sjh.run_time DESC;
     SELECT TOP 1 @step_name = @step_name+' & '+CONVERT(VARCHAR(2), sjh.step_id)+'.'+sjh.step_name+@crlf,
                  @errmessage = @errmessage + @crlf + @crlf + sjh.message + @crlf
     FROM msdb.dbo.sysjobs sj
          INNER JOIN msdb..sysjobhistory sjh ON sj.job_id = sjh.job_id
          INNER JOIN msdb..sysjobsteps sjs ON sj.job_id = sjs.job_id
     WHERE sj.job_id = @job_id
           AND sjh.step_id <> 0 --exclude the job outcome step
           AND sjh.run_status IN(@stat_failed) --filter for only failed status
     ORDER BY sjh.run_date DESC,
              sjh.run_time DESC;

-- build the email body
     SET @body = @body+'Step Name= '+@step_name+@crlf+'Run Date = '+CONVERT(VARCHAR(50), @run_datetime)+@crlf+'Run Duration = '+CONVERT(VARCHAR(50), isnull(@rundurationsec, ''))+@crlf;
     IF(@err_severity <> 0)
         SET @body = @body+'Severity = '+CONVERT(VARCHAR(10), @err_severity)+@crlf;
     SET @body = @body+'Error = '+isnull(@errmessage, '')+@crlf+@crlf+'Command = '+isnull(@command, '')+@crlf; 

-- send the email
     IF(RTRIM(@body) <> '')
         BEGIN
             SET @subject = @job_name+' failed on \\'+@@servername;
             SET @body = -- 'server= ' + @@servername + @crlf +
             'Job Name = '+@job_name+@crlf+'--------------------------------------'+@crlf+@body;

-- print 'message length = ' + convert(varchar(20),len(@body))
             PRINT @body;
             EXEC msdb.dbo.sp_send_dbmail
                  @recipients = @email_to,
                  @subject = @subject,
                  @body = @body; --sql2005+
     END;
GO
----===============================================================================================
----End: Job Failed Alert Stored Procedure
----===============================================================================================


----===============================================================================================
----Start: Job Alert Trigger
----===============================================================================================
USE [msdb];
GO
IF EXISTS
(
    SELECT *
    FROM sys.triggers
    WHERE object_id = OBJECT_ID(N'[dbo].[trJobHistory]')
)
    DROP TRIGGER [dbo].[trJobHistory];
GO
USE [msdb];
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE TRIGGER [dbo].[trJobHistory] ON [dbo].[sysjobhistory]
AFTER INSERT
AS
     BEGIN
         SET NOCOUNT ON;
         DECLARE @job_id VARCHAR(100);
         IF EXISTS
         (
             SELECT 1
             FROM inserted
             WHERE run_status IN(0, 2, 3)
         )
             BEGIN
                 SELECT @job_id = job_id
                 FROM inserted
                 WHERE run_status IN(0, 2, 3)
                 AND step_id = 0;
                 EXEC master..spJobFailedAlert
                      @job_id;
         END;
     END;
GO
--===============================================================================================
--End: Job Alert Trigger
--===============================================================================================

----===============================================================================================
----Start: Remove Alerts
----===============================================================================================
--USE [msdb]
--GO

--/****** Object: Alert [Severity 16] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 16')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 16'
--GO

--/****** Object: Alert [Severity 17] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 17')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 17'
--GO

--/****** Object: Alert [Severity 18] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 18')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 18'
--GO

--/****** Object: Alert [Severity 19] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 19')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 19'
--GO

--/****** Object: Alert [Severity 20] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 20')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 20'
--GO

--/****** Object: Alert [Severity 21] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 21')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 21'
--GO

--/****** Object: Alert [Severity 22] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 22')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 22'
--GO

--/****** Object: Alert [Severity 23] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 23')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 23'
--GO

--/****** Object: Alert [Severity 24] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 24')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 24'
--GO

--/****** Object: Alert [Severity 25] Script Date: 08/29/2010 10:44:53 ******/
--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Severity 25')
--EXEC msdb.dbo.sp_delete_alert @name=N'Severity 25'
--GO

--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error Number 823')
--EXEC msdb.dbo.sp_delete_alert @name=N'Error Number 823'
--GO

--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error Number 824')
--EXEC msdb.dbo.sp_delete_alert @name=N'Error Number 824'
--GO

--IF EXISTS (SELECT name FROM msdb.dbo.sysalerts WHERE name = N'Error Number 825')
--EXEC msdb.dbo.sp_delete_alert @name=N'Error Number 825'
--GO

----===============================================================================================
----Complete: Remove Alerts
----===============================================================================================


----===============================================================================================
----Start: Remove Operator
----===============================================================================================
--USE [msdb]
--GO
--IF EXISTS (SELECT name FROM msdb.dbo.sysoperators WHERE name = N'SQLDBATeam')
--EXEC msdb.dbo.sp_delete_operator @name=N'SQLDBATeam'
--GO
----===============================================================================================
----Complete: Remove Operator
----===============================================================================================


----===============================================================================================
----Start: Remove Database Mail Profile
----===============================================================================================
--IF EXISTS (SELECT name FROM msdb..sysmail_profile WHERE name = N'Default Profile')
--EXECUTE msdb.dbo.sysmail_delete_profile_sp
--@profile_name = N'Default Profile' 

----Create an Account for the Notifications
--IF EXISTS (SELECT name FROM msdb..sysmail_account WHERE name = N'SQL Alerts')
--EXECUTE msdb.dbo.sysmail_delete_account_sp
--@account_name = 'SQL Alerts' 

----===============================================================================================
----Complete: Remove Database Mail Profile
----===============================================================================================

----===============================================================================================
----Start: Remove Job Failed Alert Stored Procedure
----===============================================================================================
--use master
--go
--if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spJobFailedAlert]') and type in (N'p', N'pc'))
--drop procedure dbo.spJobFailedAlert
----===============================================================================================
----End: Remove Job Failed Alert Stored Procedure
----===============================================================================================


----===============================================================================================
----Start: Remove Job Alert Trigger
----===============================================================================================
--USE [msdb]
--GO
--IF EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[trJobHistory]'))
--DROP TRIGGER [dbo].[trJobHistory]
----===============================================================================================
----End: Remove Job Alert Trigger
----===============================================================================================

USE [DBATasks]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spBlockingAlert]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[spBlockingAlert]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[spBlockingAlert]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[spBlockingAlert] AS' 
END
GO

ALTER PROCEDURE [dbo].[spBlockingAlert]
(@timecheckmin          INT          = 15,
 @emailDL               VARCHAR(MAX),
 @blocked_session_count INT          = 2
)
AS
     BEGIN
	   SELECT @emailDL = email_address FROM msdb..sysoperators
	   WHERE name LIKE 'SQLDBATeam'
         DECLARE @emailProfile VARCHAR(128);
         SELECT @emailProfile = name
         FROM msdb..sysmail_profile
         WHERE profile_id = 1;
         IF EXISTS
(
    SELECT 1
    FROM dbo.tblWhoIsActive WITH (nolock)
    WHERE 1 = 1
      --AND database_name LIKE 'DBName'
	 --AND CAST(sql_text AS VARCHAR(MAX)) LIKE '%QueryText%'
      --AND CAST(sql_command AS VARCHAR(MAX)) LIKE '%QueryText%'
          --AND (blocking_session_id IS NOT NULL
          --     OR blocked_session_count > 0)
          AND blocked_session_count > @blocked_session_count
	 --AND host_name LIKE 'ServerName'
	 --AND percent_complete > 20
	 --AND login_name LIKE 'LoginName'
	 --AND program_name LIKE '%ProgramName'
	 --and query_plan is not null
	 --and session_id = 55
          AND collection_time > DATEADD(mi, -@timecheckmin, GETDATE())
	 --AND collection_time > '2018-10-19 07:12:01.110'
	 --AND collection_time < '2018-10-19 07:29:01.110'
)
--ORDER BY tempdb_current DESC
             BEGIN
                 PRINT 'YES';
                 DECLARE @tableHTML VARCHAR(MAX);
                 SET @tableHTML = N'<table border="1">'+N'<tr>
<th>CollectTime</th>
<th>dd hh:mm:ss.mss</th>
<th>SQLCommand</th>
<th>SessionID</th>
<th>LoginName</th>
<th>BlockedBy</th>
<th>TotalBlocked</th>
<th>HostName</th>
<th>DBName</th>
<th>ProgramName</th>
<th>StartTime</th>
</tr>'+CAST(
(
    SELECT td = collection_time,
           '',
           td = [dd hh:mm:ss.mss],
           '',
           td = CASE
                    WHEN LEFT(CAST(sql_command AS VARCHAR(MAX)), 50) IS NULL
                    THEN ''
                    ELSE LEFT(CAST(sql_command AS VARCHAR(MAX)), 50)
                END,
           '',
           td = session_id,
           '',
           td = login_name,
           '',
           td = CASE
                    WHEN [blocking_session_id] IS NULL
                    THEN 'LeadBlocker'
                    ELSE CONVERT(VARCHAR(4), blocking_session_id)
                END,
           '',
           td = blocked_session_count,
           '',
           td = host_name,
           '',
           td = database_name,
           '',
           td = program_name,
           '',
           td = start_time
    FROM dbo.tblWhoIsActive WITH (nolock)
    WHERE 1 = 1
      --AND database_name LIKE 'DBName'
	 --AND CAST(sql_text AS VARCHAR(MAX)) LIKE '%QueryText%'
      --AND CAST(sql_command AS VARCHAR(MAX)) LIKE '%QueryText%'
          AND (blocking_session_id IS NOT NULL
               OR blocked_session_count > 0)
	 --AND host_name LIKE 'ServerName'
	 --AND percent_complete > 20
	 --AND login_name LIKE 'LoginName'
	 --AND program_name LIKE '%ProgramName'
	 --and query_plan is not null
	 --and session_id = 55
          AND collection_time > DATEADD(mi, -@timecheckmin, GETDATE())
	 --AND collection_time > '2018-10-19 07:12:01.110'
	 --AND collection_time < '2018-10-19 07:29:01.110'
    ORDER BY 1 DESC,
             blocked_session_count DESC,
             2 DESC FOR XML PATH('tr'), TYPE
) AS NVARCHAR(MAX))+N'</table>';
                 DECLARE @subject VARCHAR(228)= 'Heavy Blocking ON  '+CONVERT(VARCHAR(128), @@SERVERNAME);
                 EXEC msdb.dbo.sp_send_dbmail
                      @recipients = @emailDL,
                      @profile_name = @emailProfile,
                      @subject = @subject,
                      @body = @tableHTML,
                      @body_format = 'HTML';
             END;
     END;
GO



USE [msdb]
GO

IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA - Alert Blocking Detail')
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA - Alert Blocking Detail', @delete_unused_schedule=1
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
select @jobId = job_id from msdb.dbo.sysjobs where (name = N'DBA - Alert Blocking Detail')
if (@jobId is NULL)
BEGIN
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Alert Blocking Detail', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobsteps WHERE job_id = @jobId and step_id = 1)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Blocking Alert Detail', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [spBlockingAlert]', 
		@database_name=N'DBATasks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 15 min', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=15, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20181219, 
		@active_end_date=99991231, 
		@active_start_time=0, 
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

USE [master];
GO
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[sp_SQLStartupCheck]')
          AND type IN(N'P', N'PC')
)
    DROP PROCEDURE [dbo].[sp_SQLStartupCheck];
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
IF NOT EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[sp_SQLStartupCheck]')
          AND type IN(N'P', N'PC')
)
    BEGIN
        EXEC dbo.sp_executesql
             @statement = N'CREATE PROCEDURE [dbo].[sp_SQLStartupCheck] AS';
END;
GO
ALTER PROC [dbo].[sp_SQLStartupCheck]
AS
     SET NOCOUNT ON;
     IF EXISTS
     (
         SELECT 1
         FROM tempdb..sysobjects
         WHERE name = '##sqlservice'
     )
         DROP TABLE ##sqlservice;
     CREATE TABLE ##sqlservice(details VARCHAR(100));
     IF EXISTS
     (
         SELECT 1
         FROM tempdb..sysobjects
         WHERE name = '##agentservice'
     )
         DROP TABLE ##agentservice;
     CREATE TABLE ##agentservice(details VARCHAR(100));
     WAITFOR DELAY '00:00:05';
     DECLARE @sname VARCHAR(100), @starttime VARCHAR(30);
     DECLARE @authmode VARCHAR(25), @subject VARCHAR(250);
     DECLARE @insname VARCHAR(50), @agentname VARCHAR(50);
     DECLARE @sqlstatus VARCHAR(100), @agentstatus VARCHAR(100);
     DECLARE @dbstatus VARCHAR(100), @dbdetail VARCHAR(2000);
     DECLARE @sctsql VARCHAR(200), @HTML VARCHAR(8000);
     DECLARE @HTML1 VARCHAR(300);
     SELECT @sname = @@SERVERNAME;
     SELECT @authmode = CASE SERVERPROPERTY('IsIntegratedSecurityOnly')
                            WHEN 1
                            THEN 'Windows'
                            ELSE 'Mixed'
                        END;
     DECLARE @Query VARCHAR(200);
     SELECT @query = DATEADD(s, ((-1) * ([ms_ticks] / 1000)), GETDATE())
     FROM sys.[dm_os_sys_info];
     SELECT @starttime = CONVERT(VARCHAR(30), create_date, 109)
     FROM sys.databases
     WHERE database_id = 2;
     IF(SERVERPROPERTY('InstanceName')) IS NOT NULL
         BEGIN
             SET @insname = 'mssql$'+CONVERT(VARCHAR(40), SERVERPROPERTY('InstanceName'));
             SET @agentname = 'sqlagent$'+CONVERT(VARCHAR(40), SERVERPROPERTY('InstanceName'));
     END;
         ELSE
         BEGIN
             SET @insname = 'mssqlserver';
             SET @agentname = 'sqlserveragent';
     END;
     DECLARE @agent NVARCHAR(512);
     SELECT @agent = COALESCE(N'SQLAgent$'+CONVERT(SYSNAME, SERVERPROPERTY('InstanceName')), N'SQLServerAgent');
     INSERT INTO ##agentservice
     EXEC master.dbo.xp_servicecontrol
          'QueryState',
          @agent;
     DECLARE @sql NVARCHAR(512);
     SELECT @sql = COALESCE(N'SQLAgent$'+CONVERT(SYSNAME, SERVERPROPERTY('InstanceName')), N'SQLServer');
     INSERT INTO ##sqlservice
     EXEC master.dbo.xp_servicecontrol
          'QueryState',
          @sql;
     IF EXISTS
     (
         SELECT 1
         FROM ##sqlservice
         WHERE details LIKE '%RUNNING%'
     )
         SET @sqlstatus = 'Running';
         ELSE
     SET @sqlstatus = '<font color="red">Not Running</font>';
     IF EXISTS
     (
         SELECT 1
         FROM ##agentservice
         WHERE details LIKE '%RUNNING%'
     )
         SET @agentstatus = 'Running';
         ELSE
     SET @agentstatus = '<font color="red">Not Running</font>';
     IF EXISTS
     (
         SELECT 1
         FROM sys.databases
         WHERE state_desc <> 'ONLINE'
     )
         BEGIN
             SET @dbstatus = '<font color="red">Some of the database(s) are offline</font>';
             SELECT @dbdetail = '<table border="1"><tr><th>Database Name</th><th>Database Status</th></tr><tr>';
             SELECT @dbdetail = @dbdetail+'<td  align="Center">'+name+'</td><td  align="Center">'+state_desc+'</td></tr></table>'
             FROM sys.databases
             WHERE state_desc <> 'ONLINE';
     END;
         ELSE
         BEGIN
             SET @dbdetail = '';
             SET @dbstatus = 'All databases are online';
     END;
     DBCC TRACEON(4199, 2371, 3042, 3226, 1117, 1118, -1);
-------No Of Instancec-------

     IF EXISTS
     (
         SELECT *
         FROM tempdb..sysobjects
         WHERE id = OBJECT_ID(N'[tempdb]..[##Ninstances]')
     )
         DROP TABLE ##Ninstances;
     CREATE TABLE ##Ninstances
     (Value         NVARCHAR(100),
      InstanceNames NVARCHAR(100),
      Data          NVARCHAR(100)
     );
     INSERT INTO ##Ninstances
     EXECUTE xp_regread
             @rootkey = 'HKEY_LOCAL_MACHINE',
             @key = 'SOFTWARE\Microsoft\Microsoft SQL Server',
             @value_name = 'InstalledInstances';

 --------------No of Nodes-----------------------------
     IF EXISTS
     (
         SELECT *
         FROM tempdb..sysobjects
         WHERE id = OBJECT_ID(N'[tempdb]..[##table12]')
     )
         DROP TABLE ##table12;
     CREATE TABLE ##table12(NodeNames NVARCHAR(100));
     DECLARE @tab NVARCHAR(MAX)= 'IF(SERVERPROPERTY(''IsClustered'')=0)
BEGIN
PRINT ''1''
       SELECT CONVERT(nvarchar(Max),Serverproperty(''MachineName'')) As NodeNames 
END
ELSE 
BEGIN
PRINT''2''

       DECLARE @NameList VARCHAR(MAX) = '''';

       SELECT @NameList = @NameList + CAST(nodename AS VARCHAR(MAX)) + '', '' FROM sys.dm_os_cluster_nodes
       SELECT @NameList = LEFT(@NameList, LEN(@NameList) - 1);
       SELECT @NameList As NodeNames

END';
     INSERT INTO ##table12
     EXEC (@tab);

-------------table------------------
     IF EXISTS
     (
         SELECT *
         FROM tempdb..sysobjects
         WHERE id = OBJECT_ID(N'[tempdb]..[##Cinfo]')
     )
         DROP TABLE ##Cinfo;
     CREATE TABLE ##Cinfo
     (IsClustered   CHAR(10),
      ActiveNode    VARCHAR(MAX),
      NoofNodes     VARCHAR(MAX),
      NoofInstances VARCHAR(MAX)
     );
     DECLARE @Cinfo1 VARCHAR(MAX)= 'select  CASE WHEN SERVERPROPERTY(''IsClustered'')=0 THEN ''NO'' ELSE ''YES'' END As IsClustered,
[ActiveNode]= convert(varchar(100), ServerProperty(''ComputerNamePhysicalNetBIOS'')),
[NO of Nodes]= (select NodeNames from ##table12),
[NO of Instances]=(Select count(*) from ##Ninstances) ';
     INSERT INTO ##Cinfo
     EXEC (@cinfo1);
     DECLARE @tableHTML4 VARCHAR(MAX);
     SET @tableHTML4 = N'<table border="1">'+N'<tr><th>IsClustered</th>
<th>ActiveNode</th>
<th>NoOfNodes</th>
<th>NoOfInstances</th>
</tr>'+CAST(
           (
               SELECT td = IsClustered,
                      '',
                      td = ActiveNode,
                      '',
                      td = NoOfNodes,
                      '',
                      td = NoofInstances,
                      ''
               FROM ##Cinfo FOR XML PATH('tr'), TYPE
           ) AS NVARCHAR(MAX))+N'</table>';
     IF EXISTS
     (
         SELECT *
         FROM tempdb..sysobjects
         WHERE id = OBJECT_ID(N'[tempdb]..[##JOBFailure]')
     )
         DROP TABLE ##JOBFailure;
     CREATE TABLE ##JOBFailure
     (JobName       VARCHAR(MAX),
      LastRunDate   VARCHAR(MAX),
      LastRunStatus VARCHAR(20),
     );
     WAITFOR DELAY '00:00:10';
     DECLARE @Jobstat NVARCHAR(MAX)= ' SELECT DISTINCT SJ.Name AS JobName, 
CAST(CONVERT(DATETIME,CAST(sjh.run_date AS CHAR(8)),101) AS CHAR(11)) AS [LastRunDate],
CASE SJH.run_status 
WHEN 0 THEN ''Failed'' 
WHEN 1 THEN ''Successful''
WHEN 2 THEN ''Retry''
WHEN 3 THEN ''Cancelled''
WHEN 4 THEN ''In Progress''
END AS LastRunStatus 
FROM msdb..sysjobhistory SJH, msdb..sysjobs SJ
WHERE SJH.job_id = SJ.job_id and  sjh.run_status <> 1 and SJH.run_date = 
(SELECT MAX(SJH1.run_date) FROM msdb..sysjobhistory SJH1 WHERE SJH.job_id = SJH1.job_id ) 
ORDER BY LastRunStatus, LastRunDate desc';
     INSERT INTO ##JOBFailure
     EXEC (@jobstat);
     DECLARE @tableHTML3 VARCHAR(MAX);
     SET @tableHTML3 = N'<table border="1">'+N'<tr><th>JobName</th>
<th>LastRunDate</th>
<th>LastRunStatus</th>
</tr>'+CAST(
           (
               SELECT td = JobName,
                      '',
                      td = LastRunDate,
                      '',
                      td = LastRunStatus,
                      ''
               FROM ##JOBFailure FOR XML PATH('tr'), TYPE
           ) AS NVARCHAR(MAX))+N'</table>';
     IF EXISTS
     (
         SELECT *
         FROM tempdb..sysobjects
         WHERE id = OBJECT_ID(N'[tempdb]..[##Errorlogtab]')
     )
         DROP TABLE ##Errorlogtab;
     CREATE TABLE ##Errorlogtab
     (id          INT IDENTITY(1, 1),
      LogDate     DATETIME,
      ProcessInfo VARCHAR(20),
      ErrorText   NVARCHAR(MAX)
     );
     INSERT INTO ##Errorlogtab
     EXEC master.dbo.sp_readErrorLog
          1,
          1;
     DECLARE @tableHTML2 VARCHAR(MAX);
     SET @tableHTML2 = N'<table border="1">'+N'<tr><th>LogDate</th>
<th>ProcessInfo</th>
<th>ErrorText</th>
</tr>'+CAST(
           (
               SELECT TOP 10 td = LogDate,
                             '',
                             td = ProcessInfo,
                             '',
                             td = ErrorText,
                             ''
               FROM ##Errorlogtab
               ORDER BY id DESC FOR XML PATH('tr'), TYPE
           ) AS NVARCHAR(MAX))+N'</table>';
     CREATE TABLE #trace
     (Traceflag INT,
      Status    INT,
      Global    INT,
      Session   INT
     );
     DECLARE @sql2 VARCHAR(30)= 'DBCC TRACESTATUS';
     INSERT INTO #trace
     EXEC (@sql2);
     SELECT Traceflag,
            CASE Status
                WHEN 1
                THEN 'Enabled'
                WHEN 0
                THEN 'Disabled'
            END AS Status,
            CASE Global
                WHEN 1
                THEN 'Yes'
                WHEN 0
                THEN 'No'
            END AS Global
     INTO #trace2
     FROM #trace;
     DROP TABLE #trace;
     DECLARE @tableHTML VARCHAR(MAX);
     SET @tableHTML = N'<table border="1">'+N'<tr><th>Traceflag</th>
<th>Status</th>
<th>Global</th>
</tr>'+CAST(
           (
               SELECT td = Traceflag,
                      '',
                      td = Status,
                      '',
                      td = Global,
                      ''
               FROM #trace2 FOR XML PATH('tr'), TYPE
           ) AS NVARCHAR(MAX))+N'</table>';
     DECLARE @query1 VARCHAR(300);
     SELECT @query1 = @@VERSION;
     SET @subject = @sname+' : SQL Server is restarted. Please Check';
     SET @HTML = '<h3>'+@sname+'</h3>'+'<h4>'+@query1+'</h4><br>'+'<table border="1"><tr><th>SQL Server Startup time</th><th>OS Reboot Time</th><th>SQL Server Service</th><th>SQL Agent Service</th> <th>Database(s) Status</th><th>Authentication Mode</th></tr><tr><td align="Center">'+@starttime+'</td><td align ="Center">'+@query+'</td> <td align="Center">'+@sqlstatus+'</td><td align="Center">'+@agentstatus+'</td><td align="Center">'+@dbstatus+'</td><td align="Center">'+@authmode+'</td></tr></table><br><br>'+@dbdetail+'<br><h3>SQL Cluster Info</h3>'+@tableHTML4+'<br><h3>SQL Agents Job Info</h3>'+@tableHTML3+'<br><h3>SQL Server Error Log info before SQL restart</h3>'+@tableHTML2+'<br><h3>Trace Flag Info</h3>'+@tableHTML;
     DECLARE @emailDL VARCHAR(MAX);
     SELECT @emailDL = email_address
     FROM msdb..sysoperators
     WHERE name LIKE 'SQLDBATeam';
     DECLARE @emailProfile VARCHAR(128);
     SELECT @emailProfile = name
     FROM msdb..sysmail_profile
     WHERE profile_id = 1;
     EXEC msdb.dbo.sp_send_dbmail
          @profile_name = @emailProfile,
          @recipients = @emailDL,
          @from_address = @emailDL,
          @subject = @subject,
          @body = @HTML,
          @body_format = 'HTML';
GO
EXEC sp_procoption
     N'[dbo].[sp_SQLStartupCheck]',
     'startup',
     '1';
GO


