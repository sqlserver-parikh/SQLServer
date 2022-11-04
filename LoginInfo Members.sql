create table #logininfo
(
    accountname varchar(256),
    type varchar(256),
    privilege varchar(256),
    mappedloginname varchar(256),
    permissionpath varchar(256)
)

declare @loginname varchar(128)
Declare DBs cursor for
SELECT name
from sys.server_principals
where type = 'g'

declare @command Nvarchar(2048);
OPEN dbs;
FETCH NEXT FROM dbs
INTO @loginname;
WHILE @@FETCH_STATUS = 0
BEGIN
    insert into #logininfo
    exec xp_logininfo @loginname, members
    FETCH NEXT FROM dbs
    INTO @loginname;
END
CLOSE dbs;
DEALLOCATE dbs;
insert into #logininfo
select name,
       type,
       '',
       '',
       ''
from sys.server_principals
where type in ( 'U', 'S' )
      and is_disabled = 0
select *
from #logininfo
drop table #logininfo
