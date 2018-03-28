--http://dba.stackexchange.com/questions/64567/how-to-clone-a-user-in-sql-server-2008-r2
--- To copy permissions of one user/role to another user/role.

USE DBATasks; -- Use the database from which you want to extract the permissions
GO
SET NOCOUNT ON;
DECLARE @OldUser SYSNAME, @NewUser SYSNAME;
SET @OldUser = 'olduser'; --The user or role from which to copy the permissions from
SET @NewUser = 'userNEW';  --The user or role to which to copy the permissions to

SELECT 'USE'+SPACE(1)+QUOTENAME(DB_NAME()) AS '--Database Context';
SELECT '--Cloning permissions from'+SPACE(1)+QUOTENAME(@OldUser)+SPACE(1)+'to'+SPACE(1)+QUOTENAME(@NewUser) AS '--Comment';
SELECT 'EXEC sp_addrolemember @rolename ='+SPACE(1)+QUOTENAME(USER_NAME(rm.role_principal_id), '''')+', @membername ='+SPACE(1)+QUOTENAME(@NewUser, '''') AS '--Role Memberships'
FROM sys.database_role_members AS rm
WHERE USER_NAME(rm.member_principal_id) = @OldUser
ORDER BY rm.role_principal_id ASC;
SELECT CASE
           WHEN perm.state <> 'W'
           THEN perm.state_desc
           ELSE 'GRANT'
       END+SPACE(1)+perm.permission_name+SPACE(1)+'ON '+QUOTENAME(USER_NAME(obj.schema_id))+'.'+QUOTENAME(obj.name)+CASE
                                                                                                                        WHEN cl.column_id IS NULL
                                                                                                                        THEN SPACE(0)
                                                                                                                        ELSE '('+QUOTENAME(cl.name)+')'
                                                                                                                    END+SPACE(1)+'TO'+SPACE(1)+QUOTENAME(@NewUser) COLLATE database_default+CASE
                                                                                                                                                                                                WHEN perm.state <> 'W'
                                                                                                                                                                                                THEN SPACE(0)
                                                                                                                                                                                                ELSE SPACE(1)+'WITH GRANT OPTION'
                                                                                                                                                                                            END AS '--Object Level Permissions'
FROM sys.database_permissions AS perm
     INNER JOIN sys.objects AS obj ON perm.major_id = obj.[object_id]
     INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
     LEFT JOIN sys.columns AS cl ON cl.column_id = perm.minor_id
                                    AND cl.[object_id] = perm.major_id
WHERE usr.name = @OldUser
ORDER BY perm.permission_name ASC,
         perm.state_desc ASC;
SELECT CASE
           WHEN perm.state <> 'W'
           THEN perm.state_desc
           ELSE 'GRANT'
       END+SPACE(1)+perm.permission_name+SPACE(1)+SPACE(1)+'TO'+SPACE(1)+QUOTENAME(@NewUser) COLLATE database_default+CASE
                                                                                                                          WHEN perm.state <> 'W'
                                                                                                                          THEN SPACE(0)
                                                                                                                          ELSE SPACE(1)+'WITH GRANT OPTION'
                                                                                                                      END AS '--Database Level Permissions'
FROM sys.database_permissions AS perm
     INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
WHERE usr.name = @OldUser
      AND perm.major_id = 0
ORDER BY perm.permission_name ASC,
         perm.state_desc ASC;
