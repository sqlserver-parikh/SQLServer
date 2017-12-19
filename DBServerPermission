WITH cte
     AS (
     SELECT b.name LoginName,
            Isnull(c.name, 'Public') ServerRole,
            b.type_desc,
            b.create_date CreateDate,
            b.modify_date ModifyDate,
            b.is_disabled Disabled,
            --Audit.HostName,
            CASE
                WHEN d.is_policy_checked = 1
                THEN 'Yes'
                ELSE 'No'
            END PolicyChecked,
            CASE
                WHEN d.is_expiration_checked = 1
                THEN 'Yes'
                ELSE 'No'
            END ExpirationChecked
     FROM sys.server_role_members a
          RIGHT JOIN sys.server_principals b ON a.member_principal_id = b.principal_id
          LEFT JOIN sys.server_principals c ON a.role_principal_id = c.principal_id
          LEFT JOIN sys.sql_logins d ON b.name = d.name
     --     LEFT JOIN
     --(
     --    SELECT DISTINCT
     --           I.loginname,
     --           I.HostName
     --    FROM sys.traces T
     --         CROSS APPLY ::fn_trace_gettable
     --    (CASE
     --         WHEN CHARINDEX('_', T.[path]) <> 0
     --         THEN SUBSTRING(T.PATH, 1, CHARINDEX('_', T.[path])-1)+'.trc'
     --         ELSE T.[path]
     --     END, T.max_files
     --    ) I
     --         LEFT JOIN sys.server_principals S ON CONVERT(VARBINARY(MAX), I.loginsid) = S.sid
     --    WHERE T.id = 1
     --          AND I.LoginSid IS NOT NULL
     --          AND HostName IS NOT NULL
     --) Audit ON Audit.LoginName = b.name
     )
     SELECT DISTINCT
            @@SERVERNAME ServerName,
            LoginName,
            ServerRole = SUBSTRING(
                                  (
                                      SELECT(', '+ServerRole)
                                      FROM cte b
                                      WHERE a.LoginName = b.LoginName FOR XML PATH('')
                                  ), 3, 8000),
            --HostName = SUBSTRING(
            --                    (
            --                        SELECT(', '+HostName)
            --                        FROM cte b
            --                        WHERE a.LoginName = b.LoginName FOR XML PATH('')
            --                    ), 3, 8000),
            CreateDate,
            ModifyDate,
            type_desc,
            CONVERT(VARCHAR(3), DATEDIFF(dd, modifydate, GETDATE()))+' Days ago' AS PasswordChanged,
            Disabled,
            LOGINPROPERTY(loginname, 'DaysUntilExpiration') DaysUntilExpiration,
            LOGINPROPERTY(loginname, 'IsExpired') IsExpired,
            LOGINPROPERTY(loginname, 'IsMustChange') IsMustChange,
            PolicyChecked,
            ExpirationChecked
     FROM cte a
     WHERE type_desc NOT IN('SERVER_ROLE', 'CERTIFICATE_MAPPED_LOGIN')
     AND LoginName NOT LIKE '##%'
     AND LoginName NOT LIKE 'NT %';
