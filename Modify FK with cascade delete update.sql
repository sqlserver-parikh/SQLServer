
DECLARE @pktable_name SYSNAME= 'mytableName', -- Wildcard pattern matching IS NOT supported.  
@pktable_owner SYSNAME= NULL, -- Wildcard pattern matching IS NOT supported.  
@pktable_qualifier SYSNAME= NULL, -- Wildcard pattern matching IS NOT supported.  
@fktable_name SYSNAME= NULL, -- Wildcard pattern matching IS NOT supported.  
@fktable_owner SYSNAME= NULL, -- Wildcard pattern matching IS NOT supported.  
@fktable_qualifier SYSNAME= NULL;     -- Wildcard pattern matching IS NOT supported.  
SET NOCOUNT ON;
DECLARE @pktable_id INT;
DECLARE @fktable_id INT;

-- select 'XXX starting parameter analysis'  
IF(@pktable_name IS NULL)
  AND (@fktable_name IS NULL)
    BEGIN   -- If neither primary key nor foreign key table names given  
        RAISERROR(15252, -1, -1);
        RETURN;
    END;
IF @fktable_qualifier IS NOT NULL
    BEGIN
        IF DB_NAME() <> @fktable_qualifier
            BEGIN   -- If qualifier doesn't match current database  
                RAISERROR(15250, -1, -1);
                RETURN;
            END;
    END;
IF @pktable_qualifier IS NOT NULL
    BEGIN
        IF DB_NAME() <> @pktable_qualifier
            BEGIN   -- If qualifier doesn't match current database  
                RAISERROR(15250, -1, -1);
                RETURN;
            END;
    END;
IF @pktable_owner = ''
    BEGIN   -- If empty owner name  
        SELECT @pktable_id = OBJECT_ID(QUOTENAME(@pktable_name));
    END;
    ELSE
    BEGIN
        SELECT @pktable_id = OBJECT_ID(ISNULL(QUOTENAME(@pktable_owner), '') + '.' + QUOTENAME(@pktable_name));
    END;
IF @fktable_owner = ''
    BEGIN   -- If empty owner name  
        SELECT @fktable_id = OBJECT_ID(QUOTENAME(@fktable_name));
    END;
    ELSE
    BEGIN
        SELECT @fktable_id = OBJECT_ID(ISNULL(QUOTENAME(@fktable_owner), '') + '.' + QUOTENAME(@fktable_name));
    END;
IF @fktable_name IS NOT NULL
    BEGIN
        IF @fktable_id IS NULL
            SELECT @fktable_id = 0;  -- fk table name is provided, but there is no such object  
    END;
IF @pktable_name IS NOT NULL
    BEGIN
        IF @pktable_id IS NULL
            SELECT @pktable_id = 0;  -- pk table name is provided, but there is no such object  
   
        SELECT 'ALTER TABLE ' + QUOTENAME(CONVERT(SYSNAME, o2.name)) + ' DROP CONSTRAINT ' + QUOTENAME(CONVERT(SYSNAME, OBJECT_NAME(f.object_id))) + ';' 'Drop FK', 
               'ALTER TABLE ' + QUOTENAME(CONVERT(SYSNAME, o2.name)) + ' WITH CHECK ADD CONSTRAINT ' + QUOTENAME(CONVERT(SYSNAME, OBJECT_NAME(f.object_id))) + ' FOREIGN KEY (' + QUOTENAME(CONVERT(SYSNAME, c2.name)) + ') REFERENCES [dbo].' + CONVERT(SYSNAME, o1.name) + '(' + CONVERT(SYSNAME, c1.name) + ') ON DELETE CASCADE ON UPDATE CASCADE;' 'CreatFK', 
               'ALTER TABLE ' + QUOTENAME(CONVERT(SYSNAME, o2.name)) + '  CHECK  CONSTRAINT ' + QUOTENAME(CONVERT(SYSNAME, OBJECT_NAME(f.object_id))) +';' 'CHECK Contraint' 
             ,  PKTABLE_QUALIFIER = CONVERT(SYSNAME, DB_NAME()) + ' Hi', 
               PKTABLE_OWNER = CONVERT(SYSNAME, SCHEMA_NAME(o1.schema_id)), 
               PKTABLE_NAME = CONVERT(SYSNAME, o1.name), 
               PKCOLUMN_NAME = CONVERT(SYSNAME, c1.name), 
               FKTABLE_QUALIFIER = CONVERT(SYSNAME, DB_NAME()), 
               FKTABLE_OWNER = CONVERT(SYSNAME, SCHEMA_NAME(o2.schema_id)), 
               FKTABLE_NAME = CONVERT(SYSNAME, o2.name), 
               FKCOLUMN_NAME = CONVERT(SYSNAME, c2.name),  
               -- Force the column to be non-nullable (see SQL BU 325751)  

               UPDATE_RULE = CONVERT(SMALLINT,
                                     CASE f.update_referential_action
                                         WHEN 1
                                         THEN 0
                                         WHEN 0
                                         THEN 1
                                         ELSE f.update_referential_action
                                     END), 
               DELETE_RULE = CONVERT(SMALLINT,
                                     CASE f.delete_referential_action
                                         WHEN 1
                                         THEN 0
                                         WHEN 0
                                         THEN 1
                                         ELSE f.delete_referential_action
                                     END), 
               FK_NAME = CONVERT(SYSNAME, OBJECT_NAME(f.object_id)), 
               PK_NAME = CONVERT(SYSNAME, i.name), 
               DEFERRABILITY = CONVERT(SMALLINT, 7)   -- SQL_NOT_DEFERRABLE  
        FROM sys.objects o1, 
             sys.objects o2, 
             sys.columns c1, 
             sys.columns c2, 
             sys.foreign_keys f
             INNER JOIN sys.foreign_key_columns k ON(k.constraint_object_id = f.object_id)
             INNER JOIN sys.indexes i ON(f.referenced_object_id = i.object_id
                                         AND f.key_index_id = i.index_id)
        WHERE o1.object_id = f.referenced_object_id
              AND (o1.object_id = @pktable_id)
              AND o2.object_id = f.parent_object_id
              AND (@fktable_id IS NULL
                   OR o2.object_id = @fktable_id)
              AND c1.object_id = f.referenced_object_id
              AND c2.object_id = f.parent_object_id
              AND c1.column_id = k.referenced_column_id
              AND c2.column_id = k.parent_column_id
    END;
    ELSE
    BEGIN
        SELECT PKTABLE_QUALIFIER = CONVERT(SYSNAME, DB_NAME()), 
               PKTABLE_OWNER = CONVERT(SYSNAME, SCHEMA_NAME(o1.schema_id)), 
               PKTABLE_NAME = CONVERT(SYSNAME, o1.name), 
               PKCOLUMN_NAME = CONVERT(SYSNAME, c1.name), 
               FKTABLE_QUALIFIER = CONVERT(SYSNAME, DB_NAME()), 
               FKTABLE_OWNER = CONVERT(SYSNAME, SCHEMA_NAME(o2.schema_id)), 
               FKTABLE_NAME = CONVERT(SYSNAME, o2.name), 
               FKCOLUMN_NAME = CONVERT(SYSNAME, c2.name),  
               -- Force the column to be non-nullable (see SQL BU 325751)  

               UPDATE_RULE = CONVERT(SMALLINT,
                                     CASE OBJECTPROPERTY(f.object_id, 'CnstIsUpdateCascade')
                                         WHEN 1
                                         THEN 0
                                         ELSE 1
                                     END), 
               DELETE_RULE = CONVERT(SMALLINT,
                                     CASE OBJECTPROPERTY(f.object_id, 'CnstIsDeleteCascade')
                                         WHEN 1
                                         THEN 0
                                         ELSE 1
                                     END), 
               FK_NAME = CONVERT(SYSNAME, OBJECT_NAME(f.object_id)), 
               PK_NAME = CONVERT(SYSNAME, i.name), 
               DEFERRABILITY = CONVERT(SMALLINT, 7)   -- SQL_NOT_DEFERRABLE  
        FROM sys.objects o1, 
             sys.objects o2, 
             sys.columns c1, 
             sys.columns c2, 
             sys.foreign_keys f
             INNER JOIN sys.foreign_key_columns k ON(k.constraint_object_id = f.object_id)
             INNER JOIN sys.indexes i ON(f.referenced_object_id = i.object_id
                                         AND f.key_index_id = i.index_id)
        WHERE o1.object_id = f.referenced_object_id
              AND (@pktable_id IS NULL
                   OR o1.object_id = @pktable_id)
              AND o2.object_id = f.parent_object_id
              AND (o2.object_id = @fktable_id)
              AND c1.object_id = f.referenced_object_id
              AND c2.object_id = f.parent_object_id
              AND c1.column_id = k.referenced_column_id
              AND c2.column_id = k.parent_column_id
        ORDER BY 1, 
                 2, 
                 3, 
                 9, 
                 4;
    END;
