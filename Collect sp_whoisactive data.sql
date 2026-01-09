CREATE OR ALTER PROCEDURE dbo.usp_LogWhoIsActive
(
    @TargetTableName       NVARCHAR(512) = N'dbo.tblWhoIsActive', 
    @RunCount              INT           = 1,                    
    @DelaySeconds          INT           = 60,                   
    @GeneralRetentionHours INT           = 72,                   
    @BlockingRetentionDays INT           = 7,                    
    @DeleteAllDays         INT           = 10,                   
    @TempdbSizeThresholdMB INT           = 400,                  
    @EnableLogging         INT           = 1                     
)
AS
BEGIN
    SET NOCOUNT ON;

    /* 1) Parameter validation */
    IF @RunCount < 1 OR @RunCount >= 100
        THROW 50010, 'Run count must be between 1 and 99.', 1;

    IF @DelaySeconds < 5 OR @DelaySeconds >= 300
        THROW 50011, 'Delay must be between 5 and 300 seconds.', 1;

    /* 2) Robust Name Parsing */
    -- Handles [db].[schema].[table], [schema].[table], or just [table]
    DECLARE @DbName     SYSNAME = ISNULL(PARSENAME(@TargetTableName, 3), DB_NAME());
    DECLARE @SchemaName SYSNAME = ISNULL(PARSENAME(@TargetTableName, 2), N'dbo');
    DECLARE @ObjectName SYSNAME = PARSENAME(@TargetTableName, 1);

    IF @ObjectName IS NULL
        THROW 50013, 'Invalid @TargetTableName. Provide as [db].[schema].[name] or [schema].[name].', 1;

    -- Fully qualified quoted name for Dynamic SQL
    DECLARE @FullQuotedName NVARCHAR(1000) = QUOTENAME(@DbName) + N'.' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@ObjectName);

    /* 3) Preview Mode */
    IF @EnableLogging IN (0, 2)
    BEGIN
        EXEC master.dbo.sp_WhoIsActive
            @get_transaction_info = 1, @get_plans = 1, @get_full_inner_text = 1,
            @get_outer_command = 1, @find_block_leaders = 1;

        IF @EnableLogging = 0 RETURN;
    END

    /* 4) Create destination table if it doesn't exist (Database Aware) */
    IF OBJECT_ID(@FullQuotedName) IS NULL
    BEGIN
        DECLARE @TableSchema NVARCHAR(MAX);

        EXEC master.dbo.sp_WhoIsActive
            @get_transaction_info = 1, @get_plans = 1, @return_schema = 1,
            @get_full_inner_text = 1, @get_outer_command = 1, @find_block_leaders = 1,
            @schema = @TableSchema OUTPUT;

        -- sp_WhoIsActive returns <table_name>. Replace with our Full Quoted Name.
        SET @TableSchema = REPLACE(@TableSchema, '<table_name>', @FullQuotedName);
        EXEC sp_executesql @TableSchema;

        -- Add index if it doesn't exist
        DECLARE @IndexSQL NVARCHAR(MAX) = N'CREATE INDEX IX_collection_time ON ' + @FullQuotedName + N'(collection_time)';
        EXEC sp_executesql @IndexSQL;
    END

    /* 5) Logging loop */
    DECLARE @Counter INT = @RunCount;
    -- Convert delay to string format HH:MM:SS for maximum WAITFOR compatibility
    DECLARE @DelayStr CHAR(8) = CONVERT(CHAR(8), DATEADD(SECOND, @DelaySeconds, '00:00:00'), 108);

    WHILE @Counter > 0
    BEGIN
        -- PASS THE FULLY QUOTED NAME to sp_WhoIsActive to prevent space/special character errors
        EXEC master.dbo.sp_WhoIsActive 
            @get_transaction_info = 1, @get_plans = 1, @get_full_inner_text = 1,
            @get_outer_command = 1, @find_block_leaders = 1,
            @destination_table = @FullQuotedName; 

        SET @Counter -= 1;

        IF @Counter > 0
        BEGIN
            RAISERROR(N'Data logged. Waiting %s...', 0, 1, @DelayStr) WITH NOWAIT;
            WAITFOR DELAY @DelayStr;
        END
    END

    /* 6) Cleanup logic */
    -- Note: tempdb_current in sp_WhoIsActive is 8KB pages. 
    -- Calculation: (@ThresholdMB * 1024) / 8 = @ThresholdMB * 128
    DECLARE @CleanupSQL NVARCHAR(MAX) = N'
        DELETE FROM ' + @FullQuotedName + N'
        WHERE 
        (
            -- Condition A: General Retention (No blocking, low tempdb)
            collection_time < DATEADD(HOUR, -@GenHours, GETDATE())
            AND ISNULL(TRY_CONVERT(BIGINT, REPLACE(REPLACE(tempdb_current, '','', ''''), '' '', '''')), 0) < (@ThresholdMB * 128)
            AND blocking_session_id IS NULL 
            AND ISNULL(TRY_CONVERT(INT, blocked_session_count), 0) = 0
        )
        OR 
        (
            -- Condition B: Blocking Retention (Keep blocks longer, but still respect tempdb threshold)
            collection_time < DATEADD(DAY, -@BlockDays, GETDATE())
            AND ISNULL(TRY_CONVERT(BIGINT, REPLACE(REPLACE(tempdb_current, '','', ''''), '' '', '''')), 0) < (@ThresholdMB * 128)
        )
        OR 
        (
            -- Condition C: Absolute Purge
            collection_time < DATEADD(DAY, -@MaxDays, GETDATE())
        );';

    EXEC sp_executesql @CleanupSQL,
        N'@GenHours INT, @BlockDays INT, @MaxDays INT, @ThresholdMB BIGINT',
        @GenHours   = @GeneralRetentionHours,
        @BlockDays  = @BlockingRetentionDays,
        @MaxDays    = @DeleteAllDays,
        @ThresholdMB= @TempdbSizeThresholdMB;

    RAISERROR(N'Cleanup and Logging completed for %s', 0, 1, @FullQuotedName) WITH NOWAIT;
END
GO
