USE tempdb
GO

CREATE OR ALTER PROCEDURE usp_LogWhoIsActive
(
    @TargetTableName VARCHAR(4000) = 'tblWhoIsActive',         -- Table to store the logged data
    @RunCount INT = 1,                                        -- Number of logging iterations
    @DelaySeconds INT = 60,                                   -- Delay between runs in seconds
    @GeneralRetentionDays INT = 10,                           -- General data retention period in days
    @TempdbSizeThresholdMB INT = 400,                         -- Tempdb size threshold in MB
    @TempdbBlockingRetentionHours INT = 72,                   -- Retention period for tempdb blocking in hours
    @BlockingRetentionDays INT = 7,                           -- Retention period for blocking sessions in days
    @EnableLogging BIT = 1                                    -- Flag to enable/disable logging
)
AS
BEGIN
    -- Convert tempdb threshold from MB to KB for comparison
    SET @TempdbSizeThresholdMB = @TempdbSizeThresholdMB * 1024

    -- If logging is disabled, just execute sp_WhoIsActive once without storing data
    IF @EnableLogging = 0
    BEGIN
        EXEC sp_WhoIsActive
            @get_transaction_info = 1,    -- Include transaction details
            @get_plans = 1,              -- Include execution plans
            @get_full_inner_text = 1,    -- Include full inner text of queries
            @get_outer_command = 1,      -- Include outer command details
            @find_block_leaders = 1      -- Identify blocking leaders
    END
    ELSE
    BEGIN
        -- Validate run count parameter
        IF @RunCount < 1 OR @RunCount >= 100
        BEGIN
            RAISERROR('Run count must be between 1 and 99.', 16, 1);
            RETURN;
        END

        -- Validate delay time parameter
        IF @DelaySeconds < 5 OR @DelaySeconds >= 300
        BEGIN
            RAISERROR('Delay must be between 5 and 300 seconds.', 16, 1);
            RETURN;
        END

        DECLARE @TableSchema VARCHAR(4000);         -- Stores the schema for table creation
        DECLARE @FormattedDelayTime VARCHAR(8);     -- Delay time in HH:MM:SS format

        -- Convert delay seconds to HH:MM:SS format for WAITFOR
        SET @FormattedDelayTime = RIGHT('0' + CAST(@DelaySeconds / 3600 AS VARCHAR), 2) + ':' +
                                 RIGHT('0' + CAST((@DelaySeconds % 3600) / 60 AS VARCHAR), 2) + ':' +
                                 RIGHT('0' + CAST(@DelaySeconds % 60 AS VARCHAR), 2);

        -- Create target table if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = @TargetTableName)
        BEGIN
            -- Generate table schema using sp_WhoIsActive
            EXEC sp_WhoIsActive
                @get_transaction_info = 1,
                @get_plans = 1,
                @return_schema = 1,         -- Return schema instead of executing
                @get_full_inner_text = 1,
                @get_outer_command = 1,
                @find_block_leaders = 1,
                @schema = @TableSchema OUTPUT;

            -- Replace placeholder with actual table name and create table
            SET @TableSchema = REPLACE(@TableSchema, '<table_name>', @TargetTableName);
            PRINT @TableSchema;
            EXEC (@TableSchema);
        END

        DECLARE @StatusMessage NVARCHAR(1000);    -- Message for logging progress

        -- Main logging loop
        WHILE @RunCount > 0
        BEGIN
            -- Execute sp_WhoIsActive and log results to target table
            EXEC dbo.sp_WhoIsActive 
                @get_transaction_info = 1,
                @get_plans = 1,
                @get_full_inner_text = 1,
                @get_outer_command = 1,
                @find_block_leaders = 1,
                @destination_table = @TargetTableName;

            SET @RunCount = @RunCount - 1;

            -- Provide status updates between runs
            IF @RunCount > 0
            BEGIN
                SET @StatusMessage = CONVERT(CHAR(19), GETDATE(), 121) + ': Data logged. Waiting...';
                RAISERROR(@StatusMessage, 0, 0) WITH NOWAIT;
                WAITFOR DELAY @FormattedDelayTime;
            END
            ELSE
            BEGIN
                SET @StatusMessage = CONVERT(CHAR(19), GETDATE(), 121) + ': Logging completed.';
                RAISERROR(@StatusMessage, 0, 0) WITH NOWAIT;
            END
        END

        -- Cleanup: Remove old records based on tempdb size and non-blocking conditions
        DELETE FROM tblWhoIsActive
        WHERE collection_time < DATEADD(HOUR, -@TempdbBlockingRetentionHours, GETDATE())
            AND CONVERT(INT, RTRIM(LTRIM(REPLACE(tempdb_current, ',', '')))) < @TempdbSizeThresholdMB
            AND (blocking_session_id IS NULL AND blocked_session_count = 0);

        -- Cleanup: Remove old records based on tempdb size and blocking retention
        DELETE FROM tblWhoIsActive
        WHERE collection_time < DATEADD(DAY, -@BlockingRetentionDays, GETDATE())
            AND CONVERT(INT, RTRIM(LTRIM(REPLACE(tempdb_current, ',', '')))) < @TempdbSizeThresholdMB;

        -- Cleanup: Remove all records older than general retention period
        DELETE FROM tblWhoIsActive
        WHERE collection_time < DATEADD(DAY, -@GeneralRetentionDays, GETDATE());
    END
END
GO
