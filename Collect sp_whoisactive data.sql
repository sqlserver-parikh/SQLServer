CREATE OR ALTER PROCEDURE usp_LogWhoIsActive
(
    @destination_table VARCHAR(4000) = 'tblWhoIsActive',
    @numberOfRuns INT = 2,
    @delay INT = 60 -- Delay in seconds
)
AS
BEGIN
    -- Validate the number of runs
    IF @numberOfRuns < 1 OR @numberOfRuns >= 100
    BEGIN
        RAISERROR('The number of runs must be greater than 0 and less than 100.', 16, 1);
        RETURN;
    END

    DECLARE @schema VARCHAR(4000);
    DECLARE @delayTime VARCHAR(8);

    -- Convert delay in seconds to HH:MM:SS format
    SET @delayTime = RIGHT('0' + CAST(@delay / 3600 AS VARCHAR), 2) + ':' +
                     RIGHT('0' + CAST((@delay % 3600) / 60 AS VARCHAR), 2) + ':' +
                     RIGHT('0' + CAST(@delay % 60 AS VARCHAR), 2);

    -- Create the table if it does not exist
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = @destination_table)
    BEGIN
        -- Get the schema for the table
        EXEC sp_WhoIsActive
             @get_transaction_info = 1,
             @get_plans = 1,
             @return_schema = 1,
             @get_full_inner_text = 1,
             @get_outer_command = 1,
             @find_block_leaders = 1,
             @schema = @schema OUTPUT;

        SET @schema = REPLACE(@schema, '<table_name>', @destination_table);
        PRINT @schema;

        EXEC (@schema);
    END

    DECLARE @msg NVARCHAR(1000);

    -- Loop to log active sessions
    WHILE @numberOfRuns > 0
    BEGIN
        EXEC dbo.sp_WhoIsActive 
            @get_transaction_info = 1, 
            @get_plans = 1,
            @get_full_inner_text = 1,
            @get_outer_command = 1,
            @find_block_leaders = 1,
            @destination_table = @destination_table;

        SET @numberOfRuns = @numberOfRuns - 1;

        IF @numberOfRuns > 0
        BEGIN
            SET @msg = CONVERT(CHAR(19), GETDATE(), 121) + ': Logged info. Waiting...';
            RAISERROR(@msg, 0, 0) WITH NOWAIT;
            WAITFOR DELAY @delayTime;
        END
        ELSE
        BEGIN
            SET @msg = CONVERT(CHAR(19), GETDATE(), 121) + ': Done.';
            RAISERROR(@msg, 0, 0) WITH NOWAIT;
        END
    END

    -- Cleanup old data
    DELETE FROM tblWhoIsActive
    WHERE collection_time < DATEADD(HOUR, -24, GETDATE())
          AND CONVERT(INT, RTRIM(LTRIM(REPLACE(tempdb_current, ',', '')))) < 400000
          AND (blocking_session_id IS NULL AND blocked_session_count = 0);

    DELETE FROM tblWhoIsActive
    WHERE collection_time < DATEADD(DAY, -7, GETDATE())
          AND CONVERT(INT, RTRIM(LTRIM(REPLACE(tempdb_current, ',', '')))) < 400000
          AND (blocking_session_id IS NOT NULL AND blocked_session_count = 0);

    DELETE FROM tblWhoIsActive
    WHERE collection_time < DATEADD(DAY, -10, GETDATE());
END
GO
