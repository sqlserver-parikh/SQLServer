USE msdb;
DECLARE @lookbackdays INT = 1;
DECLARE @intervalhours INT = 2;
DECLARE @backuptype char(1) = 'L' -- D is for full, I for differential, L is for Log
DECLARE @CurrentTime DATETIME = GETDATE();
DECLARE @RoundedTime DATETIME;
DECLARE @RECOVERYMODEL int;

IF @backuptype = 'L'
SET @RECOVERYMODEL = 3
ELSE 
SET @RECOVERYMODEL = 4

-- Calculate the nearest 3-hour interval
SET @RoundedTime = DATEADD(HOUR, ROUND(DATEDIFF(HOUR, '2000-01-01T00:00:00', @CurrentTime) / @intervalhours, 0) * @intervalhours, '2000-01-01T00:00:00');

-- Create a temporary table to represent the intervals
CREATE TABLE #Intervals (
    IntervalStart DATETIME,
    IntervalEnd DATETIME,
    DBName NVARCHAR(255)  -- Adjust the data type and length as needed
);

-- Populate the intervals for the past 30 days for each database
DECLARE @StartDate DATETIME = DATEADD(DAY, -@lookbackdays, @RoundedTime);
DECLARE @EndDate DATETIME = @RoundedTime;
DECLARE @IntervalStart DATETIME = @StartDate;

-- Assuming you have a list of database names (replace with actual database names)
DECLARE @DatabaseNames TABLE (DBName NVARCHAR(255));
INSERT INTO @DatabaseNames (DBName)
SELECT name FROM sys.databases d WHERE d.state_desc = 'ONLINE' AND database_id <> 2 AND recovery_model < @RECOVERYMODEL;

WHILE @IntervalStart < @EndDate
BEGIN
    DECLARE @IntervalEnd DATETIME = DATEADD(HOUR, @intervalhours, @IntervalStart);
    
    -- Insert rows for each database
    INSERT INTO #Intervals (IntervalStart, IntervalEnd, DBName)
    SELECT @IntervalStart, @IntervalEnd, DBName
    FROM @DatabaseNames;
    
    SET @IntervalStart = @IntervalEnd;
END

-- Query to find the maximum log backup time for each interval
;WITH BackupHistory AS (
    SELECT
        d.name AS database_name,
        bh.backup_finish_date,
        ROW_NUMBER() OVER (PARTITION BY d.name ORDER BY bh.backup_finish_date DESC) AS rn
    FROM sys.databases d
    LEFT JOIN msdb.dbo.backupset bh ON d.name = bh.database_name
        AND bh.type = @backuptype -- Log backups
		AND database_id <> 2
        AND bh.backup_finish_date >= @StartDate
		AND D.recovery_model < @RECOVERYMODEL
), CTE2 AS (
    SELECT
        i.IntervalStart,
        i.IntervalEnd,
        bh.database_name,
        MAX(bh.backup_finish_date) AS MaxBackupTime
    FROM #Intervals i
    CROSS JOIN BackupHistory bh
WHERE (bh.rn = 1 AND bh.backup_finish_date BETWEEN i.IntervalStart AND i.IntervalEnd)
   OR (bh.rn > 1 AND bh.backup_finish_date >= i.IntervalStart AND bh.backup_finish_date <= i.IntervalEnd)
   GROUP BY i.IntervalStart, i.IntervalEnd, bh.database_name
), cte3 AS (
    SELECT cte2.*, d.recovery_model_desc, d.state_desc
    FROM CTE2
    LEFT JOIN sys.databases d ON cte2.database_name = d.name
    WHERE d.recovery_model < @RECOVERYMODEL
    AND d.state_desc = 'ONLINE'
), cte4 AS 
(
    SELECT i.DBName, MAX(i.IntervalStart) AS LastFail, MIN(i.IntervalStart) AS FirstFail, COUNT(*) AS TotalFailed, ((SELECT COUNT(*) FROM #Intervals) / (SELECT COUNT(DISTINCT dbname) FROM #Intervals)) AS TotalIntervals 
    FROM cte3 c RIGHT JOIN #Intervals I ON c.database_name = i.DBName AND c.IntervalEnd = i.IntervalEnd AND c.IntervalStart = i.IntervalStart
    WHERE database_name IS NULL
    GROUP BY i.DBName
)
SELECT @@SERVERNAME ServerName, *, CONVERT(DECIMAL(6,2), TotalFailed*1.0/TotalIntervals*1.0*100) PctFailed
, case 
	WHEN @backuptype = 'L' THEN 'LOG'
	WHEN @backuptype = 'I' THEN 'DIFFERENTIAL'
	WHEN @backuptype = 'D' THEN 'FULL'
  END BackupReport 
FROM cte4
ORDER BY 1 DESC;
GO

-- Clean up the temporary table
DROP TABLE #Intervals;
