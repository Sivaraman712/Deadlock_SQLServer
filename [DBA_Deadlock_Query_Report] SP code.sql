USE [DBADB]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[DBA_Deadlock_Query_Report]
	
AS
BEGIN
	
	SET NOCOUNT ON;
	-- Step 1: Define Date Range
DECLARE @yesterday NVARCHAR(19) = FORMAT(DATEADD(DAY,-1,GETDATE()), 'yyyy-MM-dd 14:00:00'); -- Change
DECLARE @today NVARCHAR(19) = FORMAT(GETDATE(), 'yyyy-MM-dd 14:00:00'); --Change
DECLARE @DLCount INT;
DECLARE @Subject NVARCHAR(255);

SET @Subject='Client ' +@@SERVERNAME+' SQL Server Deadlock Report' -- change
-- Step 2: Extract and Process Deadlock Data
WITH DeadlockEvents AS (
    SELECT XMLREPORT AS DeadlockXML , ID ,EventTime
    FROM DBADB.dbo.DL_REPORT_XML_DATA
),
Victim AS (
    SELECT 
        EventTime,
        DeadlockXML,
        V.value('@id', 'NVARCHAR(100)') AS VictimProcessID
    FROM DeadlockEvents
    CROSS APPLY DeadlockXML.nodes('/deadlock/victim-list/victimProcess') AS VP(V)
),
ProcessDetails AS (
    SELECT 
        D.EventTime,
        d.DeadlockXML,
        V.VictimProcessID,
        P.value('@id', 'NVARCHAR(100)') AS ProcessID,
        P.value('(inputbuf)[1]', 'NVARCHAR(MAX)') AS InputBuf,
        (
            SELECT STRING_AGG(F.value('@procname', 'NVARCHAR(200)'), ' -> ')
            FROM P.nodes('executionStack/frame') AS FS(F)
            WHERE F.value('@procname', 'NVARCHAR(200)') NOT IN ('unknown')
        ) AS SPCallChain,
        (
            SELECT TOP 1 F.value('text()[1]', 'NVARCHAR(MAX)')
            FROM P.nodes('executionStack/frame') AS FS(F)
            ORDER BY F.value('@line', 'INT') DESC
        ) AS FinalQuery,
        R.value('@objectname', 'NVARCHAR(200)') AS LockedObject,
        R.value('local-name(.)', 'NVARCHAR(100)') AS LockType,
        D.ID
    FROM Victim V
    JOIN DeadlockEvents D ON D.EventTime = V.EventTime
    CROSS APPLY D.DeadlockXML.nodes('/deadlock/process-list/process') AS PL(P)
    OUTER APPLY D.DeadlockXML.nodes('/deadlock/resource-list/*') AS RL(R)
),
FinalReport AS (
    SELECT 
        EventTime,
        MAX(CASE WHEN ProcessID = VictimProcessID THEN FinalQuery END) AS VictimQuery,
        MAX(CASE WHEN ProcessID <> VictimProcessID THEN FinalQuery END) AS WaiterQuery,
        MAX(CASE WHEN ProcessID = VictimProcessID THEN InputBuf END) AS VictimExecutedStmt,
        MAX(CASE WHEN ProcessID <> VictimProcessID THEN InputBuf END) AS WaiterExecutedStmt,
        --MAX(CASE WHEN ProcessID = VictimProcessID THEN SPCallChain END) AS VictimSPChain,
        --MAX(CASE WHEN ProcessID <> VictimProcessID THEN SPCallChain END) AS WaiterSPChain,
        MAX(CASE WHEN ProcessID = VictimProcessID THEN LockedObject END) AS DeadlockResource,
        MAX(CASE WHEN ProcessID <> VictimProcessID THEN LockType END) AS LockType,
        D.ID
    FROM ProcessDetails D
    GROUP BY EventTime, ID
)

-- Step 3: Insert into Temp Table
SELECT * 
INTO #DeadlockReport
FROM FinalReport
WHERE EventTime BETWEEN @yesterday AND @today
ORDER BY EventTime;


 SELECT @DLCount= count(1)
    FROM DBADB.dbo.DL_REPORT_XML_DATA
WHERE EventTime BETWEEN @yesterday AND @today

-- Step 4: Convert to HTML with Header
DECLARE @html NVARCHAR(MAX) = 
    N'<html>' +
	'<head>' +
	'<style>' +
    'body { font-family: Arial, sans-serif; color: #333; line-height: 1.6; }' +
    'table { width: 100%; border-collapse: collapse; margin-top: 20px; }' +
    'table, th, td { border: 1px solid #ddd; }' +
    'th, td { padding: 12px; text-align: centre; }' +
    'th { background-color: #333333; color: white; }' +
    'tr:nth-child(even) { background-color: #f2f2f2; }' +
    'p { margin: 0 0 10px; }' +
    'h2 { color: #556B2F;text-align: centre; }' +
    '</style>' +
	'</head>' +
	'<body>'+
    N'<h2>InvoiceMart Deadlock Report - '+ @@Servername+ ' - at-aws-treds-core-proddb.cfdommbpfnas.ap-south-1.rds.amazonaws.com </h2>' +
    N'<p>This report lists deadlock events captured between <b>' + @yesterday + '</b> and <b>' + @today + '</b>.</p>' +
	N'<p> Deadlock Incident between <b>' + @yesterday + '</b> and <b>' + @today + ' is '+ cast(@DLCount as nvarchar) +'  </p>' +
    N'<table border="1" cellpadding="4" cellspacing="0">' +
    N'<tr><th>EventTime</th><th>VictimQuery</th><th>WaiterQuery</th><th>VictimExecutedStmt</th><th>WaiterExecutedStmt</th>' +
    N'<th>DeadlockResource</th><th>LockType</th></tr>' + --<th>VictimSPChain</th><th>WaiterSPChain</th><th>ID</th>
    CAST((
        SELECT 
            td = EventTime, '',
            td = VictimQuery, '',
            td = WaiterQuery, '',
            td = VictimExecutedstmt, '',
            td = WaiterExecutedstmt, '',
            --td = VictimSPChain, '',
           -- td = WaiterSPChain, '',
            td = DeadlockResource, '',
            td = LockType, ''
            
        FROM #DeadlockReport
        FOR XML PATH('tr'), TYPE
    ) AS NVARCHAR(MAX)) +
    N'</table>
	<br>
	<p> <strong>Note:</strong> The query details are extracted from the Deadlock XML report. For more information, please refer to the table DBADB.dbo.DL_REPORT_XML_DATA. </p> 
	</body></html>';


 --Step 5: Send Email via Database Mail
 if @DLCount>0
 BEGIN
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = '',  -- Replace with your actual profile name
	@recipients = '',
    @subject = @Subject,
    @body = @html,
    @body_format = 'HTML';
END


DROP table #DeadlockReport;

END

GO


