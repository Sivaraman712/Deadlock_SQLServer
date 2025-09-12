USE [DBADB]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

USE [DBADB]
GO

CREATE TABLE [dbo].[DL_REPORT_XML_DATA](
	[EVENTTIME] [datetime] NULL,
	[XMLREPORT] [xml] NULL,
	[ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED)

	GO

CREATE PROCEDURE [dbo].[DBA_Deadlock_data_import]
	
AS
BEGIN


	
	SET NOCOUNT ON;

Truncate table dbadb.[dbo].[DL_REPORT_XML_DATA];

INSERT INTO [dbo].[DL_REPORT_XML_DATA]
           ([EVENTTIME]
           ,[XMLREPORT]) 
select  timestamp_utc at time zone 'UTC' at time zone 'India Standard Time' as Event_time_IST,
cast(ef.event_data as xml).query('/event/data/value/deadlock') AS Deadlock_XML
from sys.fn_xe_file_target_read_file('system_health*.xel',NULL, NULL, NULL) ef
where ef.object_name='xml_deadlock_report' and CAST(ef.timestamp_utc AS DATETIME2(7)) >  DATEADD(day, -2, GETUTCDATE())  -- Change
END

GO


