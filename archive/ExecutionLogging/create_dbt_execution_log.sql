CREATE TABLE IF NOT EISTS [dbo].[DBTExecutionLog] (
	[ExecutionId] [int] IDENTITY(1,1) NOT NULL,
	[ExecutionGUID] [uniqueidentifier] NULL,
	[Command] [nvarchar](50) NULL,
	[CommandLineParams] [nvarchar](4000) NULL,
	[StartDateTime] [datetimeoffset](3) NULL,
	[EndDateTime] [datetimeoffset](3) NULL,
	[UserName] [nvarchar](100) NULL,
	[TargetName] [nvarchar](100) NULL,
	[DBTVersion] [nvarchar](50) NULL,
	[CompletionStatus] [nvarchar](50) NULL,
	[DurationMinutes]  AS (datediff(minute,[StartDateTime],[EndDateTime])) PERSISTED,
	[DurationSeconds]  AS (datediff(second,[StartDateTime],[EndDateTime])) PERSISTED
) ON [PRIMARY]
