{{
    config(
        materialized='table',
        database='SimulationsAnalyticsStage',
        alias='StageTime',
        tags=['staging', 'time']
    )
}}

WITH Numbers AS (
    SELECT TOP (86400) -- 86400 seconds in a day
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS TimeValue
    FROM 
        [master].[dbo].[spt_values] AS n1
    CROSS JOIN [master].[dbo].[spt_values] AS n2
)

SELECT
     TimeValue                                                  AS TimeKey
    ,CAST(DATEADD(SECOND, TimeValue, '00:00:00') AS TIME)       AS [Time]
    ,DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00'))     AS Hour
    ,DATEPART(MINUTE, DATEADD(SECOND, TimeValue, '00:00:00'))   AS Minute
    ,DATEPART(SECOND, DATEADD(SECOND, TimeValue, '00:00:00'))   AS Second
    ,CASE WHEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) < 12 THEN 'AM' ELSE 'PM' END AS AMPM
    ,DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00'))     AS Hour24
    ,CASE 
        WHEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) = 0 THEN 12
        WHEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) <= 12 THEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00'))
        ELSE DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) - 12
     END                                                        AS Hour12
    ,DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) * 60 + DATEPART(MINUTE, DATEADD(SECOND, TimeValue, '00:00:00')) AS MinuteOfDay
    ,DATEDIFF(SECOND, '00:00:00', DATEADD(SECOND, TimeValue, '00:00:00')) AS SecondOfDay
    ,CASE 
        WHEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) BETWEEN 5  AND 11 THEN 'Morning'
        WHEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN DATEPART(HOUR, DATEADD(SECOND, TimeValue, '00:00:00')) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
     END                                                        AS PeriodOfDay
    ,FORMAT(DATEADD(SECOND, TimeValue, '00:00:00'), 'h:mm tt')  AS TimeInWords
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3))             AS HVRChangeTime
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3))             AS StageCreatedDatetime
FROM 
    Numbers
ORDER BY     
    [Time]