{{
    config(
        materialized='table',
        database='SimulationsAnalyticsStage',
        alias='StageDate',
        unique_key='DateKey',        
        indexes=[
            {'columns': ['CalendarDate']},
            {'columns': ['AccountingPeriodKey']}
        ],        
        tags=['staging', 'date']
    )
}}

WITH Numbers AS (
    SELECT TOP (36525) -- 100 years worth of days
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS N
    FROM 
        {{ source('utility', 'numbers_table') }} t1
        CROSS JOIN {{ source('utility', 'numbers_table') }} t2
),
DateTable AS (
    SELECT
        DATEADD(DAY, N, '1950-01-01') AS [Date]
    FROM 
        Numbers
    WHERE 
        DATEADD(DAY, N, '1950-01-01') <= '2050-12-31'
)
SELECT 
    CONVERT(INT, FORMAT([Date], 'yyyyMMdd')) AS DateKey
    ,[Date] AS CalendarDate
    ,YEAR([Date]) * 100 + MONTH([Date]) AS AccountingPeriodKey
    ,CAST(FORMAT([Date], 'MMMM yyyy') AS varchar(50)) AS AccountingPeriodName
    ,DATEFROMPARTS(YEAR([Date]), MONTH([Date]), 1) AS AccountingPeriodStartDate
    ,EOMONTH([Date]) AS AccountingPeriodEndDate
    ,YEAR([Date]) AS Year
    ,DATEPART(QUARTER, [Date]) AS Quarter
    ,MONTH([Date]) AS Month
    ,DATENAME(MONTH, [Date]) AS MonthName
    ,LEFT(DATENAME(MONTH, [Date]), 3) AS MonthAbbreviation
    ,DAY([Date]) AS Day
    ,DATEPART(WEEKDAY, [Date]) AS DayOfWeek
    ,DATENAME(WEEKDAY, [Date]) AS DayName
    ,CASE WHEN DATEPART(WEEKDAY, [Date]) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend
    ,DATEPART(WEEK, [Date]) AS WeekOfYear
FROM 
    DateTable
ORDER BY 
    [Date]