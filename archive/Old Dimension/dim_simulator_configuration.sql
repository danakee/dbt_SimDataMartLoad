{{
    config(
        materialized='dim_simulator_configuration',
        database='SimulationsAnalytics',
        alias='DimSimulatorConfiguration',
        tags=['mart', 'simulatorconfiguration']
    )
}}

SELECT 
     [SimulatorConfigurationPKey]
    ,[SimulatorPKey]
    ,[ConfigurationName]
    ,[LocationName]
    ,[IsDefault]
    ,[SourceCreatedDatetime]
    ,[SourceLastUpdatedDatetime]
    ,CAST(NULL AS int) AS [SimulatorKey] -- Snowflake to DimSimulator
    ,[HvrChangeTime]
FROM 
    {{ ref('stg_simulator_configuration') }}
