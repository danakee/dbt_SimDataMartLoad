{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SimulatorConfigurationPKey',
        compare_columns=[
            'SimulatorPKey',
            'ConfigurationName',
            'LocationName',
            'IsDefault',
            'SourceCreatedDatetime',
            'SourceLastUpdatedDatetime',
            'SimulatorKey',
            'HvrChangeTime'
        ],
        merge_update_columns=[
            'SimulatorPKey',
            'ConfigurationName',
            'LocationName',
            'IsDefault',
            'SourceCreatedDatetime',
            'SourceLastUpdatedDatetime',
            'SimulatorKey',
            'HvrChangeTime',
            'EDWLastUpdatedDatetime'
        ],
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
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWCreatedDatetime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWLastUpdatedDatetime]    
FROM 
    {{ ref('stg_simulator_configuration') }}
WHERE
    1=1
    {% if is_incremental() %}
    AND [HvrChangeTime] > (SELECT ISNULL(MAX([HvrChangeTime]), '1900-01-01') FROM {{ this }})
    {% endif %}  
