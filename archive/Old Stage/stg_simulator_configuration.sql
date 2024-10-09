{{ 
    config(
        materialized='table',
        database='SimulationsAnalytics',
        alias='StageSimulatorConfiguration',
        tags=['staging']
    )
}}

WITH [SimulationConfiguration] AS (
    SELECT 
        -- COUNT(1) OVER (PARTITION BY [c].[cfgid]) AS [SimulationConfigurationMultiCount]
         [c].[PKey]         AS [SimulatorConfigurationPKey]
        ,[s].[PKey]         AS [SimulatorPKey]
        ,[c].[name]         AS [ConfigurationName]
        ,[l].[Name]         AS [LocationName]
        ,[c].[default]      AS [IsDefault]
        ,[c].[createdt]     AS [SourceCreatedDatetime]
        ,[c].[lastmoddt]    AS [SourceLastUpdatedDatetime]
        ,(SELECT MAX([ct]) FROM (VALUES 
                ([s].[hvr_change_time]),
                ([so].[hvr_change_time]),
                ([scc].[hvr_change_time]),
                ([c].[hvr_change_time]),
                ([l].[hvr_change_time])
        ) AS [ChangeTime]([ct])) AS [HvrChangeTime]
    FROM
        {{ source('sim2', 'tblSim') }} AS [s]
        JOIN {{ source('sim2', 'tblObjectHier') }} AS [so]
            ON [s].[PKey] = [so].[fk_head]
        JOIN {{ source('sim2', 'tblClassLink') }} AS [scc]
            ON  [so].[fk_classlink] = [scc].[PKey]
            AND [scc].[name] = 'sim_to_cfg'
        JOIN {{ source('sim2', 'tblConfig') }} AS [c]
            ON [so].[fk_tail] = [c].[PKey]
        LEFT JOIN {{ source('employee2', 'tblLocation') }} AS [l]
            ON [s].[loc_id] = [l].[PKey]
    WHERE   
        1=1
        --AND [s].[latest] = 1 -- Latest simulator
        --AND [c].[latest] = 1 -- Latest configuration
)

SELECT 
     [SimulatorConfigurationPKey]
    ,[SimulatorPKey]
    ,[ConfigurationName]
    ,[LocationName]
    ,[IsDefault]
    ,[SourceCreatedDatetime]
    ,[SourceLastUpdatedDatetime]
    ,[HvrChangeTime]
FROM 
    [SimulationConfiguration];
