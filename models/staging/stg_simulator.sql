{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageSimulator',
        tags=['staging', 'simulator']
    )
}}

{% set source_query %}
    WITH SimulatorWithAttributes AS (
        SELECT
             CAST(s.PKey AS int)        AS SimulatorPKey
            ,s.sim_id                   AS SimulatorId
            ,TRIM(s.name)               AS SimulatorName
            ,s.createdt                 AS SourceCreatedDate
            ,s.lastmod_dt               AS SourceLastUpdatedDate
            ,s.corpid                   AS FSIAssetNumber
            ,s.loc_id                   AS LocationId
            ,s.lead_loc_id              AS LeadLocationId
            ,s.dest_id                  AS DestinationId
            ,s.ils_yn                   AS IsILS
            ,s.ready_for_trng           AS DateReadyForTraining
            ,st.PKey                    AS SimulatorTypeKey
            ,st.name                    AS SimulatorTypeName
            ,acm.PKey                   AS AircraftModelKey
            ,acm.code                   AS AircraftModelCode
            ,acm.model_name             AS AircraftModelName
            ,acmt.typevalue             AS AircraftModelTypeValue
            ,acmt.name                  AS AircraftTypeName
            ,acmt.description           AS AircraftTypeDescription
            ,ss.PKey                    AS StatusKey
            ,ss.name                    AS StatusName
            ,s.lost_time                AS TrackLostTime
            ,sg.PKey                    AS SimulatorGroupKey
            ,sg.name                    AS SimulatorGroupName
            ,sg.description             AS SimulatorGroupDescription
            ,s.shipdt                   AS ShipDate
            ,s.fk_owner                 AS OwnerId
            ,own.custid                 AS OwnerCustomerId
            ,own.description            AS OwnerDescription
            ,mfg.mfg_id                 AS ManufacturerId
            ,mfg.mfg_name               AS ManufacturerName
            ,(SELECT MAX(ct) FROM (VALUES 
                    (s.hvr_change_time), 
                    (mfg.hvr_change_time),
                    (st.hvr_change_time),
                    (sg.hvr_change_time),
                    (acm.hvr_change_time),
                    (acmt.hvr_change_time),
                    (own.hvr_change_time),
                    (stg.hvr_change_time),
                    (ss.hvr_change_time)
            ) AS ChangeTime(ct))                        AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
        FROM 
            {{ source('sim2', 'tblSim') }} AS s
            LEFT JOIN {{ source('sim2', 'tblMfg') }} AS mfg
                ON s.fk_mfg = mfg.PKey
            LEFT JOIN {{ source('sim2', 'tblSimType') }} AS st
                ON s.fk_type = st.PKey
            LEFT JOIN {{ source('sim2', 'tblSimGroup') }} AS sg
                ON s.fk_simgroup = sg.PKey
            LEFT JOIN {{ source('sim2', 'tblACModels') }} AS acm
                ON s.fk_acmodel = acm.PKey
            LEFT JOIN {{ source('sim2', 'tblACModelType') }} AS acmt
                ON acm.typevalue = acmt.PKey
            LEFT JOIN {{ source('sim2', 'tblOwner') }} AS own
                ON s.fk_owner = own.PKey
            LEFT JOIN {{ source('sim2', 'tblSimTypeGroup') }} AS stg
                ON st.fk_parentypepk = stg.PKey
            LEFT JOIN {{ source('sim2', 'tblStatus') }} AS ss
                ON s.fk_status = ss.PKey
    )

    INSERT INTO {{ this }} (
         SimulatorPKey
        ,SimulatorId
        ,SimulatorName
        ,SourceCreatedDate
        ,SourceLastUpdatedDate
        ,FSIAssetNumber
        ,LocationId
        ,LeadLocationId
        ,DestinationId
        ,IsILS
        ,DateReadyForTraining
        ,SimulatorTypeKey
        ,SimulatorTypeName
        ,AircraftModelKey
        ,AircraftModelCode
        ,AircraftModelName
        ,AircraftModelTypeValue
        ,AircraftTypeName
        ,AircraftTypeDescription
        ,StatusKey
        ,StatusName
        ,TrackLostTime
        ,SimulatorGroupKey
        ,SimulatorGroupName
        ,SimulatorGroupDescription
        ,ShipDate
        ,OwnerId
        ,OwnerCustomerId
        ,OwnerDescription
        ,ManufacturerId
        ,ManufacturerName
        ,HvrChangeTime
        ,StageCreatedDatetime
    )
    SELECT 
         SimulatorPKey
        ,SimulatorId
        ,SimulatorName
        ,SourceCreatedDate
        ,SourceLastUpdatedDate
        ,FSIAssetNumber
        ,LocationId
        ,LeadLocationId
        ,DestinationId
        ,IsILS
        ,DateReadyForTraining
        ,SimulatorTypeKey
        ,SimulatorTypeName
        ,AircraftModelKey
        ,AircraftModelCode
        ,AircraftModelName
        ,AircraftModelTypeValue
        ,AircraftTypeName
        ,AircraftTypeDescription
        ,StatusKey
        ,StatusName
        ,TrackLostTime
        ,SimulatorGroupKey
        ,SimulatorGroupName
        ,SimulatorGroupDescription
        ,ShipDate
        ,OwnerId
        ,OwnerCustomerId
        ,OwnerDescription
        ,ManufacturerId
        ,ManufacturerName
        ,HvrChangeTime
        ,StageCreatedDatetime
    FROM 
        SimulatorWithAttributes
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "sim2.tblSim",
        "sim2.tblMfg",
        "sim2.tblSimType",
        "sim2.tblSimGroup",
        "sim2.tblACModels",
        "sim2.tblACModelType",
        "sim2.tblOwner",
        "sim2.tblSimTypeGroup",
        "sim2.tblStatus"
    ],
    unique_key="SimulatorPKey"
) }}
