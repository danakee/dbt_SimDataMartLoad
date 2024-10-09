{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimSimulatorQualification',
        should_log=true,
        tags=['mart', 'dim', 'simulatorqualification']
    )
}}

-- Add the below columns for future snowflake surrogate key columns
-- 'SimulatorConfigurationKey',
-- 'OriginalSimulatorQualificationProjectKey',

{% set columns_to_update = [
    'SimulatorConfigurationPKey',
    'SimulatorPKey',
    'QualificationAgencyId',
    'QualificationLevelId',
    'SimulatorQualificationEffectiveDate',
    'SimulatorQualificationExpiryDate',
    'SimulatorQualificationDescription',
    'SimulatorQualificationProcessStartDate',
    'ScheduledRequalificationDate',
    'InitialSimulatorQualificationDate',
    'IsSimulatorQualificationDeleted',
    'OriginalSimulatorQualificationProjectName',
    'IsHiddenInReport',
    'SimulatorQualificationLevelName',
    'SimulatorQualificationLevelDescription',
    'IsQualificationLevelApproved',
    'ACCircularName',
    'IsACCircularApproved',
    'SimulatorQualificationAgencyName',
    'SimulatorQualificationAgencyDescription',
    'AgencySimulatorId',
    'IsAgencyDomestic',
    'AgencySponsorCountryCode',
    'IsAgencyDeleted',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['SimulatorQualificationPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                SimulatorQualificationKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                SimulatorQualificationPKey int NOT NULL,
                SimulatorConfigurationPKey int NOT NULL,
                SimulatorPKey int NOT NULL,
                QualificationAgencyId int NULL,
                QualificationLevelId int NULL,
                SimulatorQualificationEffectiveDate date NULL,
                SimulatorQualificationExpiryDate date NULL,
                SimulatorQualificationDescription varchar(512) NULL,
                SimulatorQualificationProcessStartDate date NULL,
                ScheduledRequalificationDate date NULL,
                InitialSimulatorQualificationDate date NULL,
                IsSimulatorQualificationDeleted bit NULL,
                OriginalSimulatorQualificationProjectName varchar(25) NULL,
                IsHiddenInReport bit NULL,
                SimulatorQualificationLevelName varchar(50) NULL,
                SimulatorQualificationLevelDescription varchar(256) NULL,
                IsQualificationLevelApproved bit NULL,
                ACCircularName varchar(50) NULL,
                IsACCircularApproved bit NULL,
                SimulatorQualificationAgencyName varchar(20) NULL,
                SimulatorQualificationAgencyDescription varchar(100) NULL,
                AgencySimulatorId varchar(50) NULL,
                IsAgencyDomestic bit NULL,
                AgencySponsorCountryCode int NULL,
                IsAgencyDeleted bit NULL,
                --SimulatorConfigurationKey int NULL,                   -- Placeholder for future snowflake surrogate key columns
                --OriginalSimulatorQualificationProjectKey int NULL,    -- Placeholder for future snowflake surrogate key columns
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_simulator_qualification'),
    unique_key='SimulatorQualificationPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}