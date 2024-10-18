{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimIssue',
        should_log=true,
        tags=['mart', 'dim', 'issue']
    )
}}

{% set columns_to_update = [
    'IssueId',
    'IssueDate',
    'IssueTime',
    'SimulatorPKey',
    'SimulatorId',
    'IsAffectedSystem',
    'AffectedConfigurationList',
    'CenterKey',
    'IssueDescription',
    'IssueTypeDescription',
    'IssuePriorityNumber',
    'IssuePriority',
    'IssueStatusName',
    'IssueCategoryName',
    'IssueClassificationName',
    'IssueCloseTimestamp',
    'TrainingRestrictionText',
    'IsCriticalTrainingIssue',
    'TrainingCategoryName',
    'JobNumber',
    'IsMMI',
    'DocumentNumber',
    'ChargeCode',
    'DueDate',
    'WrittenByName',
    'WrittenByContactInfoTxt',
    'RegulatoryAgencyName',
    'LostTimeTypeDescription',
    'LostTimeMinutesNumber',
    'FSTDLostTimeMinutesNumber',
    'FSTDTypeName',
    'RootCauseTypeName',
    'RootCauseTypeDescription',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['IssuePKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                IssueKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
	            IssuePKey int NOT NULL,
	            IssueId int NULL,
	            IssueDate date NULL,
	            IssueTime time(7) NULL,
	            SimulatorPKey int NOT NULL,
	            SimulatorId int NULL,
	            IsAffectedSystem bit NULL,
	            AffectedConfigurationList varchar(8000) NULL,
	            CenterKey int  NULL,
	            IssueDescription varchar(3000) NULL,
	            IssueTypeDescription varchar(50) NULL,
	            IssuePriorityNumber varchar(25) NULL,
                IssuePriority varchar(1000) NULL,
	            IssueStatusName varchar(30) NULL,
	            IssueCategoryName varchar(50) NULL,
	            IssueClassificationName varchar(25) NULL,
	            IssueCloseTimestamp datetime NULL,
	            TrainingRestrictionText varchar(1024) NULL,
	            IsCriticalTrainingIssue bit NULL,
	            TrainingCategoryName nvarchar(50) NULL,
	            JobNumber varchar(25) NOT NULL,
	            IsMMI bit NULL,
	            DocumentNumber varchar(50) NULL,
	            ChargeCode varchar(35) NULL,
	            DueDate date NULL,
	            WrittenByName varchar(128) NULL,
	            WrittenByContactInfoTxt varchar(128) NULL,
	            RegulatoryAgencyName varchar(20) NULL,
	            LostTimeTypeDescription varchar(20) NULL,
	            LostTimeMinutesNumber int NULL,
	            FSTDLostTimeMinutesNumberint NULL,
	            FSTDTypeName varchar(20) NULL,
	            RootCauseTypeName varchar(50) NULL,
	            RootCauseTypeDescription varchar(1000) NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_issue'),
    unique_key='IssuePKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}