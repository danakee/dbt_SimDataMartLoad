{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimDepartment',
        should_log=true,
        tags=['mart', 'dim', 'department']
    )
}}

{% set columns_to_update = [
	'DepartmentId',
	'LocationId',
	'DepartmentLocationName',
	'DepartmentName',
	'EACDepartmentName',
	'EACGroupId',
	'EACGroupName',
	'DivisionIdCode',
	'DivisionName',
	'ParentDivisionIdCode',
	'ParentDivisionName',
	'LocationTypeCode',
	'LocationTypeDescription',
	'DepartmentHeadId',
	'DepartmentPoolId',
	'DepartmentPoolName',
	'DepartmentHeadName',
	'DepartmentHeadTitle',
	'IsActive',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['DepartmentPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                DepartmentKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
	            DepartmentPKey int NOT NULL,
	            DepartmentId varchar(50) NULL,
	            LocationId varchar(15) NULL,
	            DepartmentLocationName varchar(50) NULL,
	            DepartmentName varchar(50) NULL,
	            EACDepartmentName varchar(50) NULL,
	            EACGroupId int NULL,
	            EACGroupName varchar(50) NULL,
	            DivisionIdCode varchar(10) NULL,
	            DivisionName varchar(50) NULL,
	            ParentDivisionIdCode varchar(10) NULL,
	            ParentDivisionName varchar(50) NULL,
	            LocationTypeCode varchar(50) NULL,
	            LocationTypeDescription varchar(256) NULL,
	            DepartmentHeadId int NULL,
	            DepartmentPoolId int NULL,
	            DepartmentPoolName varchar(50) NULL,
	            DepartmentHeadName varchar(102) NULL,
	            DepartmentHeadTitle varchar(256) NULL,
	            IsActive bit NULL,
	            HVRChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_department'),
    unique_key='DepartmentPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}