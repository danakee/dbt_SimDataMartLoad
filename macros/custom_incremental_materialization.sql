{% materialization custom_incremental_materialization, default %}

{% set target_relation = this.incorporate(type='table') %}
{% set existing_relation = load_relation(this) %}
{% set sql = model['compiled_sql'] %}
{% set full_refresh_mode = flags.FULL_REFRESH %}
{% set should_log = kwargs.get('should_log', False) %}

{% if existing_relation is none %}
    {% set build_sql %}
        CREATE TABLE {{ target_relation }} AS (
            SELECT 
                * 
            FROM 
                ({{ sql }}) AS subquery
            WHERE 
                1 = 0
        )
    {% endset %}
    {% do run_query(build_sql) %}
    {% do log("Created initial empty table " ~ target_relation, info=True) %}
{% endif %}

{% if should_log %}
    {{ log("Executing SQL for model " ~ model.name, info=True) }}
{% endif %}

{% call statement('main') %}
    {{ sql }}
{% endcall %}

{{ return({'relations': [target_relation]}) }}

{% endmaterialization %}