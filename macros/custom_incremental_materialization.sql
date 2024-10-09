{% materialization custom_incremental_materialization, default %}

{% set target_relation = this.incorporate(type='table') %}
{% set sql = model['compiled_sql'] %}
{% set full_refresh_mode = flags.FULL_REFRESH %}
{% set should_log = kwargs.get('should_log', False) %}

{% if full_refresh_mode %}
    {% set relation_exists = adapter.get_relation(this.database, this.schema, this.identifier) is not none %}
    {% if relation_exists %}
        {% set truncate_sql %}
            TRUNCATE TABLE {{ target_relation }}
        {% endset %}
        {% do run_query(truncate_sql) %}
        {{ log("Table " ~ target_relation ~ " truncated due to --full-refresh", info=True) }}
    {% else %}
        {{ log("Table " ~ target_relation ~ " does not exist, skipping truncation", info=True) }}
    {% endif %}
{% endif %}

{% if should_log %}
    {{ log("Executing SQL for model " ~ model.name, info=True) }}
{% endif %}

{% call statement('main') %}
    {{ sql }}
{% endcall %}

{{ return({'relations': [target_relation]}) }}

{% endmaterialization %}