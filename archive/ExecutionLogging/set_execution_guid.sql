{% macro set_execution_guid() %}
    {% if execute %}
        {% set start_result = log_execution_start() %}
        {% do context.update({'execution_guid': start_result.execution_guid}) %}
    {% endif %}
{% endmacro %}