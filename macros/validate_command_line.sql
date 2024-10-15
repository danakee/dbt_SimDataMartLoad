{% macro validate_command_line() %}

    {% if execute %}
        {% set command_line = env_var('DBT_COMMAND_LINE', '') %}
        {% set execution_timestamp = env_var('DBT_EXECUTION_TIMESTAMP', '') %}
        {% set current_timestamp = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') %}
        
        {{ log("Debug - Command Line from env: " ~ command_line, info=True) }}
        {{ log("Debug - Execution Timestamp: " ~ execution_timestamp, info=True) }}
        {{ log("Debug - Current Timestamp: " ~ current_timestamp, info=True) }}

        {% if not command_line %}
            {{ exceptions.raise_compiler_error("DBT_COMMAND_LINE environment variable is not set. Please run dbt using the provided wrapper script.") }}
        {% elif not execution_timestamp %}
            {{ exceptions.raise_compiler_error("DBT_EXECUTION_TIMESTAMP environment variable is not set. Please run dbt using the provided wrapper script.") }}
        {% else %}
            {% set time_difference = modules.datetime.datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S') - modules.datetime.datetime.strptime(execution_timestamp, '%Y-%m-%d %H:%M:%S') %}
            {% set time_difference_seconds = time_difference.total_seconds() %}
            
            {{ log("Debug - Time Difference (seconds): " ~ time_difference_seconds, info=True) }}
            
            {% if time_difference_seconds > 30 %}  {# 30 seconds #}
                {{ exceptions.raise_compiler_error("DBT_EXECUTION_TIMESTAMP is stale. Please rerun dbt using the wrapper script. Time difference: " ~ time_difference_seconds ~ " seconds") }}
            {% endif %}
        {% endif %}
    {% else %}
        {{ log("Skipping command line validation during parse/compile phase", info=True) }}
    {% endif %}
    
{% endmacro %}