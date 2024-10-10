-- macros/log_run_parameters.sql
{% macro log_run_parameters() %}
    {% set run_started_at_str = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}
    {% set params = [] %}

    -- Capture variables passed via --vars
    {% set vars_dict = fromjson(env_var('DBT_VARS', '{}')) %}
    {% for key, value in vars_dict.items() %}
        {% do params.append({'name': key, 'value': value}) %}
    {% endfor %}

    -- Capture flags
    {% set flag_attrs = ['FULL_REFRESH', 'STORE_FAILURES', 'WHICH', 'PROFILES_DIR', 'DEBUG', 'INDIRECT_SELECTION'] %}
    {% for attr in flag_attrs %}
        {% if attr in flags %}
            {% do params.append({'name': attr, 'value': flags[attr]}) %}
        {% endif %}
    {% endfor %}

    -- Capture selected models
    {% if selected_resources is defined %}
        {% set model_names = [] %}
        {% for resource in selected_resources %}
            {% if resource.startswith('model.') %}
                {% do model_names.append(resource.split('.')[-1]) %}
            {% endif %}
        {% endfor %}
        {% do params.append({'name': 'SELECTED_MODELS', 'value': model_names | join(', ')}) %}
    {% endif %}

    -- Build and execute insert statements
    {% for param in params %}
        {% call statement('insert_param') %}
            INSERT INTO [SimulationsAnalyticsLogging].[dbo].[DBTExecutionParameterLog] (
                InvocationGUID, 
                ParameterName, 
                ParameterValue, 
                StartDateTime
            )
            VALUES (
                '{{ invocation_id }}',
                '{{ param.name }}',
                '{{ param.value | replace("''", "''''") }}',
                '{{ run_started_at_str }}'
            );
        {% endcall %}
    {% endfor %}
{% endmacro %}