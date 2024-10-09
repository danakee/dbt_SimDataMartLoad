{% macro sqlserver__get_merge_sql(target, source, unique_key, dest_columns, predicates=None) %}
    {# Extract column names from dest_columns #}
    {% set dest_columns = dest_columns | map(attribute='column') | list %}

    {# Exclude specific columns #}
    {% set excluded_columns = ['ProjectKey', 'EDWCreatedDatetime', 'EDWLastUpdatedDateTime'] %}
    {% set dest_columns = dest_columns | reject('in', excluded_columns) %}

    {# Convert dest_columns to a comma-separated string #}
    {% set dest_columns_csv = dest_columns | join(', ') %}

    {# Ensure unique_key is a list #}
    {% if unique_key is string %}
        {% set unique_key = [unique_key] %}
    {% endif %}

    {# Handle predicates #}
    {% if predicates %}
        {% if predicates is string %}
            {% set predicates_sql = ' AND ' ~ predicates %}
        {% else %}
            {% set predicates_sql = ' AND ' ~ predicates | join(' AND ') %}
        {% endif %}
    {% else %}
        {% set predicates_sql = '' %}
    {% endif %}

    {# Build merge condition #}
    {% set merge_on = [] %}
    {% for uk in unique_key %}
        {% do merge_on.append('target.' ~ uk ~ ' = source.' ~ uk) %}
    {% endfor %}

    {# Prepare updates, excluding unique keys #}
    {% set update_columns = dest_columns | reject('in', unique_key) %}
    {% set updates = [] %}
    {% for col in update_columns %}
        {% do updates.append('target.' ~ col ~ ' = source.' ~ col) %}
    {% endfor %}
    {% set updates_sql = updates | join(',\n    ') %}

    MERGE {{ target }} AS target
    USING {{ source }} AS source
    ON {{ merge_on | join(' AND ') }}{{ predicates_sql }}
    {% if updates_sql %}
    WHEN MATCHED THEN
        UPDATE SET
            {{ updates_sql }}
    {% endif %}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            {{ dest_columns_csv }}
        )
        VALUES (
            {% for col in dest_columns %}
                source.{{ col }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        );
{% endmacro %}
