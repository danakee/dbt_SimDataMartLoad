-- Overwrite for not_null.sql
{% macro test_not_null(model, column_name)%}
    -- Custom not_null test macro
    SELECT
        {{ column_name }}
    FROM
        {{ model }}
    WHERE
        {{ column_name }} is null
{% endmacro %}