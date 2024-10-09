-- macros/log_execution_end.sql
-- Purpose: Macro to update the DBTExecutionLog table with the end date and time of the execution and the completion status.
-- Usage: This macro is used to update the DBTExecutionLog table with the end date and time of the execution and the completion status.
-- It is typically called at the end of a dbt run to log the completion of the process.
{% macro log_execution_end() %}
    {% if execute %}
        {% if invocation_id %}
            {% set update_statement %}
                UPDATE 
                    {{ source('logging', 'DBTExecutionLog') }}
                SET 
                    EndDateTime = SYSDATETIMEOFFSET(),
                    CompletionStatus = 'Success'
                WHERE 
                    InvocationGUID = TRY_CAST('{{ invocation_id }}' AS UNIQUEIDENTIFIER);
                
                SELECT @@ROWCOUNT AS RowsAffected;
            {% endset %}

            {% set results = run_query(update_statement) %}

            {% if results %}
                {% set rows_affected = results.columns[0].values()[0] %}
                {% do log('Rows affected: ' ~ rows_affected, info=True) %}
            {% else %}
                {% do log('No results returned from update query.', info=True) %}
            {% endif %}
        {% else %}
            {% do log('Warning: No InvocationGUID provided. Update to log table was not performed.', info=True) %}
        {% endif %}
    {% endif %}
{% endmacro %}