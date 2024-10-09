-- Purpose: Macro to update the DBTExecutionLog table with the end date and time of the execution and the completion status.
-- Usage: This macro is used to update the DBTExecutionLog table with the end date and time of the execution and the completion status.
-- It is typically called at the end of a dbt run to log the completion of the process.
{% macro log_execution_end(execution_guid) %}
    {% if execute %}
        {% do log('Updating log entry. ExecutionGUID: ' ~ execution_guid, info=True) %}

        {% if execution_guid %}
            {% set update_statement %}
                UPDATE 
                    {{ source('logging', 'DBTExecutionLog') }}
                SET 
                    EndDateTime = SYSDATETIMEOFFSET(),
                    CompletionStatus = 'Success'
                WHERE 
                    ExecutionGUID = TRY_CAST('{{ execution_guid }}' AS UNIQUEIDENTIFIER);
                
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
            {% do log('Warning: No ExecutionGUID provided. Update to log table was not performed.', info=True) %}
        {% endif %}
    {% endif %}
{% endmacro %}