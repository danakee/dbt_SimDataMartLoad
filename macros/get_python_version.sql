{% macro get_python_version() %}
  {{ return("'" ~ var('python_version') ~ "'") }}
{% endmacro %}