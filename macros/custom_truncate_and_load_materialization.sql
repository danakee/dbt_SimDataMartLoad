{% materialization custom_truncate_and_load, default %}
    {%- set target_relation = this.incorporate(type='table') -%}

    -- Execute the model's SQL (which will call load_stage_table)
    {% call statement('main') %}
        {{ sql }}
    {% endcall %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}