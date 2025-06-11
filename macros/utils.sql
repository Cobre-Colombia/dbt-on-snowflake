{% macro parse_json_column(column_name) %}
    try_cast({{ column_name }} as variant)
{% endmacro %}
