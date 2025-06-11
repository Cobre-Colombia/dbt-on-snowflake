{% macro apply_filter_conditions(rules) %}
    case
    {% for rule in rules %}
        {% set conds = [] %}
        {% for k, v in rule['PROPERTY_FILTERS'].items() %}
            {% if k | lower != 'validation' %}
                {% set column = k | lower %}
                {% set val_list = v | map('lower') | join("','") %}
                {% do conds.append(column ~ " in ('" ~ val_list ~ "')") %}
            {% endif %}
        {% endfor %}
        when {{ conds | join(' and ') }} then '{{ rule["PRODUCT_NAME"] }}'
    {% endfor %}
    else null
    end
{% endmacro %}
