{% macro calculate_revenue(pricing_struct, total_amount, match_name, transaction_count) %}
    {% set pricing = fromjson(pricing_struct) %}
    {% if pricing['pricingType'] == 'LINEAR' %}
        {% if pricing.get('isPricePercentage', False) %}
            {{ total_amount }} * ({{ pricing['pricePerUnit'] }} / 100)
        {% else %}
            {{ pricing['pricePerUnit'] }}
        {% endif %}
    {% elif pricing['pricingType'] == 'VOLUME' %}
        {% set tier = pricing['tiers'][0] %}
        {% if tier.get('isPricePercentage', False) %}
            ({{ total_amount }} * ({{ tier['price'] }} / 100)) + {{ tier.get('fee', 0) }}
        {% else %}
            {{ tier['price'] }} + {{ tier.get('fee', 0) }}
        {% endif %}
    {% else %}
        0
    {% endif %}
{% endmacro %}
