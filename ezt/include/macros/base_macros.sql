
{% macro source(source) -%}
    {% for table in sources -%}
        {% if table.name == source -%}
            {{ table.name }}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}