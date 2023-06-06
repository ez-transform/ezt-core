{% macro source(source_name) -%}
    {% for table in sources -%}
        {% if table["name"] == source_name -%}
            {{ table["name"] }}
        {%- endif %}
    {%- endfor %}
{%- endmacro %}

{% macro model(model_name) -%}
    {% for table in models %}
        {% if table["name"] == model_name %}
            {{ table["name"] }}
        {% endif %}
    {% endfor %}
{%- endmacro %}