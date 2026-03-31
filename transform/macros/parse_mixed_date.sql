{% macro parse_mixed_date(expr) -%}
  case
    when {{ expr }} is null then null
    when trim(cast({{ expr }} as string)) = '' then null
    when regexp_contains(cast({{ expr }} as string), r'^\d{4}-\d{2}-\d{2}$')
      then safe.parse_date('%Y-%m-%d', cast({{ expr }} as string))
    when regexp_contains(cast({{ expr }} as string), r'^\d{2}/\d{2}/\d{4}$')
      then safe.parse_date('%d/%m/%Y', cast({{ expr }} as string))
    else null
  end
{%- endmacro %}
