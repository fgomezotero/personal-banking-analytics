{% macro parse_amount(expr) -%}
  case
    when {{ expr }} is null then null
    when trim(cast({{ expr }} as string)) = '' then null
        when regexp_contains(cast({{ expr }} as string), r',')
          and regexp_contains(cast({{ expr }} as string), r'\.')
      then safe_cast(replace(cast({{ expr }} as string), ',', '') as float64)
    when regexp_contains(cast({{ expr }} as string), r',')
      then safe_cast(replace(cast({{ expr }} as string), ',', '.') as float64)
    else safe_cast(cast({{ expr }} as string) as float64)
  end
{%- endmacro %}
