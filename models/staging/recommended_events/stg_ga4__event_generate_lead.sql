{{
  config(
      enabled = false,
  )
}}

with generate_lead_with_params as (
    select *,
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value') }}
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("generate_lead_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("generate_lead_custom_parameters") )}}
    {% endif %}
    from {{ref('stg_ga4__events')}}
    where event_name = 'generate_lead'
    {% if is_incremental() %} 
      and event_date_dt >= date_sub(current_date(), interval {{ var('static_incremental_days', 3) }} day)
    {% endif %}
)

select * from generate_lead_with_params