-- Stay mindful of performance/cost when using this model.
-- Making this model partitioned on date is not possible because there's no way to create a single record 
-- per session AND partition on date. 


{{
    config(
        materialized = 'incremental',
        unique_key = 'session_key',
        partition_by={
            "field": "session_start_date",
            "data_type": "date"
        },
        tags = ["incremental"]
    )
}}

{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days', 3)) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}

select
    client_key,
    session_key,
    stream_id,
    max(user_id) as user_id,
    min(session_partition_min_timestamp) as session_start_timestamp,
    min(session_partition_date) as session_start_date,
    sum(session_partition_count_page_views) as count_pageviews,
    sum(session_partition_sum_event_value_in_usd) as sum_event_value_in_usd,
    max(session_partition_max_session_engaged) as is_session_engaged,
    sum(session_partition_sum_engagement_time_msec) as sum_engaged_time_msec,
    min(session_number) as session_number
    {% if var('conversion_events', false) %}
        {% for ce in var('conversion_events',[]) %}
            , sum({{ce}}_count) as count_{{ce}}
        {% endfor %}
    {% endif %}
from {{ref('fct_ga4__sessions_daily')}}

{% if is_incremental() %}
    where session_partition_date in ({{ partitions_to_replace | join(',') }})
{% endif %}

group by 1,2,3

