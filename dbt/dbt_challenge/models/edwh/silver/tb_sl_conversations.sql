{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['id_conversation', 'ts_message'],
    on_schema_change = 'append_new_columns'
  )
}}

select
    str_message,
    str_interlocutor_name,
    ts_message,
    id_conversation,
    lag(ts_message) over (partition by id_conversation order by id_conversation, ts_message asc) as ts_previous_message,
    timestamp_diff(
        ts_message,
        lag(ts_message) over (partition by id_conversation order by id_conversation, ts_message asc),
        second
    ) as num_seconds_response,
    current_timestamp() as ts_load
from {{ ref('tb_br_conversations') }}
{% if is_incremental() %}
  where ts_message > (select max(ts_message) from {{ this }})
{% endif %} 
order by id_conversation, ts_message asc