{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['id_conversation', 'ts_message'],
    on_schema_change = 'append_new_columns'
  )
}}

select
    msg_text as str_message,
    Interlocutor as str_interlocutor_name,
    msg_date as ts_message,
    conversation_id as id_conversation,
    current_timestamp() as ts_load
from {{ source('src_raw', 'tb_ext_conversations') }}
{% if is_incremental() %}
  where timestamp(msg_date) > (select max(msg_date) from {{ this }})
{% endif %} 