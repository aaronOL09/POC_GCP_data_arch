
select
    percentile_cont(num_seconds_response ,0.5) over() as median
from {{ ref('tb_sl_conversations') }}
where str_interlocutor_name = 'Agent'
    and ts_previous_message is not null
limit 1