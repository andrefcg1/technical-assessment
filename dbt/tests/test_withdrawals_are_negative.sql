-- Simple test: Make sure withdrawals are always negative

select *
from {{ ref('model_1_daily_player_transactions') }}
where withdrawals > 0