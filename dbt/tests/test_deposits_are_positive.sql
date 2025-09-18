-- Simple test: Make sure deposits are never negative

select *
from {{ ref('model_1_daily_player_transactions') }}
where deposits < 0