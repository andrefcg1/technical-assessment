-- Model 1: One row per player daily with deposits and withdrawals columns
-- Withdrawals should be negative

with daily_transactions as (
    select 
        player_id,
        date(timestamp) as transaction_date,
        sum(case when type = 'Deposit' then amount else 0 end) as deposits,
        sum(case when type = 'Withdraw' then -amount else 0 end) as withdrawals
    from {{ ref('transactions_extended') }}
    group by player_id, date(timestamp)
)

select 
    player_id,
    transaction_date,
    deposits,
    withdrawals
from daily_transactions
order by player_id, transaction_date
