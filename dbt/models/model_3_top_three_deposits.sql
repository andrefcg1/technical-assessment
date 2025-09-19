-- Model 3: One row per player with their three largest deposit amounts

with player_deposits as (
    select 
        player_id,
        amount as deposit_amount,
        row_number() over (partition by player_id order by amount desc) as deposit_rank
    from {{ ref('transactions_extended') }}
    where type = 'Deposit'
),

top_three_deposits as (
    select 
        player_id,
        max(case when deposit_rank = 1 then deposit_amount end) as largest_deposit,
        max(case when deposit_rank = 2 then deposit_amount end) as second_largest_deposit,
        max(case when deposit_rank = 3 then deposit_amount end) as third_largest_deposit
    from player_deposits
    where deposit_rank <= 3
    group by player_id
)

select 
    player_id,
    largest_deposit,
    second_largest_deposit,
    third_largest_deposit
from top_three_deposits
order by player_id
