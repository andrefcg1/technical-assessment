-- Model 2: Sum and count of deposits per player country for KYC approved players with Discord affiliate origin

with kyc_discord_players as (
    select 
        p.id as player_id,
        p.country_code,
        p.is_kyc_approved,
        a.origin as affiliate_origin
    from {{ ref('players_extended') }} p
    left join {{ ref('affiliates_extended') }} a on p.affiliate_id = a.id
    where p.is_kyc_approved = true 
    and a.origin = 'Discord'
),

player_deposits as (
    select 
        kdp.player_id,
        kdp.country_code,
        sum(t.amount) as total_deposits,
        count(t.amount) as deposit_count
    from kyc_discord_players kdp
    left join {{ ref('transactions_extended') }} t 
        on kdp.player_id = t.player_id 
        and t.type = 'Deposit'
    group by kdp.player_id, kdp.country_code
)

select 
    country_code,
    sum(total_deposits) as total_deposits_by_country,
    sum(deposit_count) as total_deposit_count_by_country
from player_deposits
group by country_code
order by total_deposits_by_country desc
