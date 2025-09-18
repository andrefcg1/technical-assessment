-- Simple test: Make sure we only count deposits from verified Discord players

with verified_discord_players as (
    select 
        p.id as player_id,
        p.country_code
    from {{ ref('players_extended') }} p
    left join {{ ref('affiliates_extended') }} a on p.affiliate_id = a.id
    where p.is_kyc_approved = true 
    and a.origin = 'Discord'
)

select 
    m.country_code,
    m.total_deposits_by_country
from {{ ref('model_2_kyc_discord_deposits') }} m
where not exists (
    select 1 
    from verified_discord_players vdp
    where vdp.country_code = m.country_code
)