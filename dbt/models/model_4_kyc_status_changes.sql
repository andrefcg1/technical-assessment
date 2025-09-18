-- Model 4: Analysis of KYC status changes over time

with kyc_changes as (
    select 
        id as player_id,
        is_kyc_approved,
        dbt_valid_from as change_date,
        dbt_valid_to as change_end_date,
        lag(is_kyc_approved) over (partition by id order by dbt_valid_from) as previous_kyc_status,
        case 
            when lag(is_kyc_approved) over (partition by id order by dbt_valid_from) = false 
                 and is_kyc_approved = true 
            then 'approved'
            when lag(is_kyc_approved) over (partition by id order by dbt_valid_from) = true 
                 and is_kyc_approved = false 
            then 'rejected'
            else 'no_change'
        end as status_change_type
    from {{ ref('player_kyc_status_snapshot') }}
    where dbt_valid_from is not null
),

kyc_approvals as (
    select 
        kc.player_id,
        kc.change_date as approval_date,
        datediff('day', 
            p.created_at::timestamp,
            kc.change_date::timestamp
        ) as days_to_approval
    from kyc_changes kc
    left join {{ ref('players_extended') }} p on kc.player_id = p.id
    where kc.status_change_type = 'approved'
),

player_summary as (
    select 
        p.id as player_id,
        p.country_code,
        p.created_at,
        ka.approval_date,
        ka.days_to_approval,
        case 
            when ka.approval_date is not null then true
            else false
        end as was_ever_approved,
        p.is_kyc_approved as currently_approved
    from {{ ref('players_extended') }} p
    left join kyc_approvals ka on p.id = ka.player_id
)

select 
    country_code,
    count(*) as total_players,
    count(case when was_ever_approved then 1 end) as players_ever_approved,
    count(case when currently_approved then 1 end) as players_currently_approved,
    round(
        count(case when was_ever_approved then 1 end) * 100.0 / count(*), 2
    ) as approval_rate_percentage,
    round(avg(case when days_to_approval is not null then days_to_approval end), 1) as avg_days_to_approval
from player_summary
group by country_code
order by approval_rate_percentage desc
