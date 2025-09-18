{% snapshot player_kyc_status_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
  )
}}

select 
    id,
    affiliate_id,
    country_code,
    is_kyc_approved,
    created_at,
    updated_at
from {{ ref('players_extended') }}

{% endsnapshot %}
