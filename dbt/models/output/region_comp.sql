{{ config(
    materialized='table'
) }}

with temp as (
    select
        REGION,
        DATE,
        SERIES,
        VALUE
    from {{ ref('stg_eia') }}
),

stats as (
    select
        DATE,
        SERIES,
        min(VALUE) as value_min,
        max(VALUE) as value_max,
        avg(VALUE) as value_avg
    from temp
    group by DATE, SERIES
),

ranked as (
    select
        b.REGION,
        b.DATE,
        b.SERIES,
        b.VALUE,
        rank() over (
            partition by b.DATE, b.SERIES
            order by b.VALUE desc
        ) as value_rank_desc
    from temp b
)

select
    r.REGION,
    r.DATE,
    r.SERIES,
    r.VALUE,
    r.value_rank_desc,
    s.value_min,
    s.value_max,
    s.value_avg
from ranked r
join stats s
  on r.DATE  = s.DATE
 and r.SERIES = s.SERIES
