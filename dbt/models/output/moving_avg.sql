with temp as (
    select
        REGION,
        DATE,
        SERIES,
        VALUE
    from {{ ref('stg_eia') }}
),

with_ma as (
    select
        REGION,
        DATE,
        SERIES,
        VALUE,
        avg(VALUE) over (
            partition by REGION, SERIES
            order by DATE
            rows between 6 preceding and current row
        ) as value_ma_7
    from temp
)

select *
from with_ma
