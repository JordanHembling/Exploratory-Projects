with sample_customer as (
  SELECT *
  FROM {{ source('sample', 'customer') }}
),

sample_orders as (
  SELECT *
  FROM {{ source('sample', 'orders') }}
)

select
    c.c_custkey
    , c.c_name
    , c.c_nationkey as nation
    , sum(o.o_totalprice) as total_order_price
from sample_customer c
left join sample_orders o on c.c_custkey = o.o_custkey

{{group_by(3)}}
