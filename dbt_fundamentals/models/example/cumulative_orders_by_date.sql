with orders as (
  SELECT *
  FROM {{ source('sample', 'orders') }}
)

select distinct
    o.o_orderdate as date
    , sum(o.o_totalprice) over (partition by date) as sales
    , sum(o.o_totalprice) over (order by date) as cumulative_sales
from orders o

{% if target.name == 'dev' %}
where year(o_orderdate) = 1996
{% endif %}

order by date
