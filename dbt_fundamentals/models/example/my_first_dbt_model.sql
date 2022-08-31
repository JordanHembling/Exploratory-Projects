{{ config(alias='first_model', tags=['nightly', 'example'])}}

with source_data as (

    select 1 as id, 'TX' as state, '2020-02-01 00:01:00.000'::timestamp as updated_at
    union all
    select 2 as id, 'CO' as state, '2020-01-01 00:00:00.000'::timestamp as updated_at
    union all
    select 3 as id, 'VT' as state, '2020-01-01 00:00:00.000'::timestamp as updated_at

)

select *
from source_data
--where id >= {{ var('my_third_variable') }}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
