{{ config(materialized='incremental', unique_key='t_time')}}


select *, to_time(concat(T_HOUR::varchar, ':', T_MINUTE, ':', T_SECOND)) as time from snowflake_sample_data.tpcds_sf10tcl.time_dim
where time <= current_time()

{% if is_incremental() %}
 and time > (select max(time) from {{ this }} )
{% endif %}
