
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 
    -- stats_source
    s.source_id,
    s.store,
    s.product_id as track_id,
    (date_trunc('week', s.date_stat) + interval '4 day') as date_stat, 
    -- s.date_stat,
    sum(s.quantity) as streams,
    sum(s.skips) as skips,
    -- stats_play
    p.total_streams,
    -- detail
    csd.title as pL_title, 
    csd."owner",
    -- upc
    upc.artist_id,
    upc.title as track_title,
    upc.upc,
    upc.release_date

    from sales.stats_source s
    inner join {{ ref('stats_play_agg') }} as p on p.artist_id = s.artist_id and p.product_id = s.product_id
    inner join {{ ref('clean_source_detail') }} as csd on csd.source_id = s.source_id
    inner join {{ ref('upc') }} as upc on upc.artist_id = s.artist_id and upc.track_id = s.product_id
    where s.date_stat > '2024-01-01'
    /*and s.artist_id in 
        (select p.artist_id
        from sales.stats_source p
        where date_stat > '2024-01-01'
        group by 1
        order by sum(p.quantity) desc 
        limit 10 )*/
    group by 1,2,3,4,7,8,9,10,11,12,13

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
