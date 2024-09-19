
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select p.artist_id, p.product_id, sum(p.quantity) as total_streams 
    from sales.stats_play p
    where date_stat > '2024-01-01'
    /*and p.artist_id in 
        (select p.artist_id
        from sales.stats_source p
        where date_stat > '2024-01-01'
        group by 1
        order by sum(p.quantity) desc 
        limit 10 )*/
    group by 1, 2
    having sum(quantity)>100
   
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
