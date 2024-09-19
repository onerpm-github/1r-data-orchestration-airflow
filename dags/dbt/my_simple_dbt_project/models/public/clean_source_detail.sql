
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    with source_detail as (
    select source_id,
    title, 
    "owner",
    row_number() over(partition by source_id order by date desc) as row_n
    from sales.stats_source_detail
    )

    select source_id
    , title
    , "owner"
    from source_detail t 
    where row_n = 1

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
