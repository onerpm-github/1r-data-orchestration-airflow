
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select distinct a.artist_id, upc, a.release_date, t.track_id, t.title  -- added distinct to it to eliminate duplicated rows
    from content.albums a 
    inner join content.projects p on a.album_id = p.product_id
    inner join content.tracks t on a.album_id = t.album_id 
    where p.product_type = 'album'
    and a.release_date between '2024-01-01' and (date_trunc('week', current_date) - interval '3 day') /* la ultima semana entre sab y vier */
    and p.admin_id is not null
    and p.admin_id <> 0
    and a.status_id in (4,7)
   
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
