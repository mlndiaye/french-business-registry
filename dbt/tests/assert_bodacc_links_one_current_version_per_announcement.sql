select bodacc_announcement_id, count(*) as current_count
from {{ source('lakehouse', 'bodacc_sirene_links') }}
where is_current = true
group by bodacc_announcement_id
having count(*) != 1
