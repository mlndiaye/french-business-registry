select bodacc_announcement_id, valid_from, count(*) as version_count
from {{ source('lakehouse', 'bodacc_sirene_links') }}
group by bodacc_announcement_id, valid_from
having count(*) > 1
