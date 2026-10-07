select bodacc_announcement_id, valid_from, valid_to, is_current
from {{ source('lakehouse', 'bodacc_sirene_links') }}
where
    (is_current = true and valid_to is not null)
    or (is_current = false and valid_to is null)
