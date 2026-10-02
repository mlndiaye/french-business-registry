select siret, valid_from, valid_to, is_current
from {{ source('lakehouse', 'sirene_etablissements_historized') }}
where
    (is_current = true and valid_to is not null)
    or (is_current = false and valid_to is null)
