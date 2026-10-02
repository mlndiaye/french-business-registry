select siret, valid_from, count(*) as version_count
from {{ source('lakehouse', 'sirene_etablissements_historized') }}
group by siret, valid_from
having count(*) > 1
