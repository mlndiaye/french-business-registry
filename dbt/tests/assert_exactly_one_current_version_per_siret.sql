select siret, count(*) as current_count
from {{ source('lakehouse', 'sirene_etablissements_historized') }}
where is_current = true
group by siret
having count(*) != 1
