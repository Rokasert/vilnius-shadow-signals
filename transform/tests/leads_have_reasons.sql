{{ config(tags=['google']) }}

-- Every lead must say why it is a lead.
select company_code, category
from {{ ref('leads') }}
where reasons is null or len(reasons) = 0
