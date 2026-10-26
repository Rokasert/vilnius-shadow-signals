{{ config(tags=['google']) }}

-- Which probe categories found each place (a place can be both a restaurant and a bar).
select distinct place_id, unnest(categories) as category
from {{ ref('places') }}
