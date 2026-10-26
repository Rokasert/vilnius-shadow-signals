{{ config(tags=['google']) }}

-- One row per Google place in the newest Places pull, flattened from the raw API responses.
-- Fields are read from JSON so a field Google omits everywhere doesn't break the model.
with hits as (
    select
        category,
        _run_id as source_run_id,
        unnest(from_json(json_extract(to_json(response), '$.places'), '["JSON"]')) as place
    from {{ source('bronze', 'google__places') }}
),

flat as (
    select
        place ->> '$.id' as place_id,
        category,
        source_run_id,
        place ->> '$.displayName.text' as name,
        place ->> '$.formattedAddress' as formatted_address,
        place ->> '$.shortFormattedAddress' as short_address,
        try_cast(place ->> '$.location.latitude' as double) as latitude,
        try_cast(place ->> '$.location.longitude' as double) as longitude,
        place ->> '$.primaryType' as primary_type,
        from_json(place -> '$.types', '["VARCHAR"]') as types,
        place ->> '$.businessStatus' as business_status,
        place ->> '$.googleMapsUri' as google_maps_uri,
        place ->> '$.websiteUri' as website,
        place ->> '$.nationalPhoneNumber' as phone,
        try_cast(place ->> '$.rating' as double) as rating,
        try_cast(place ->> '$.userRatingCount' as integer) as review_count,
        place ->> '$.priceLevel' as price_level,
        place -> '$.regularOpeningHours.periods' as opening_periods
    from hits
),

places as (
    select
        place_id,
        any_value(name) as name,
        any_value(formatted_address) as formatted_address,
        any_value(short_address) as short_address,
        any_value(latitude) as latitude,
        any_value(longitude) as longitude,
        any_value(primary_type) as primary_type,
        any_value(types) as types,
        list(distinct category order by category) as categories,
        any_value(business_status) as business_status,
        any_value(google_maps_uri) as google_maps_uri,
        any_value(website) as website,
        any_value(phone) as phone,
        any_value(rating) as rating,
        any_value(review_count) as review_count,
        any_value(price_level) as price_level,
        any_value(opening_periods) as opening_periods,
        any_value(source_run_id) as source_run_id
    from flat
    where place_id is not null
    group by place_id
),

-- Opening periods: day 0 = Sunday; a period without a close time means open 24/7.
periods as (
    select
        place_id,
        unnest(from_json(
            opening_periods,
            '[{"open": {"day": "INTEGER", "hour": "INTEGER", "minute": "INTEGER"},
               "close": {"day": "INTEGER", "hour": "INTEGER", "minute": "INTEGER"}}]'
        )) as period
    from places
    where opening_periods is not null
),

hours as (
    select
        place_id,
        least(168, sum(
            case
                when period.close is null then 10080
                else (
                    period.close.day * 1440 + period.close.hour * 60 + period.close.minute
                    - (period.open.day * 1440 + period.open.hour * 60 + period.open.minute)
                    + 10080
                ) % 10080
            end
        ) / 60.0) as weekly_open_hours,
        case
            when bool_or(period.close is null) then 7 else count(distinct period.open.day)
        end as open_days,
        bool_or(period.close is null or period.open.day in (0, 6)) as opens_weekend,
        bool_or(period.close is null) as is_open_24_7,
        bool_or(
            period.close is null
            or period.close.day <> period.open.day
            or period.close.hour >= 22
        ) as closes_late
    from periods
    group by place_id
),

enriched as (
    select
        p.place_id,
        p.name,
        {{ name_key('p.name') }} as name_key,
        list_filter(
            string_split(regexp_replace(lower(strip_accents(p.name)), '[^a-z0-9]+', ' ', 'g'), ' '),
            lambda x: x <> ''
        ) as name_tokens,
        p.formatted_address,
        p.short_address,
        p.latitude,
        p.longitude,
        coalesce(regexp_matches(p.formatted_address, '\bVilnius\b'), false) as is_in_vilnius,
        {{ street_key_from_address('p.formatted_address') }} as street_key,
        {{ house_key_from_address('p.formatted_address') }} as house_key,
        p.primary_type,
        p.types,
        p.categories,
        p.business_status,
        coalesce(p.business_status = 'OPERATIONAL', false) as is_operational,
        p.google_maps_uri,
        p.website,
        lower(regexp_extract(p.website, '^https?://(?:www\.)?([^/:?#]+)', 1)) as website_host,
        p.phone,
        p.rating,
        coalesce(p.review_count, 0) as review_count,
        p.price_level,
        h.weekly_open_hours,
        h.open_days,
        h.opens_weekend,
        h.is_open_24_7,
        h.closes_late,
        p.source_run_id
    from places as p
    left join hours as h on p.place_id = h.place_id
),

words as (
    select
        list(word) as generic,
        list(word) filter (where kind in ('location', 'legal', 'common')) as non_brand
    from {{ ref('generic_name_words') }}
),

-- A website shared by several places that have no name word in common is a platform
-- (booking site, directory, social network), not the business's own site.
host_tokens as (
    select e.website_host, e.place_id, unnest(list_distinct(list_filter(
        e.name_tokens, lambda x: length(x) >= 3 and not list_contains(w.non_brand, x)
    ))) as token
    from enriched as e
    cross join words as w
    where e.website_host is not null
),

shared_hosts as (
    select
        e.website_host,
        count(distinct e.place_id) as places,
        (
            select max(n) from (
                select count(distinct t.place_id) as n
                from host_tokens as t
                where t.website_host = e.website_host
                group by t.token
            )
        ) as places_with_top_token
    from enriched as e
    where e.website_host is not null
    group by e.website_host
    having count(distinct e.place_id) >= 3
),

platform_hosts as (
    select website_host
    from shared_hosts
    where coalesce(places_with_top_token, 0) < 0.6 * places
    union
    select e.website_host
    from enriched as e
    inner join {{ ref('platform_hosts') }} as h
        on e.website_host = h.host or e.website_host like '%.' || h.host
)

select
    e.*,
    -- the name without generic words such as "kirpykla" or "Vilniuje", e.g. 'Kirpykla Ona' -> 'ona'
    array_to_string(list_sort(list_filter(
        e.name_tokens, lambda x: not list_contains(w.generic, x)
    )), '') as brand_key,
    ph.website_host is not null as is_platform_website,
    -- a directory link (rekvizitai.vz.lt/imone/<company>/) names the company in its address
    nullif(regexp_replace(
        regexp_replace(
            lower(strip_accents(regexp_extract(
                e.website, '(?:rekvizitai\.vz\.lt/imone|118\.lt/imones)/([^/?#]+)', 1
            ))),
            '(^|_)(uab|mb|ii|vsi|ab)(_|$)',
            '_',
            'g'
        ),
        '[^a-z0-9]+',
        '',
        'g'
    ), '') as directory_name_key
from enriched as e
cross join words as w
left join platform_hosts as ph on e.website_host = ph.website_host
