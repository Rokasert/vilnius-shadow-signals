{# Lower-case, strip Lithuanian diacritics and anything that is not a letter or digit. #}
{% macro match_key(expression) -%}
    regexp_replace(lower(strip_accents(coalesce({{ expression }}, ''))), '[^a-z0-9]+', '', 'g')
{%- endmacro %}


{# match_key without legal-form words, so 'UAB "Kava"' and 'Uždaroji akcinė bendrovė „Kava“' agree. #}
{% macro name_key(expression) -%}
    regexp_replace(
        regexp_replace(
            lower(strip_accents(coalesce({{ expression }}, ''))),
            '\b(uzdaroji akcine bendrove|akcine bendrove|mazoji bendrija|individuali imone|viesoji istaiga|uab|ab|mb|ii|vsi)\b',
            ' ',
            'g'
        ),
        '[^a-z0-9]+',
        '',
        'g'
    )
{%- endmacro %}


{# House key: the building number with its letter, without premises ('22A-5' -> '22a'). #}
{% macro house_key(expression) -%}
    lower(regexp_extract(replace(coalesce({{ expression }}, ''), ' ', ''), '^(\d+[A-Za-z]?)', 1))
{%- endmacro %}


{# Street segment of a free-text address: the first comma-separated part that starts with a
   letter and holds a number, skipping postcodes ('Ozo g. 25, 07150 Vilnius' -> 'Ozo g. 25'). #}
{% macro address_street_part(expression) -%}
    trim(coalesce(list_filter(
        string_split(coalesce({{ expression }}, ''), ','),
        lambda x: regexp_matches(trim(x), '^[^\d]')
            and regexp_matches(x, '\d')
            and not regexp_matches(trim(x), '^(LT-?)?\d{5}')
    )[1], ''))
{%- endmacro %}


{# Street key of a free-text address: the street name without its type ('g.', 'pr.', ...),
   comparable with match_key() of the address register's street name. #}
{% macro street_key_from_address(expression) -%}
    regexp_replace(
        regexp_replace(
            lower(strip_accents(regexp_replace({{ address_street_part(expression) }}, '\s*\d.*$', ''))),
            '\s+(g|pr|al|pl|skg|tak|kel|krant|aklg|a|skv|per)\.?$',
            ''
        ),
        '[^a-z0-9]+',
        '',
        'g'
    )
{%- endmacro %}


{# House key of a free-text address: the last number in the street segment ('Gedimino pr. 9-12' -> '9'). #}
{% macro house_key_from_address(expression) -%}
    lower(regexp_extract(
        replace(
            regexp_extract(
                {{ address_street_part(expression) }},
                '(\d+\s?[A-Za-z]?)(?:\s*[-/][0-9A-Za-z]+)?\s*$',
                1
            ),
            ' ',
            ''
        ),
        '^(\d+[A-Za-z]?)',
        1
    ))
{%- endmacro %}
