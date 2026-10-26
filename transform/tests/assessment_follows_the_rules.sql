{{ config(tags=['google']) }}

-- The points are the sum of the signals, and every tier follows the stated rules.
select company_code, category, tier, score, tier_reason
from {{ ref('company_assessment') }}
where
    score <> 2 * staff_gap::int + staff_gap_severe::int + activity_outruns_size::int
    + low_declared_wage::int + accounts_missing::int + not_vat_registered::int
    or (tier = 'not assessed') <> (exclusion_reason is not null)
    or (tier in ('priority', 'review') and (not is_busy or score < 2))
    or (tier = 'priority' and (not staff_gap or score < 3))
    or (tier = 'none' and is_busy and score >= 2 and exclusion_reason is null)
    or tier_reason not like case tier
        when 'priority' then 'Priority:%'
        when 'review' then 'Review%'
        when 'none' then 'Not a lead:%'
        else 'Not scored:%'
    end
