with base as (

    select
        company_partner_group_id,
        company_listing_id,
        partner_listing_uuid,
        partner_portfolio_name,
        partner_group_uuid,
        application_group_id,
        is_submitted,
        created_ts,
        created_date,
        assigned_ts,
        cycle_time_hours,
        workflow_status,
        assignee_name,
        group_decision,

        -- raw values (will be overridden)
        denial_reason as raw_denial_reason,
        cancelled_reason as raw_cancelled_reason,

        is_likely_fraud,
        company_portfolio_name,
        market_name,
        pms_unit_id,
        company_unit_id,
        company_property_id,
        region_name,
        owner_category,
        owner_subcategory,
        property_owner_type,
        property_division_type,
        property_address,
        unit_name,

        submitted_ts,
        submitted_date,
        submitted_week,
        submitted_dow_num,
        submitted_dow_name,
        submitted_week_num,
        submitted_iso_week_num,
        submitted_month,
        submitted_month_name,
        submitted_month_num,
        submitted_quarter,
        submitted_year,
        submitted_iso_year,

        pending_review_ts,
        pending_review_date,
        pending_review_week,
        pending_review_dow_num,
        pending_review_dow_name,
        pending_review_week_num,
        pending_review_iso_week_num,
        pending_review_month,
        pending_review_month_name,
        pending_review_month_num,
        pending_review_quarter,
        pending_review_year,
        pending_review_iso_year,

        decision_ts,
        decision_date,
        decision_week,
        decision_dow_num,
        decision_dow_name,
        decision_week_num,
        decision_iso_week_num,
        decision_month,
        decision_month_name,
        decision_month_num,
        decision_quarter,
        decision_year,
        decision_iso_year,

        -- deterministic hash for demo logic
        mod(abs(farm_fingerprint(cast(application_group_id as string))), 100) as hash_bucket

    from {{ source('my_datasets', 'raw_application_data') }}

)

select
    company_partner_group_id,
    company_listing_id,
    partner_listing_uuid,
    partner_portfolio_name,
    partner_group_uuid,
    application_group_id,
    is_submitted,
    created_ts,
    created_date,
    assigned_ts,
    cycle_time_hours,
    workflow_status,
    assignee_name,
    group_decision,

    case
        when group_decision = 'Declined' then
            case
                when hash_bucket < 15 then 'Credit'
                when hash_bucket < 30 then 'Income'
                when hash_bucket < 45 then 'Eviction'
                when hash_bucket < 60 then 'Rental Debt'
                when hash_bucket < 75 then 'Criminal Record'
                when hash_bucket < 85 then 'Bankruptcy'
                when hash_bucket < 95 then 'Fraud'
                else 'Prohibited Breed'
            end
        else null
    end as denial_reason,

    case
        when group_decision = 'Cancelled' then
            case
                when hash_bucket < 30 then 'No Longer Interested'
                when hash_bucket < 55 then 'Property No Longer Available'
                when hash_bucket < 80 then 'Unresponsive'
                else 'Other'
            end
        else null
    end as cancelled_reason,

    is_likely_fraud,
    company_portfolio_name,
    market_name,
    pms_unit_id,
    company_unit_id,
    company_property_id,
    region_name,
    owner_category,
    owner_subcategory,
    property_owner_type,
    property_division_type,
    property_address,
    unit_name,

    submitted_ts,
    submitted_date,
    submitted_week,
    submitted_dow_num,
    submitted_dow_name,
    submitted_week_num,
    submitted_iso_week_num,
    submitted_month,
    submitted_month_name,
    submitted_month_num,
    submitted_quarter,
    submitted_year,
    submitted_iso_year,

    pending_review_ts,
    pending_review_date,
    pending_review_week,
    pending_review_dow_num,
    pending_review_dow_name,
    pending_review_week_num,
    pending_review_iso_week_num,
    pending_review_month,
    pending_review_month_name,
    pending_review_month_num,
    pending_review_quarter,
    pending_review_year,
    pending_review_iso_year,

    decision_ts,
    decision_date,
    decision_week,
    decision_dow_num,
    decision_dow_name,
    decision_week_num,
    decision_iso_week_num,
    decision_month,
    decision_month_name,
    decision_month_num,
    decision_quarter,
    decision_year,
    decision_iso_year

from base