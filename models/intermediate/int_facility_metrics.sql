with avg_metrics as (
    select
        facility_state,
        survey_start_date,
        survey_stop_date,

        round(avg(care_clean_lms),0) as avg_care_clean_lms,
        round(avg(comm_lms),0) as avg_comm_lms,
        round(avg(rating_lms),0) as avg_rating_lms,
        round(avg(recommend_lms),0) as avg_recommend_lms,

        sum(num_completed_surveys) as total_completed_surveys,
        sum(num_sampled_patients) as total_sampled_patients,

        round(safe_divide(sum(num_completed_surveys), sum(num_sampled_patients)), 4) as avg_survey_completion_rate

    from {{ ref('stg_oas_cahps_survey') }}
    group by 1, 2, 3
),

state_metrics as (
    select
        *,
        row_number() over (partition by facility_state order by total_completed_surveys desc) as row_num
    from avg_metrics
)

select
    facility_state,
    avg_care_clean_lms,
    avg_comm_lms,
    avg_rating_lms,
    avg_recommend_lms,
    total_completed_surveys,
    total_sampled_patients,
    avg_survey_completion_rate
from
    state_metrics
where row_num = 1
order by 1 asc