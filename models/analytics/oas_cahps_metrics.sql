select

    -- Facility information
    ocs.facility_id,
    ocs.facility_name,
    ocs.facility_address,
    ocs.facility_city,
    ocs.facility_state,
    ocs.facility_zip_code,
    case when ocs.facility_phone_number = '(000) 000-0000' then 'Unknown' 
        else ocs.facility_phone_number end as facility_phone_number,

    -- Care & Clean quality
    ocs.care_clean_definitely,
    ocs.care_clean_somewhat,
    ocs.care_clean_not,
    ocs.care_clean_lms,
    fm.avg_care_clean_lms as state_avg_care_clean_lms,

    -- Communication metrics
    ocs.comm_definitely,
    ocs.comm_somewhat,
    ocs.comm_not,
    ocs.comm_lms,
    fm.avg_comm_lms as state_avg_comm_lms,

    -- Rating for facility
    ocs.rating_9_10,
    ocs.rating_7_8,
    ocs.rating_0_6,
    ocs.rating_lms,
    fm.avg_rating_lms as state_avg_rating_lms,

    -- Recommend likeliness
    ocs.recommend_definitely,
    ocs.recommend_probably,
    ocs.recommend_no,
    ocs.recommend_lms,
    fm.avg_recommend_lms as state_avg_recommends_lms,

    -- Survey totals
    ocs.num_sampled_patients,
    fm.total_sampled_patients as state_total_sampled_patients,
    ocs.num_completed_surveys,
    fm.total_completed_surveys as state_total_completed_surveys,
    round(safe_divide(ocs.num_completed_surveys, ocs.num_sampled_patients), 4) as survey_completion_rate,
    fm.avg_survey_completion_rate as state_avg_survey_completion_rate,

    -- Survey details
    ocs.footnote_code,
    ocs.survey_start_date,
    ocs.survey_stop_date

from {{ ref('stg_oas_cahps_survey') }} as ocs 
left join {{ ref('int_facility_metrics') }} as fm 
on ocs.facility_state = fm.facility_state
where (ocs.footnote_code != 1 or ocs.footnote_code is null)