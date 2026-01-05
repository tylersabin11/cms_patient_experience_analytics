select
    `Facility ID` as facility_id,
    `Facility Name` as facility_name,
    Address as facility_address,
    City_Town as facility_city,
    State as facility_state,
    `Zip Code` as facility_zip_code,
    `Telephone Number` as facility_phone_number,
    -- Care & cleanliness
    `Patients who reported that staff definitely gave care in a professional way and the facility was clean` as care_clean_definitely,
    `Patients who reported that staff somewhat gave care in a professional way or the facility was somewhat clean` as care_clean_somewhat,
    `Patients who reported that staff did not give care in a professional way or the facility was not clean` as care_clean_not,
    `Facilities and staff linear mean score` as care_clean_lms,
    `Patients who reported that staff definitely communicated about what to expect during and after the procedure` as comm_definitely,
    `Patients who reported that staff somewhat communicated about what to expect during and after the procedure` as comm_somewhat,
    `Patients who reported that staff did not communicate about what to expect during and after the procedure` as comm_not,
    `Communication about your procedure linear mean score` as comm_lms,
    `Patients who gave the facility a rating of 9 or 10 on a scale from 0 _lowest_ to 10 _highest_` as rating_9_10,
    `Patients who gave the facility a rating of 7 or 8 on a scale from 0 _lowest_ to 10 _highest_` as rating_7_8,
    `Patients who gave the facility a rating of 0 to 6 on a scale from 0 _lowest_ to 10 _highest_` as rating_0_6,
    `Patients' rating of the facility linear mean score` as rating_lms,
    `Patients who reported YES they would DEFINITELY recommend the facility to family or friends` as recommend_definitely,
    `Patients who reported PROBABLY YES they would recommend the facility to family or friends` as recommend_probably,
    `Patients who reported NO_ they would not recommend the facility to family or friends` as recommend_no,
    `Patients recommending the facility linear mean score` as recommend_lms,
    case when `Footnote` = 1 then 'The number of cases/patients is too few to report.'
        when `Footnote` = 3 then 'Results are based on a shorter time period than required'
        when `Footnote` = 6 then 'Fewer than 100 patients completed the CAHPS survey. Use these scores with caution, as the number of surveys may be too low to reliably assess facility performance.'
        when `Footnote` = 10 then 'Very few patients were eligible for the CAHPS survey. The scores shown reflect fewer than 50 completed surveys. Use these scores with caution, as the number of surveys may be too low to reliably assess facility performance.'
        when `Footnote` = 11 then 'There were discrepancies in the data collection process'
        else concat('Unknown Footnote Code: ', cast(`Footnote` as string)) end as survey_footnote,
    `Number of Sampled Patients` as num_sampled_patients,
    `Number of Completed Surveys` as num_completed_surveys,
    round(safe_divide(`Number of Completed Surveys`, `Number of Sampled Patients`), 4) as survey_completion_rate,
    `Start Date` as survey_start_date,
    `End Date` as survey_stop_date
from {{ source('my_datasets', 'raw_oas_cahps')}}