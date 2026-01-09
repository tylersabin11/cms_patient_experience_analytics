# OAS CAHPS Analytics

This project models and analyzes **Outpatient and Ambulatory Surgery CAHPS (OAS CAHPS)** survey data using **dbt** and **BigQuery**, with the goal of producing analytics-ready datasets for reporting and BI tools like Looker.

The focus is on transforming raw CMS-style survey data with complex column naming, suppression rules, and footnote metadata into clean, well-documented models that can be safely used for analysis.

---

## Project Goals

- Clean and standardize raw OAS CAHPS survey data
- Handle CMS footnote logic and suppression rules correctly
- Create reliable, analytics-ready tables for facility-level reporting
- Serve as a portfolio example of healthcare-focused analytics engineering work

---

## Tech Stack

- **dbt**
- **BigQuery**
- **SQL**
- **Looker** (planned for visualization)

---

## Data Overview

The raw dataset includes:
- Facility identifiers and location details
- Survey response distributions (ratings, communication, cleanliness, recommendations)
- Linear mean scores
- Survey sampling and completion counts
- CMS footnote codes that qualify or suppress results

A key challenge of this data is the presence of:
- Extremely long column names
- Inconsistent spacing and formatting
- Footnote codes that affect reportability

These are addressed in the staging layer.

---

## Project Structure

```text
models/
  analytics/
    oas_cahps_metrics.sql
    analytics_tests.yml
  intermediate/
    int_facility_metrics.sql
    intermediate_tests.yml
  staging/
    stg_oas_cahps_survey.sql
    staging_tests.yml

macros/
    generate_schema_name.sql
seeds/
    oas_cahps_footnotes.csv
snapshots/
tests/
