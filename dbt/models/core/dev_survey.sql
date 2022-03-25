{{ config(materialized='table') }}

with ds_2017 as (
    select * from {{ ref('staging_2017_survey') }}
),
ds_2018 as (
    select * from {{ ref('staging_2018_survey') }}
),
ds_2019 as (
    select * from {{ ref('staging_2019_survey') }}
),
ds_2020 as (
    select * from {{ ref('staging_2020_survey') }}
),
ds_2021 as (
    select * from {{ ref('staging_2021_survey') }}
),
all_surveys as (
    select * from ds_2017 union all select * from ds_2018 union all select * from ds_2019 union all select * from ds_2020 union all select * from ds_2021
)
select * from all_surveys
