{{ config(materialized='view') }}

select MainBranch as profession,
    Employment as employment,
    Country as country,
    EdLevel as education,
    'null' as major,
    cast(Age as string) as age,
    split(DevType, ';') as dev_type,
    YearsCodePro as prof_code,
    OrgSize as org_size,
    cast(ConvertedCompYearly as float64) as annual_salary,
    Currency as currency,
    split(DatabaseHaveWorkedWith, ';') as curr_db,
    split(DatabaseWantToWorkWith, ';') as next_db,
    split(LanguageHaveWorkedWith, ';') as curr_lang,
    split(LanguageWantToWorkWith, ';') as next_lang,
    split(OpSys, ';') as op_sys,
    split(PlatformHaveWorkedWith, ';') as web_platform,
    split(PlatformWantToWorkWith, ';') as next_platform,
    array<STRING>[] as ide,
    2021 as s_year
from {{ source('staging', '21_part_clust') }}