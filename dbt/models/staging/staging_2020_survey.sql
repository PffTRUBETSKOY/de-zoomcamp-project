{{ config(materialized='view') }}

select MainBranch as profession,
    Employment as employment,
    Country as country,
    EdLevel as education,
    UndergradMajor as major,
    cast(Age as string) as age,
    split(DevType, ';') as dev_type,
    YearsCodePro as prof_code,
    OrgSize as org_size,
    cast(ConvertedComp as float64) as annual_salary,
    CurrencySymbol as currency,
    split(DatabaseWorkedWith, ';') as curr_db,
    split(DatabaseDesireNextYear, ';') as next_db,
    split(LanguageWorkedWith, ';') as curr_lang,
    split(LanguageDesireNextYear, ';') as next_lang,
    split(OpSys, ';') as op_sys,
    split(PlatformWorkedWith, ';') as web_platform,
    split(PlatformDesireNextYear, ';') as next_platform,
    array<STRING>[] as ide,
    2020 as s_year
from {{ source('staging', '20_part_clust') }}