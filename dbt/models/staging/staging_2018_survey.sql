{{ config(materialized='view') }}

select 'null' as profession,
    Employment as employment,
    Country as country,
    FormalEducation as education,
    UndergradMajor as major,
    cast(Age as string) as age,
    split(DevType, ';') as dev_type,
    YearsCodingProf as prof_code,
    CompanySize as org_size,
    cast(ConvertedSalary as float64) as annual_salary,
    Currency as currency,
    split(DatabaseWorkedWith, ';') as curr_db,
    split(DatabaseDesireNextYear, ';') as next_db,
    split(LanguageWorkedWith, ';') as curr_lang,
    split(LanguageDesireNextYear, ';') as next_lang,
    split(OperatingSystem, ';') as op_sys,
    split(PlatformWorkedWith, ';') as web_platform,
    split(PlatformDesireNextYear, ';') as next_platform,
    split(IDE, ';') as ide,
    2018 as s_year
from {{ source('staging', '18_part_clust') }}