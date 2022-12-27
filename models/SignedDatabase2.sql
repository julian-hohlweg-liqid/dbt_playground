{{ config(materialized="table") }}

with raw as (
    select *
    from {{ ref('SignedDatabase') }}
)

select * from raw