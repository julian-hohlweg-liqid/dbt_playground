{{ config(materialized="table") }}


UPDATE {{ ref('SignedDatabase') }}
ADD COLUMN new_column STRING