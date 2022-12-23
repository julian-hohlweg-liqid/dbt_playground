{{ config(materialized="table") }}

with contract as (
    select 
        Contact__c as Contact_Id,
        Id as Contract_Id,
        CAST(CustomerSignedDate as DATE) as CustomerSignedDate,
        RecordTypeId,
        CAST(peo_portfolio_commitment_amount__c as NUMERIC) as peo_portfolio_commitment_amount__c,
        peo_portfolio_product__c,
        InvestmentStrategy__c,
        RiskLevel__c,
        CAST(InvestmentAmount__c as NUMERIC) as InvestmentAmount__c,
        InvestmentType__c
    from `third-being-207111.RAW.SF_CONTRACT`
    where RecordTypeId != "0122X000000or7uQAA"
)

select *
from contract