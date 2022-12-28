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
),

docusign as (
    select 
        dsfs__Contract__c,
        dsfs__Envelope_Status__c,
        MIN((CAST(dsfs__Completed_Date_Time__c as DATE))) as dsfs__Completed_Date_Time__c
    from `third-being-207111.RAW.SF_DOCUSIGN_STATUS`
    where dsfs__Envelope_Status__c = "Completed"
    group by dsfs__Envelope_Status__c, dsfs__Contract__c
),

joined_table as (
    select *
    from contract left join docusign
    on contract.Contract_Id = docusign.dsfs__Contract__c
)

select
    RecordTypeId,
    (CASE
        WHEN RecordTypeId = '0122X000000orDeQAI' THEN 'Wealth'
        WHEN RecordTypeId = '0127R000000tY5tQAE' THEN 'Access'
    END) AS Product_Drill_2 
from joined_table

