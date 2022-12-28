{{ config(materialized='table') }}

with contract as (
    select 
        Contact__c as Contact_Id,
        Id as Contract_Id,
        cast(CustomerSignedDate as date) as CustomerSignedDate,
        RecordTypeId,
        cast(peo_portfolio_commitment_amount__c as numeric) as peo_portfolio_commitment_amount__c,
        peo_portfolio_product__c,
        InvestmentStrategy__c,
        RiskLevel__c,
        cast(InvestmentAmount__c as numeric) as InvestmentAmount__c,
        InvestmentType__c
    from `third-being-207111.RAW.SF_CONTRACT`
    where RecordTypeId != '0122X000000or7uQAA'
),

docusign as (
    select 
        dsfs__Contract__c,
        dsfs__Envelope_Status__c,
        min((cast(dsfs__Completed_Date_Time__c as date))) as dsfs__Completed_Date_Time__c
    from `third-being-207111.RAW.SF_DOCUSIGN_STATUS`
    where dsfs__Envelope_Status__c = 'Completed'
    group by dsfs__Envelope_Status__c, dsfs__Contract__c
),

contact as (
    select
        Id,
        Email
    from `third-being-207111.RAW.SF_CONTACT`
),

joined_table_1 as (
    select *
    from contract
    left join docusign
    on contract.Contract_Id = docusign.dsfs__Contract__c
),

joined_table_2 as (
    select *
    from joined_table_1
    left join contact
    on joined_table_1.Contact_Id = contact.Id
)

select
    Contact_Id,
    (case
        when RecordTypeId = '0122X000000orDeQAI' then 'Wealth'
        when RecordTypeId = '0127R000000tY5tQAE' then 'Access'
    end) as Product_Drill_2,
    (case
        when RecordTypeId = '0122X000000orDeQAI' then InvestmentStrategy__c
        when RecordTypeId = '0127R000000tY5tQAE' then peo_portfolio_product__c
    end) as Product_Drill_1,
    (case
        when RecordTypeId = '0122X000000orDeQAI' then InvestmentAmount__c
        when RecordTypeId = '0127R000000tY5tQAE' then peo_portfolio_commitment_amount__c
    end) as Amount,
    (case
        when RecordTypeId = '0122X000000orDeQAI' then CustomerSignedDate
        when RecordTypeId = '0127R000000tY5tQAE' then dsfs__Completed_Date_Time__c
    end) as Signature_Date
from joined_table_2
where not (Email LIKE '%@liqid%')

