{{ config(materialized = 'table') }}

with contract as (
    select 
        Id as Contract_Id,
        Contact__c as Contact_Id,
        Status as Contract_Status,
        cast(CustomerSignedDate as date) as Signature_Date,
        cast(ClosingDate__c as date) as Closing_Date,
        cast(peo_portfolio_commitment_amount__c as numeric) as peo_portfolio_commitment_amount__c,
        cast(InvestmentAmount__c as numeric) as InvestmentAmount__c,
        RecordTypeId,
        peo_portfolio_product__c,
        InvestmentStrategy__c,
        RiskLevel__c,
        InvestmentType__c,
        Portfolio__c
    from `third-being-207111.RAW.SF_CONTRACT`
    where RecordTypeId != '0122X000000or7uQAA'
),

contact as (
    select
        Id,
        Email
    from `third-being-207111.RAW.SF_CONTACT`
),

portfolio as (
    select
        Id as Portfolio_Id,
        Portfolio_State__c as Portfolio_State,
        cast(Portfolio_State_Changed__c as date) as Portfolio_State_Changed,
        RecordTypeId as Portfolio_RecordTypeId
    from `third-being-207111.RAW.SF_PORTFOLIO`
    where
        RecordTypeId != '0127R000000L3HSQA0'
        and RecordTypeId is not null
),

joined_table_1 as (
    select *
    from contract
    left join contact
    on contract.Contact_Id = contact.Id
),

joined_table_2 as (
    select *
    from joined_table_1
    left join portfolio
    on joined_table_1.Portfolio__c = portfolio.Portfolio_Id
),

pre_final as (
    select
        Contract_Id,
        Contact_Id,
        Contract_Status,
        Signature_Date,
        Closing_Date,
        Portfolio_Id,
        Portfolio_State,
        Portfolio_State_Changed,
        Email,
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
        end) as Amount
    from joined_table_2
    where
        not (Email LIKE '%@liqid%')
        and Signature_Date is not null
),

calculation_first_signature as (
    select
        Contact_Id as Contact_Id_2,
        min(Signature_Date) as First_Signed_Date
    from pre_final
    group by 
        Contact_Id_2  
),

joined_table_3 as (
   select *
   from pre_final
   left join calculation_first_signature
   on pre_final.Contact_Id = calculation_first_signature.Contact_Id_2
),

final as (
   select
    Contract_Id,
    Contact_Id,
    Contract_Status,
    Closing_Date,
    Portfolio_Id,
    Portfolio_State,
    Portfolio_State_Changed, 
    Email,
    Product_Drill_2,
    Product_Drill_1,
    Amount,
    Signature_Date,
    First_Signed_Date,
    (case
        when date_diff(Signature_Date, First_Signed_Date, day ) > 15 then 'Cross Sell'
        when date_diff(Signature_Date, First_Signed_Date, day ) <= 15 then 'First Product'
    end) as Signature_Type
    from joined_table_3
)

select *
from final
 
