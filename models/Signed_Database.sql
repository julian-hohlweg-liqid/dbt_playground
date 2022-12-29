{{ config(materialized = 'table') }}

with contract as (
    select 
        Id as Contract_Id,
        Contact__c as Contact_Id,
        Status as Contract_Status,
        cast(CustomerSignedDate as date) as CustomerSignedDate,
        cast(ClosingDate__c as date) as Closing_Date,
        cast(peo_portfolio_commitment_amount__c as numeric) as peo_portfolio_commitment_amount__c,
        cast(InvestmentAmount__c as numeric) as InvestmentAmount__c,
        RecordTypeId,
        peo_portfolio_product__c,
        InvestmentStrategy__c,
        RiskLevel__c,
        InvestmentType__c,
        Portfolio__c,
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
    group by
        dsfs__Envelope_Status__c,
        dsfs__Contract__c
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
        RecordTypeId as Portfolio_RecordTypeId,
        Contract__c,
    from `third-being-207111.RAW.SF_PORTFOLIO`
    where
        RecordTypeId != '0127R000000L3HSQA0'
        and RecordTypeId is not null
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
),

joined_table_3 as (
    select *
    from joined_table_2
    left join portfolio
    on joined_table_2.Contract_Id = portfolio.Contract__c
),

pre_final_1 as (
    select
        Contract_Id,
        Contact_Id,
        Contract_Status,
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
        end) as Amount,
        (case
            when RecordTypeId = '0122X000000orDeQAI' then CustomerSignedDate
            when RecordTypeId = '0127R000000tY5tQAE' then dsfs__Completed_Date_Time__c
        end) as Signature_Date,
    from joined_table_3
    where not (Email LIKE '%@liqid%')
),

pre_final_2 as (
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
    (select
        min(Signature_Date)
        from joined_table_3
        group by 
            Contact_Id
        ) as First_Signed_Date,    
    from pre_final_1
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
    from pre_final_2
)

select * from final
 
