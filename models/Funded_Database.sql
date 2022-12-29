{{ config(materialized = 'table') }}

with transactions as (
    select
        Id as Transaction_Id,
        Type__c,
        PortfolioRef__c as Portfolio_Id,
        cast(SettlementDate__c as date) as Transaction_Date,
        Amount__c as Amount
    from `third-being-207111.RAW.SF_TRANSACTION`
    where
        Type__c = 'Deposit'
        and Amount__c is not null
),

portfolio as (
    select
        Id,
        RecordTypeId,
        Contract__c as Contract_Id,
        Portfolio_State__c as Portfolio_State,
        cast(Portfolio_State_Changed__c as date) as Portfolio_State_Changed,
    from `third-being-207111.RAW.SF_PORTFOLIO`
    where
        RecordTypeId != '0127R000000L3HSQA0'
        and RecordTypeId is not null
),

joined_table_1 as (
    select *
    from transactions
    left join portfolio
    on transactions.Portfolio_Id = portfolio.Id
)

select *
from joined_table_1