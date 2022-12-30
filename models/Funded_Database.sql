{{ config(materialized = 'table') }}

with transactions as (
    select
        Id as Transaction_Id,
        Type__c,
        PortfolioRef__c as Portfolio_Id,
        cast(SettlementDate__c as date) as Transaction_Date,
        cast(Amount__c as numeric) as Amount
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
),

to_be_joined as (
    select
        Contact_Id,
        Contract_Status,
        Contract_Id as Contract_Id_2,
        Product_Drill_1,
        Product_Drill_2,
        Email
    from {{ ref('Signed_Database') }}
),

joined_table_2 as (
    select *
    from joined_table_1
    left join to_be_joined
    on joined_table_1.Contract_Id = to_be_joined.Contract_Id_2
),

to_be_unionized as (
    select
        Contact_Id,
        Contract_Id,
        Contract_Status,
        Amount,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Signature_Date as Transaction_Date,
        Portfolio_State,
        Portfolio_State_Changed,
        Email
    from {{ ref('Signed_Database') }}
),

unionized_table_1 as (
    select
        Contact_Id,
        Contract_Id,
        Contract_Status,
        Amount,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Transaction_Date,
        Portfolio_State,
        Portfolio_State_Changed,
        Email,
        Transaction_Id   
    from joined_table_2
    union all
    select
        Contact_Id,
        Contract_Id,
        Contract_Status,
        Amount,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Transaction_Date,
        Portfolio_State,
        Portfolio_State_Changed,
        Email,
        null as Transaction_Id
    from to_be_unionized
),

calculation_first_transaction as (
    select
        Contact_Id as Contact_Id_2,
        min(Transaction_Date) as Initial_Investment_Date
    from unionized_table_1
    group by 
        Contact_Id_2  
),

joined_table_3 as (
   select *
   from unionized_table_1
   left join calculation_first_transaction
   on unionized_table_1.Contact_Id = calculation_first_transaction.Contact_Id_2
),

final as (
    select
        Contact_Id,
        Contract_Id,
        Contract_Status,
        Amount,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Transaction_Date,
        Portfolio_State,
        Portfolio_State_Changed,
        Email,
        Transaction_Id,
        Initial_Investment_Date
    from joined_table_3
)

select *
from final