{{ config(materialized = 'table') }}

with transactions as (
    select
        Id as Transaction_Id,
        SettlementDate__c as Trsansaction_Date,
        Type__c as Transaction_Type,
        Name as Transaction_Name,
        PortfolioRef__c as Portfolio_Id,
        Amount__c as Transaction_Amoint
    from `third-being-207111.RAW.SF_TRANSACTION`
),

wealth_signings as (
    select
        Contact_Id,
        Contract_Id,
        Portfolio_Id as Portfolio_Id_2,
        Product_Drill_1,
        Product_Drill_2,
        Signature_Date,
        First_Signed_Date,
        Amount,
        Signature_Type,
        Portfolio_State,
        Portfolio_State_Changed,
        Contract_Status,
        Email
    from {{ ref('Signed_Database') }}
    where Product_Drill_2 = 'Wealth'
),

access_fundings as (
    select
        Contact_Id,
        Contract_Id,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Signature_Date as Transaction_Date,
        'Deposit' as Transaction_Type,
        First_Signed_Date,
        Amount as Transaction_Amount,
        Signature_Type,
        Portfolio_State,
        Portfolio_State_Changed,
        Contract_Status,
        Email,
        Closing_Date
    from {{ ref('Signed_Database') }}
     Product_Drill_2 = 'Access'
     and Closing_Date is not null
),

joined_table_1 as (
    select *
    transactions
    inner join wealth_signings
    on transactions.Portfolio_Id = wealth_signings.Portfolio_Id_2
),

unionized_table_1 as (
    select 
        Contact_Id,
        Contract_Id,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Transaction_Date,
        Transaction_Type,
        First_Signed_Date,
        Transaction_Amount,
        Signature_Type,
        Portfolio_State,
        Portfolio_State_Changed,
        Contract_Status,
        Email,
        Closing_Date,
        null as Transaction_Id,
        null as Transaction_Name
    from access_fundings
    union all
    select
        Contact_Id,
        Contract_Id,
        Portfolio_Id,
        Product_Drill_1,
        Product_Drill_2,
        Transaction_Date,
        Transaction_Type,
        First_Signed_Date,
        Transaction_Amount,
        Signature_Type,
        Portfolio_State,
        Portfolio_State_Changed,
        Contract_Status,
        Email,
        Closing_Date,
        Transaction_Id,
        Transaction_Name
    from joined_table_1
),

contact as (
    select
        Id as Contact_Id_2,
        cast(CreatedDate as date) as Contact_Created_Date
    from `third-being-207111.RAW.SF_CONTACT`
),

portfolio as (
    select
        Id as Portfolio_Id_2,
        VBankDepositNumber__c as V_Bank_Number,
        Management_Fee_Percentage__c as Management_Fee_Percentage,
    from `third-being-207111.RAW.SF_PORTFOLIO`
),

joined_table_2 as (
    select *
    from unionized_table_1
    left join contact
    on unionized_table_1.Contact_Id = contact.Contact_Id_2
),

joined_table_3 as (
    select *
    from joined_table_2
    left join portfolio
    on joined_table_2.Portfolio_Id = portfolio.Portfolio_Id_2
),

calculation

