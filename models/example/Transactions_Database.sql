{{ config(materialized = 'table') }}

with transactions as (
    select
        Id as Transaction_Id,
        cast(SettlementDate__c as date) as Transaction_Date,
        Type__c as Transaction_Type,
        Name as Transaction_Name,
        PortfolioRef__c as Portfolio_Id,
        cast(Amount__c as numeric) as Transaction_Amount
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
        Closing_Date,
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
        Signature_Date,
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
    where
        Product_Drill_2 = 'Access'
        and Closing_Date is not null
),

joined_table_1 as (
    select *
    from transactions
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
        Signature_Date,
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
        Signature_Date,
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

calculation_first_transaction_contact as (
     select
        Contact_Id as Contact_Id_3,
        min(Transaction_Date) as First_Transaction_Date_Contact
    from joined_table_3
    group by 
        Contact_Id_3 
),

joined_table_4 as (
    select * 
    from joined_table_3
    left join calculation_first_transaction_contact
    on joined_table_3.Contact_Id = calculation_first_transaction_contact.Contact_Id_3
),

calculation_first_transaction_portfolio as (
     select
        Portfolio_Id as Portfolio_Id_3,
        min(Transaction_Date) as First_Transaction_Date_Portfolio
    from joined_table_4
    group by 
        Portfolio_Id_3 
),

joined_table_5 as (
    select *
    from joined_table_4
    left join calculation_first_transaction_portfolio
    on joined_table_4.Portfolio_Id = calculation_first_transaction_portfolio.Portfolio_Id_3
),

final as (
    select
        Transaction_Id,
        Transaction_Name,
        Transaction_Type,
        Transaction_Amount,
        Transaction_Date,
        (case
            when date_diff(Transaction_Date, First_Transaction_Date_Portfolio, day ) > 15 then 'Top Up'
            when date_diff(Transaction_Date, First_Transaction_Date_Portfolio, day ) <= 15 then 'Top Up'
        end) as Initial_or_Top_Up_Transaction,
        Contact_Id,
        Contract_Id,
        Contract_Status,
        Portfolio_Id,
        V_Bank_Number,
        Contact_Created_Date,
        Management_Fee_Percentage,
        Portfolio_State,
        Portfolio_State_Changed,
        Signature_Date,
        First_Signed_Date,
        First_Transaction_Date_Contact,
        First_Transaction_Date_Portfolio,
        Product_Drill_1,
        Product_Drill_2,
        Email
    from joined_table_5
)

select *
from final



