{{ config(materialized = 'table') }}

with contact as (
    select
        Id as Contact_Id,
        Email,
        cast(CreatedDate as date) as Lead_Creation_Date,
        Phone,
        MobilePhone,
        Lead_Origin__c as Lead_Origin,
        (case
            when Phone is not null then 'Sales'
            when MobilePhone is not null then 'Sales'
            else 'Digital'
        end) as Lead_Type
    from `third-being-207111.RAW.SF_CONTACT`
),

calculation_signed_client as (
    select
        Contact_Id as Contact_Id_2,
        min(First_Signed_Date) as First_Signed_Date,
        1 as Signed_Client
    from {{ ref('Signed_Database') }}
    group by
        Contact_Id_2
),

calculation_funded_client as (
    select
        Contact_Id as Contact_Id_3,
        min(First_Transaction_Date_Contact) as First_Transaction_Date_Contact,
        1 as Funded_Client
    from {{ ref('Funded_Database') }}
    group by
        Contact_Id_3
),

joined_table_1 as (
    select *
    from contact
    left join calculation_signed_client
    on contact.Contact_Id = calculation_signed_client.Contact_Id_2
),

joined_table_2 as (
    select *
    from joined_table_1
    left join calculation_funded_client
    on joined_table_1.Contact_Id = calculation_funded_client.Contact_Id_3
),

final as (
    select
        Contact_Id,
        Email,
        Lead_Creation_Date,
        Lead_Type,
        Lead_Origin,
        Signed_Client,
        First_Signed_Date,
        Funded_Client,
        First_Transaction_Date_Contact
    from joined_table_2
)

select *
from final

