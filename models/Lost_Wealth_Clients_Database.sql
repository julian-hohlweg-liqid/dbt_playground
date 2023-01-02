{{ config(materialized = 'table') }}

with wealth_portfolios as (
    select *
    from {{ ref('Signed_Database') }}
    where Product_Drill_2 = 'Wealth'
),

closed_wealth_portfolios as (
    select *
    from wealth_portfolios
    where
        Portfolio_State = 'Closed'
        or Portfolio_State = 'Closing'
),

calculation_closed_count as (
    select 
        Contact_Id as Contact_Id_2,
        count(*) as Closed_Count
    from closed_wealth_portfolios
    group by Contact_Id_2
),

calculation_max_date as (
    select 
        Contact_Id as Contact_Id_3,
        max(Portfolio_State_Changed) as Max_Date
    from closed_wealth_portfolios
    group by Contact_Id_3
),

calculation_total_count as (
    select
        Contact_Id,
        count(*) as Total_Count
    from wealth_portfolios
    group by Contact_Id
),

joined_table_1 as (
    select *
    from calculation_closed_count
    left join calculation_max_date
    on calculation_closed_count.Contact_Id_2 = calculation_max_date.Contact_Id_3
),

joined_table_2 as (
    select * 
    from calculation_total_count
    left join joined_table_1
    on calculation_total_count.Contact_Id = joined_table_1.Contact_Id_2
),

pre_final as (
    select
        Contact_Id,
        Total_Count,
        Closed_Count,
        Max_Date,
        (case
            when Total_Count = Closed_Count then Max_Date
            else null
        end) as Lost_Wealth_Client_Date
    from joined_table_2
),

final as (
    select
        Contact_Id,
        Lost_Wealth_Client_Date
    from pre_final
    where Lost_Wealth_Client_Date is not null
)

select *
from final