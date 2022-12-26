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
)

select *
from contract left join docusign
on contract.Contract_Id = docusign.dsfs__Contract__c

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
ADD COLUMN Product_Drill_2 STRING;
UPDATE `third-being-207111.dbt_analytics.SignedDatabase`
SET Product_Drill_2 =
CASE
    WHEN RecordTypeId = "0122X000000orDeQAI" THEN "Wealth"
    WHEN RecordTypeId = "0127R000000tY5tQAE" THEN "Access"
  END
WHERE TRUE;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
ADD COLUMN Amount NUMERIC;
UPDATE `third-being-207111.dbt_analytics.SignedDatabase`
SET Amount = 
CASE
  WHEN Product_Drill_2 = "Wealth" THEN InvestmentAmount__c
  WHEN Product_Drill_2 = "Access" THEN peo_portfolio_commitment_amount__c
 END
WHERE TRUE;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
ADD COLUMN Product_Drill_1 STRING;
UPDATE `third-being-207111.dbt_analytics.SignedDatabase`
SET Product_Drill_1 =
CASE
  WHEN Product_Drill_2 = "Wealth" THEN InvestmentStrategy__c
  WHEN Product_Drill_2 = "Access" THEN peo_portfolio_product__c
 END
WHERE TRUE;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
ADD COLUMN Signature_Date DATE;
UPDATE `third-being-207111.dbt_analytics.SignedDatabase`
SET Signature_Date =
CASE
  WHEN Product_Drill_2 = "Wealth" THEN CustomerSignedDate
  WHEN Product_Drill_2 = "Access" THEN dsfs__Completed_Date_Time__c
 END
WHERE TRUE;

DELETE FROM `third-being-207111.dbt_analytics.SignedDatabase` WHERE Signature_Date IS NULL;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN CustomerSignedDate;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN dsfs__Completed_Date_Time__c;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN RecordTypeId;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN peo_portfolio_commitment_amount__c;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN peo_portfolio_product__c;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN InvestmentStrategy__c;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN RiskLevel__c;

ALTER TABLE `third-being-207111.dbt_analytics.SignedDatabase`
DROP COLUMN InvestmentAmount__c;
