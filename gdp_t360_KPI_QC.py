# Databricks notebook source
# DBTITLE 1,Total NDS check by businessdate and CurrencyCode
# MAGIC %sql
# MAGIC select a.*,
# MAGIC b.total_nds
# MAGIC from
# MAGIC (
# MAGIC select 
# MAGIC businessdate,
# MAGIC fiscalyearnumber,
# MAGIC fiscalweekinyearnumber,
# MAGIC CurrencyCode,
# MAGIC sum(netdiscountedsalesrevenueamount) as total_nds_check
# MAGIC from edap_pub_customersales.Customer_Sales_Transaction360_summary
# MAGIC where businessdate between "2020-07-27" and  "2020-08-09"
# MAGIC and customertransactionind = 1
# MAGIC and storecountrycode = 'US'
# MAGIC and voidflag = 'N'
# MAGIC group by 1,2,3,4) as a
# MAGIC inner join 
# MAGIC (select
# MAGIC businessdate,
# MAGIC fiscalyearnumber,
# MAGIC fiscalweekinyearnumber,
# MAGIC CurrencyCode,
# MAGIC sum(total_nds) as total_nds
# MAGIC from cma_teams_store360.cw_gdp_t360_1month
# MAGIC group by 1,2,3,4) as b
# MAGIC on
# MAGIC a.businessdate= b.businessdate
# MAGIC and a.fiscalyearnumber=b.fiscalyearnumber
# MAGIC and a.fiscalweekinyearnumber=b.fiscalweekinyearnumber
# MAGIC and a.CurrencyCode=b.CurrencyCode

# COMMAND ----------

# DBTITLE 1,Total NDS check by businessdate, loyaltyprogramname and CurrencyCode
# MAGIC %sql
# MAGIC select a.*,
# MAGIC b.total_nds
# MAGIC from
# MAGIC (
# MAGIC select 
# MAGIC businessdate,
# MAGIC fiscalyearnumber,
# MAGIC fiscalweekinyearnumber,
# MAGIC loyaltyprogramname,
# MAGIC CurrencyCode,
# MAGIC sum(netdiscountedsalesrevenueamount) as total_nds_check
# MAGIC from edap_pub_customersales.Customer_Sales_Transaction360_summary
# MAGIC where businessdate between "2020-07-27" and  "2020-08-09"
# MAGIC and customertransactionind = 1
# MAGIC and storecountrycode = 'US'
# MAGIC and voidflag = 'N'
# MAGIC group by 1,2,3,4,5) as a
# MAGIC inner join 
# MAGIC (select
# MAGIC businessdate,
# MAGIC fiscalyearnumber,
# MAGIC fiscalweekinyearnumber,
# MAGIC loyaltyprogramname,
# MAGIC CurrencyCode,
# MAGIC sum(total_nds) as total_nds
# MAGIC from cma_teams_store360.cw_gdp_t360_1month
# MAGIC group by 1,2,3,4,5) as b
# MAGIC on
# MAGIC a.businessdate= b.businessdate
# MAGIC and a.fiscalyearnumber=b.fiscalyearnumber
# MAGIC and a.fiscalweekinyearnumber=b.fiscalweekinyearnumber
# MAGIC and a.loyaltyprogramname=b.loyaltyprogramname
# MAGIC and a.CurrencyCode=b.CurrencyCode

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.final_table;

# COMMAND ----------

# DBTITLE 1,Row by row checks for each KPI
# MAGIC %sql
# MAGIC create table gdp_t360_check1 as 
# MAGIC select
# MAGIC a.businessdate,
# MAGIC a.fiscalyearnumber,
# MAGIC a.fiscalweekinyearnumber,
# MAGIC a.customer_type,
# MAGIC a.loyaltyprogramname,
# MAGIC a.country_code,
# MAGIC a.customerorderchannelname, 
# MAGIC a.physical_payment_flg,
# MAGIC a.digital_payment_flg,
# MAGIC a.split_tender_flg,
# MAGIC a.Payment_Mode,
# MAGIC a.mobile_scan_type,
# MAGIC a.CurrencyCode,
# MAGIC   sum(a.netdiscountedsalesrevenueamount) as total_nds_check,
# MAGIC   sum(a.digitalsvcndsamount)as total_digitalsvcndsamount_check,
# MAGIC   sum(a.physicalsvcndsamount)as total_physicalsvcndsamount_check,
# MAGIC   sum(a.otherpaymenttypendsamount) as total_otherpaymenttypendsamount_check,
# MAGIC   sum(a.bankcarddigitalndsamount)as total_bankcarddigitalndsamount_check,
# MAGIC   sum(a.bankcardphysicalndsamount)as total_bankcardphysicalndsamount_check,
# MAGIC   sum(a.paypalndsamount)as total_paypalndsamount_check,
# MAGIC   sum(a.CashNDSAmount)as total_cashndsamount_check,
# MAGIC   round((sum(a.itemquantity)) / (count(distinct a.salestransactionid)), 0) as unit_per_transaction_check,
# MAGIC   round((sum(a.netdiscountedsalesrevenueamount)) / (count(distinct a.salestransactionid)), 0) as average_ticket_check,
# MAGIC   count (distinct a.salestransactionid) as total_trans_check
# MAGIC   from
# MAGIC (
# MAGIC select 
# MAGIC loyaltyprogramname,
# MAGIC fiscalyearnumber,
# MAGIC fiscalweekinyearnumber, 
# MAGIC salestransactionid,
# MAGIC postransactionid as transactionid,
# MAGIC businessdate,
# MAGIC customerorderchannelname, 
# MAGIC ordertotaltaxamount, 
# MAGIC ordertenderamount,
# MAGIC netdiscountedsalesrevenueamount, 
# MAGIC unitsinordercount,
# MAGIC digitalsvctenderamount,
# MAGIC bankcarddigitaltenderamount,
# MAGIC paypaltenderamount,
# MAGIC physicalsvctenderamount,
# MAGIC cashtenderamount,
# MAGIC bankcardphysicaltenderamount,
# MAGIC otherpaymenttypetenderamount,
# MAGIC storecountrycode as country_code,
# MAGIC itemquantity,
# MAGIC CurrencyCode,
# MAGIC case when (accountid is not null)
# MAGIC         and (loyaltyprogramname is not null) then 'SR'
# MAGIC         else 'NON_SR'
# MAGIC       end as customer_type
# MAGIC , case when PhysicalSVCTenderAmount + CashTenderAmount +  BankCardPhysicalTenderAmount + OtherPaymentTypeTenderAmount  > 0 then 1 else 0 end as physical_payment_flg
# MAGIC , case when DigitalSVCTenderAmount +  BankCardDigitalTenderAmount + PayPalTenderAmount > 0 then 1 else 0 end as digital_payment_flg
# MAGIC , case when (PhysicalSVCTenderAmount + CashTenderAmount +  BankCardPhysicalTenderAmount + OtherPaymentTypeTenderAmount > 0 
# MAGIC        and  DigitalSVCTenderAmount +  BankCardDigitalTenderAmount + PayPalTenderAmount > 0) 
# MAGIC        or (SVCTenderInd + CashTenderInd + BankCardTenderInd + PayPalTenderInd + OtherTenderTypeInd  > 1 )
# MAGIC        then 1 else 0 end as split_tender_flg
# MAGIC , case when CustomerOrderChannelName = 'MOP' or DigitalSVCTenderAmount +  BankCardDigitalTenderAmount + PayPalTenderAmount > 0 then 'Scan and Pay' 
# MAGIC        when PhysicalSVCTenderAmount + CashTenderAmount +  BankCardPhysicalTenderAmount + OtherPaymentTypeTenderAmount > 0  then 'Scan Only'
# MAGIC        else 'Error' end as Payment_Mode
# MAGIC , case when OLMobileAppScanInd = 1 then 'Non-SVC Scan'
# MAGIC        when customeridmethodtype = 'id' then 'Scan Only'
# MAGIC      when SVCMobileAppScanInd = 1 then 'SVC Scan' end as mobile_scan_type
# MAGIC , case when netdiscountedsalesrevenueamount <= DigitalSVCTenderAmount  then netdiscountedsalesrevenueamount 
# MAGIC     when netdiscountedsalesrevenueamount > DigitalSVCTenderAmount then DigitalSVCTenderAmount end as DigitalSVCNDSAmount
# MAGIC , case when netdiscountedsalesrevenueamount > DigitalSVCTenderAmount
# MAGIC          and netdiscountedsalesrevenueamount <= PhysicalSVCTenderAmount + DigitalSVCTenderAmount then netdiscountedsalesrevenueamount - DigitalSVCTenderAmount 
# MAGIC             when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount then PhysicalSVCTenderAmount end as PhysicalSVCNDSAmount
# MAGIC , case when  netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount
# MAGIC         and netdiscountedsalesrevenueamount <= PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount then netdiscountedsalesrevenueamount - DigitalSVCTenderAmount - PhysicalSVCTenderAmount
# MAGIC             when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount then OtherPaymentTypeTenderAmount end as OtherPaymentTypeNDSAmount
# MAGIC , case when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount
# MAGIC         and netdiscountedsalesrevenueamount <= PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount
# MAGIC               then netdiscountedsalesrevenueamount - DigitalSVCTenderAmount - PhysicalSVCTenderAmount - OtherPaymentTypeTenderAmount
# MAGIC             when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount + BankCardDigitalTenderAmount then BankCardDigitalTenderAmount end as BankCardDigitalNDSAmount
# MAGIC , case when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount
# MAGIC         and netdiscountedsalesrevenueamount <= PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount
# MAGIC               then netdiscountedsalesrevenueamount - DigitalSVCTenderAmount - PhysicalSVCTenderAmount - OtherPaymentTypeTenderAmount - BankCardDigitalTenderAmount
# MAGIC             when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount + BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount
# MAGIC               then BankCardPhysicalTenderAmount end as BankCardPhysicalNDSAmount
# MAGIC , case when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount
# MAGIC         and netdiscountedsalesrevenueamount <= PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount + PayPalTenderAmount
# MAGIC               then netdiscountedsalesrevenueamount - DigitalSVCTenderAmount - PhysicalSVCTenderAmount - OtherPaymentTypeTenderAmount - BankCardDigitalTenderAmount - BankCardPhysicalTenderAmount
# MAGIC             when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount + BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount + PayPalTenderAmount
# MAGIC               then PayPalTenderAmount end as PayPalNDSAmount
# MAGIC , case when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount + PayPalTenderAmount
# MAGIC         and netdiscountedsalesrevenueamount <= PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount +  BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount + PayPalTenderAmount + CashTenderAmount
# MAGIC               then netdiscountedsalesrevenueamount - DigitalSVCTenderAmount - PhysicalSVCTenderAmount - OtherPaymentTypeTenderAmount - BankCardDigitalTenderAmount - BankCardPhysicalTenderAmount - PayPalTenderAmount
# MAGIC             when netdiscountedsalesrevenueamount > PhysicalSVCTenderAmount + DigitalSVCTenderAmount + OtherPaymentTypeTenderAmount + BankCardDigitalTenderAmount + BankCardPhysicalTenderAmount + PayPalTenderAmount + CashTenderAmount
# MAGIC               then CashTenderAmount end as CashNDSAmount
# MAGIC from edap_pub_customersales.Customer_Sales_Transaction360_summary
# MAGIC where businessdate between "2020-07-27" and  "2020-08-02"
# MAGIC and customertransactionind = 1
# MAGIC and storecountrycode = 'US'
# MAGIC and voidflag = 'N'
# MAGIC ) as a
# MAGIC
# MAGIC group by 1,2,3,4,5,6,7,8,9,10,11,12,13

# COMMAND ----------

###removed null records as tables will not get join on null values
%sql
create table gdp_t360_check2 as 
select a.* from default.gdp_t360_check1 as a
where  a.loyaltyprogramname is not null
and a.country_code is not null
and a.customerorderchannelname is not null
and  a.physical_payment_flg is not null
and  a.digital_payment_flg is not null
and  a.split_tender_flg is not null
and  a.Payment_Mode is not null
and a.mobile_scan_type is not null
and a.CurrencyCode is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC create table final_table as 
# MAGIC select 
# MAGIC a.businessdate,
# MAGIC a.fiscalyearnumber,
# MAGIC a.fiscalweekinyearnumber,
# MAGIC a.customer_type,
# MAGIC a.loyaltyprogramname,
# MAGIC a.country_code,
# MAGIC a.customerorderchannelname, 
# MAGIC a.physical_payment_flg,
# MAGIC a.digital_payment_flg,
# MAGIC a.split_tender_flg,
# MAGIC a.Payment_Mode,
# MAGIC a.mobile_scan_type,
# MAGIC a.CurrencyCode,
# MAGIC a.total_nds_check,
# MAGIC b.total_nds,
# MAGIC case when a.total_DigitalSVCNDSAmount_check is null then 0 else a.total_DigitalSVCNDSAmount_check end as total_DigitalSVCNDSAmount_check,
# MAGIC b.total_DigitalSVCNDSAmount,
# MAGIC case when a.total_PhysicalSVCNDSAmount_check is null then 0 else a.total_PhysicalSVCNDSAmount_check end as total_PhysicalSVCNDSAmount_check,
# MAGIC b.total_PhysicalSVCNDSAmount,
# MAGIC case when a.total_OtherPaymentTypeNDSAmount_check is null then 0 else a.total_OtherPaymentTypeNDSAmount_check end as total_OtherPaymentTypeNDSAmount_check,
# MAGIC b.total_OtherPaymentTypeNDSAmount,
# MAGIC case when a.total_BankCardDigitalNDSAmount_check is null then 0 else a.total_BankCardDigitalNDSAmount_check end as total_BankCardDigitalNDSAmount_check,
# MAGIC b.total_BankCardDigitalNDSAmount,
# MAGIC case when a.total_BankCardPhysicalNDSAmount_check is null then 0 else a.total_BankCardPhysicalNDSAmount_check end as total_BankCardPhysicalNDSAmount_check,
# MAGIC b.total_BankCardPhysicalNDSAmount,
# MAGIC case when a.total_PayPalNDSAmount_check is null then 0 else a.total_PayPalNDSAmount_check end as total_PayPalNDSAmount_check,
# MAGIC b.total_PayPalNDSAmount,
# MAGIC case when a.total_CashNDSAmount_check is null then 0 else a.total_CashNDSAmount_check end as total_CashNDSAmount_check,
# MAGIC b.total_CashNDSAmount,
# MAGIC a.unit_per_transaction_check,
# MAGIC b.unit_per_transaction,
# MAGIC a.average_ticket_check,
# MAGIC b.average_ticket,
# MAGIC a.total_trans_check,
# MAGIC b.total_trans
# MAGIC from default.gdp_t360_check2 as a
# MAGIC left join 
# MAGIC cma_teams_store360.cw_gdp_t360_1month as b
# MAGIC on
# MAGIC a.businessdate= b.businessdate
# MAGIC and a.fiscalyearnumber=b.fiscalyearnumber
# MAGIC and a.fiscalweekinyearnumber=b.fiscalweekinyearnumber
# MAGIC and a.customer_type=b.customer_type
# MAGIC and a.loyaltyprogramname=b.loyaltyprogramname
# MAGIC and a.country_code=b.country_code
# MAGIC and a.customerorderchannelname=b.customerorderchannelname
# MAGIC and a.physical_payment_flg=b.physical_payment_flg
# MAGIC and a.digital_payment_flg=b.digital_payment_flg
# MAGIC and a.split_tender_flg=b.split_tender_flg
# MAGIC and a.Payment_Mode=b.Payment_Mode
# MAGIC and a.mobile_scan_type=b.mobile_scan_type
# MAGIC and a.CurrencyCode=b.CurrencyCode

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_table
# MAGIC where total_nds_check<> total_nds
# MAGIC or total_DigitalSVCNDSAmount_check<>total_DigitalSVCNDSAmount
# MAGIC or total_PhysicalSVCNDSAmount_check<> total_PhysicalSVCNDSAmount
# MAGIC or total_OtherPaymentTypeNDSAmount_check<>total_OtherPaymentTypeNDSAmount
# MAGIC or total_BankCardDigitalNDSAmount_check<> total_BankCardDigitalNDSAmount
# MAGIC or total_BankCardPhysicalNDSAmount_check<> total_BankCardPhysicalNDSAmount
# MAGIC or total_PayPalNDSAmount_check<> total_PayPalNDSAmount
# MAGIC or total_CashNDSAmount_check<>total_CashNDSAmount
# MAGIC or unit_per_transaction_check<>unit_per_transaction
# MAGIC or average_ticket_check<>average_ticket
# MAGIC or total_trans_check<>total_trans
