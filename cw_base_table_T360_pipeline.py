# Databricks notebook source
# DBTITLE 1,Imports &Variables
# dbutils.library.installPyPI('timezonefinder')
# dbutils.library.restartPython()
# dbutils.library.list()
# Importing all the libs
from pyspark.sql import functions as F
from datetime import date,timedelta
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType,
)


# Input & Output Tablenames
# t360_tbl="edap_preprodpub_customersales.customer_sales_transaction360_line_item"
# prod_hier_tbl = "edap_pub_productitem.enterprise_product_hierarchy"
# one_store_tbl = "cma_teams_store360.s360_one_store_kpi"
# store_tbl = "edap_pub_store.store_edw_restricted"
# cust_tbl = "cma_teams_store360.donotuse_customer_segment_all"
# promo_tbl = "cdl_prod_intermediate.product_workbench_ingest_promo_ingest"
# ls_tbl = "edap_pub_customersales.customer_svc_transaction_history_restricted"
# mop_tbl = "cdl_prod_publish.mobile_clickstream_hits"
# xid_tbl = "edap_pub_customer.enterprise_customer_external_id_mapping_restricted"
# svc_ls_tbl = "cma_teams_store360.s360_cw_svc_mrcht_ls_map"
# out_tbl_base = "cma_teams_store360.donotuse_ch_item_dec_agg_weekly_base"
t360_tbl=dbutils.widgets.get('t360_tbl')
prod_hier_tbl = dbutils.widgets.get('prod_hier_tbl')
one_store_tbl=dbutils.widgets.get('one_store_tbl')
store_tbl=dbutils.widgets.get('store_tbl')
cust_tbl = dbutils.widgets.get('cust_tbl')
promo_tbl = dbutils.widgets.get('promo_tbl')
ls_tbl=dbutils.widgets.get('ls_tbl')
mop_tbl = dbutils.widgets.get('mop_tbl')
xid_tbl=dbutils.widgets.get('xid_tbl')
svc_ls_tbl=dbutils.widgets.get('svc_ls_tbl')
out_tbl_base=dbutils.widgets.get('out_tbl_base')

# List of column names

# line_item_cols = [
#     "businessdate",
#     "fiscalyearnumber",
#     "fiscalweekinyearnumber",
#     "storenumber",
#     F.col("storecountrycode").alias("country_code"),
#     F.col("POSTransactionHQDTM").alias("datetime"),
#     F.col("POSTransactionUTCDTM").alias("utc_datetime"),
#     "transactionid",
#     "posorderchannelcode",
#     "transactionnumber",
#     "customertransactionind",
#     "accountid",
#     "loyaltyprogramname",
#     "loyaltyprogramlevelname",
#     "grosssaleslocalamount",
#     "grosslineitemqty",
#     "netdiscountedsalesamount",
#     "starbucksmobileappscanind",
#     "moptransactionind",
#     "itemnumber",
#     "parentitemmodifiedind",
#     "modifierind",
#     "operatingsystemname",
# ]

t360_cols=[
"accountid",
"loyaltyprogramname",
"weekdayind",
"fiscalyearnumber",
"fiscalweekinyearnumber", 
"fiscalweekbegindate",
"daypartcode",
"salestransactionid",
F.col("postransactionid").alias("transactionid"),
"salestransactionutcdate",
"storenumber",
 F.col("salestransactionlocaldatetime").alias("datetime"),
"businessdate",
"customertransactionind",
"customerorderchannelname", 
"voidflag",
"childlinesequencenumber",
"itemid",
"revenueitemind",
"itemquantity", 
"finallineitempriceamount",
"netdiscountedsalesrevenueamount", 
F.col('storecountrycode').alias('country_code')
  ]
# prod_hier_cols = [
#     F.col("itemid").alias("itemnumber"),
#     "producttypedescription",
#     "productcategorydescription",
#     "productstyledescription",
#     "NotionalProductDescription",
# ]


one_store_cols = [
    "storenumber",
    "store_type",
    "ownership_type",
    "country_code",
]

store_cols = [
  "storenumber",
  "StoreTimeZoneDescription"
]

# tender_cols = [
#     "businessdate",
#     "storenumber",
#     F.col("storecountrycode").alias("country_code"),
#     "transactionnumber",
#     F.col("transactionhalfhoursid").alias("halfhoursid"),
#     "transactionhoursid",
# ]

half_hour_cols = ["halfhoursid", "retaildaypartname"]


promo_cols = ["lto_name", "start_date", F.col("item_id").alias("itemid")]

ls_col = [
    "accountid",
    "businessdate",
    "fiscalyearnumber",
    "fiscalweekinyearnumber",
    "transactionmerchantid",
    "transactionalternatemerchantid",
    "svctransactionNumber",
    "localtransactionamount",
#     "mobiledeviceid",
  
    "salessubchannelcode",
    "loyaltyprogramname",
#     "loyaltyprogramlevelname",
#     F.col("touchpayind").alias("starbucksmobileappscanind"),
    F.col("mobileorderpayind").alias("moptransactionind"), 
    F.col("registerlocaldatetime").alias("datetime"),
#     F.col("deviceplatformname").alias("operatingsystemname"),
]

svc_cols = [
    "storenumber",
    F.col("svc_mrcht_id").alias("transactionmerchantid"),
    F.col("alt_mid_id").alias("transactionalternatemerchantid"),
]

mop_col = [
    F.col("pt_date").alias("businessdate"),
    "user_authenticated_session",
    "operating_system",
    "session_id",
    "xid_top",
    "store_number",
    "order_token",
    "hit_timestamp"
]

xid_col = [F.col("externaluserid").alias("xid_top"), "accountid"]



lpn = ["MSR_USA"]
country_list = ["US"]
promo_country = ["US","US & CA"]
# promo_dt = '2019-10-10'


start_dt=dbutils.widgets.get('start_dt')
end_dt=dbutils.widgets.get('end_dt')
promo_dt=dbutils.widgets.get('promo_dt')
# promo_dt=F.lit((start_dt - timedelta(56)))

print(start_dt)

# COMMAND ----------

print(promo_dt)

# COMMAND ----------

from pyspark.sql import functions as F
test=F.lit((date(2019,12,12) - timedelta(56)))
print(test)

# COMMAND ----------

# DBTITLE 1,Load Tables in Dataframe
# Creating dataframe for promotional data
# Adding new col which is 8 weeks in future from the start date of LTO

promo_df = (
     spark.table(promo_tbl)
    .select(promo_cols)
    .filter((F.col("country").isin(promo_country))
     & (F.col("start_date") >= promo_dt)
     & (F.col("start_date") <= end_dt ))
    .withColumn("lto_8wk_date", F.date_add(F.col("start_date"), 56))
)


# creating Dataframe for LEGACY_POS_LINE_ITEM table.
# creating new cols like customer_type,week_level and sales channel code.
# sales Channel code is added to make the distinction between LS and CO trsanction.
# changing the type of NDS so that it will get merge with LS trasacntion data

# line_item_df = (
#     spark.table(line_item_tbl)
#     .select(line_item_cols)
#     .filter(
#         (F.col("customertransactionind") != 0)
#         & (F.col("businessdate") >= start_dt)
#         & (F.col("businessdate") <= end_dt)
#         & (F.col("country_code").isin(country_list))
#     )
#     .withColumn(
#         "customer_type",
#         F.when(
#             (F.col("accountid").isNotNull())
#             & (F.col("loyaltyprogramname").isNotNull()),
#             F.lit("SR"),
#         ).otherwise(F.lit("NON_SR")),
#     )
#     .withColumn(
#         "week_level",
#         F.when(F.dayofweek("businessdate").isin(1, 7), F.lit("weekend")).otherwise(
#             F.lit("weekday")
#         ),
#     )
#     .withColumn("saleschannelcode", F.lit("CO"))
#     .withColumn("nds", F.col("netdiscountedsalesamount").cast(DecimalType(20, 4)))
#     .drop(F.col("netdiscountedsalesamount"))
# )


# creating Dataframe for T360 table.
# creating new cols like customer_type,week_level
# changing the type of NDS so that it will get merge with LS trasacntion data

# there is Weekdayind to identify weekend or weekday

t360_df = (
    spark.table(t360_tbl)
    .select(t360_cols)
    .filter(
        (F.col("customertransactionind") != 0)
        & (F.col("businessdate") >= start_dt)
        & (F.col("businessdate") <= end_dt)
        & (F.col("country_code").isin(country_list))
        & (F.col("voidflag")!="Y")
      & (F.col("revenueitemind")==1)
    

    )
    .withColumn(
        "customer_type",
        F.when(
            (F.col("accountid").isNotNull())
            & (F.col("loyaltyprogramname").isNotNull()),
            F.lit("SR"),
        ).otherwise(F.lit("NON_SR")),
    )

    .withColumn("moptransactionind",
              F.when(
                F.col("customerorderchannelname")=='MOP',
                1).when(F.col("customerorderchannelname")!='MOP',
                0),
               )
#     .withColumn("saleschannelcode", F.lit("CO"))
     .withColumn(
        "week_level",
        F.when(F.dayofweek("businessdate").isin(1, 7), F.lit("weekend")).otherwise(
            F.lit("weekday")
        ),
    )
   .withColumn("nds", 
                F.col("netdiscountedsalesrevenueamount").cast(DecimalType(20, 4))
               )
    .drop(F.col("netdiscountedsalesrevenueamount"))
    )



one_store_df = spark.table(one_store_tbl).select(one_store_cols)

store_edw_df = (
        spark.table(store_tbl)
        .select(store_cols)
        .withColumn(
            'StoreTimeZone',F.split(F.col('StoreTimeZoneDescription'),' ').getItem(1))
)
# creating dataframe for tender table

# gby_cols = ["businessdate", "storenumber", "transactionnumber", "country_code"]

# tender_df = (
#     spark.table(tender_tbl)
#     .select(tender_cols)
#     .filter(
#         (F.col("businessdate") >= start_dt)
#         & (F.col("businessdate") <= end_dt)
#         & (F.col("transactionhoursid").isNotNull())
#         & (F.col("country_code").isin(country_list))
#         & (F.col("halfhoursid").isNotNull())
#     )
#     .groupBy(gby_cols)
#     .agg(
#         F.first("halfhoursid").alias("halfhoursid"),
#         F.first("transactionhoursid").alias("transactionhoursid"),
#     )
# )

# creating dataframe for half hour dimension table

# half_hour_df = spark.table(half_hour_tbl).select(half_hour_cols)



# creating dataframe for LS transaction table
# creating new cols like customer_type,week_level and sales channel code.
# sales Channel code is added to make the distinction between LS and CO trsanction.
# multiplyig the NDS with -1 to convert it to positive value.

ls_df = (
    spark.table(ls_tbl)
    .select(ls_col)
    .filter(
        (F.col("businessdate") >= start_dt)
        & (F.col("businessdate") <= end_dt)
        & (F.col("saleschannelcode") == "LS")
        & (F.col("accountid") != "0")
        & (F.col("svctransactioninternalrequestname") == "Redemption")
        & (F.col("salessubchannelcode").substr(-2, 2).isin(country_list))
    )
     .withColumn(
        "week_level",
        F.when(F.dayofweek("businessdate").isin(1, 7), F.lit("weekend")).otherwise(
            F.lit("weekday")
        ),
    )
    .withColumn("country_code", F.col("salessubchannelcode").substr(-2, 2))
    .withColumn("customer_type", F.lit("SR"))
#     .withColumn("saleschannelcode", F.lit("LS"))
    .withColumn("nds", F.col("localtransactionamount") * (-1))
    .withColumn("transactionid", F.col("svctransactionNumber").cast(StringType()))
#     .withColumn("transactionnumber", F.col("svctransactionNumber").cast(StringType()))
    .drop("salessubchannelcode", "localtransactionamount", "svctransactionNumber")
)

# creating MOP dataframe.
# filtering the session which converted into orders.

mop_df = (
    spark.table(mop_tbl)
    .select(mop_col)
    .filter(
        (F.col("businessdate") >= start_dt)
        & (F.col("businessdate") <= end_dt)
        & (F.col("guest_checkout_hit") == 0)
        & (F.col("valid_session_fl") == 1)
        & (F.col("siri_session_fl") == 0)
        & (F.col("order_token").isNotNull())
        & (F.col("store_number").isNotNull())
        & (F.col("xid_top").isNotNull())
        & (F.col("country_geo") == "United States")
    )
    .withColumn("storenumber", F.split(F.col("store_number"), "-").getItem(0))
    .withColumn(
        "utc_datetime",
        F.from_unixtime(
            F.substring(F.col("hit_timestamp"), 1, 10), "yyyy-MM-dd HH:mm:ss"
        ).cast("timestamp"),
    )
    .drop("visit_start_time", "store_number")
)


# # creating dataframe for external user id table

xid_df = spark.table(xid_tbl).select(xid_col)

svc_merchid_df = spark.table(svc_ls_tbl).select(svc_cols)

# COMMAND ----------

# DBTITLE 1,Join LS data to SVC_MAP data to get storenumber
# getting the storeno for LS transaction
svc_ls_join_keys = ["transactionalternatemerchantid", "transactionmerchantid"]

ls_one_store_df = ls_df.join(svc_merchid_df, svc_ls_join_keys, "left").drop(
    "transactionalternatemerchantid", "transactionmerchantid"
)

# COMMAND ----------

# DBTITLE 1,Convert MOP UTC timestamp to Local timestamp
mop_store_df = (
      mop_df.join(broadcast(store_edw_df), "storenumber", "left")
)




mop_local_df = (
          mop_store_df
         .withColumn("datetime", F.from_utc_timestamp(F.col("utc_datetime"), F.col("StoreTimeZone")))
)

# COMMAND ----------

# DBTITLE 1,Create Daypart function
def get_daypart(df):

    daypart_df = df.withColumn(
        "daypartcode",
        F.when(
            (F.hour(df.datetime) >= 0)
            & (F.hour(df.datetime) < 9),
            'Early Morning: Before 9 AM',
        )
        .when(
            (F.hour(df.datetime) >= 9)
            & (F.hour(df.datetime) < 11),
            'Mid-Morning: 9 AM - 11 AM',
        )
        .when(
            (F.hour(df.datetime) >= 11)
            & (F.hour(df.datetime) < 14),
            'Lunchtime: 11 AM - 2 PM',
        )
        .when(
            (F.hour(df.datetime) >= 14)
            & (F.hour(df.datetime) < 17),
            'Afternoon: 2 PM - 5 PM',
        )
        .when(
            (F.hour(df.datetime) >= 17)
            & (F.hour(df.datetime) < 24),
            'Evening: After 5 PM',
        )
    )
    return daypart_df

# COMMAND ----------

# DBTITLE 1,Define halfhoursid for LS transactions and MOP session
ls_one_store_daypart_df = get_daypart(ls_one_store_df)
mop_daypart_df = get_daypart(mop_local_df)
# t360_halfhour_df=get_daypart(t360_df)



# COMMAND ----------

# DBTITLE 1,Get retaildaypart for all the transactions 


# # Join t369 with half_hour data to get retaildaypart

# t360_retail_df = t360_halfhour_df.join(
#      half_hour_df, "halfhoursid", "left"
# )

# Join ls data  with half_hour data to get retaildaypart

# ls_one_store_final_df = ls_one_store_halfhour_df.join(
#     half_hour_df, "halfhoursid", "left"
# )

# Join MOP data  with half_hour data to get retaildaypart
# Join MOP data with XID table to get accountid


mop_final_df = mop_daypart_df.join(xid_df, "xid_top", "left")


gb_cols_mop = ["businessdate", "daypartcode", "accountid", "storenumber"]

mop_final_order_df = (
    mop_final_df.withColumn(
        "app",
        F.when(
            (F.col("user_authenticated_session") == "TRUE")
            & (F.col("user_authenticated_session") == "YES"),
            F.col("session_id"),
        ),
    )
    .groupBy(gb_cols_mop)
    .agg(
        F.countDistinct("session_id").alias("session_count"),
        F.countDistinct("app").alias("app_session_count"),
    )
)

# COMMAND ----------

# DBTITLE 1,Merge line item and LS store data

def merge_data(df_left, df_right):
  
    left_types = {f.name: f.dataType for f in df_left.schema}
    right_types = {f.name: f.dataType for f in df_right.schema}
    left_fields = set((f.name, f.dataType, f.nullable) for f in df_left.schema)
    right_fields = set((f.name, f.dataType, f.nullable) for f in df_right.schema)

    # First go over left-unique fields
    for l_name, l_type, l_nullable in left_fields.difference(right_fields):
        if l_name in right_types:
            r_type = right_types[l_name]
        df_right = df_right.withColumn(l_name, F.lit(None).cast(l_type))

    # Now go over right-unique fields
    for r_name, r_type, r_nullable in right_fields.difference(left_fields):
        if r_name in left_types:
            l_type = left_types[r_name]
        df_left = df_left.withColumn(r_name, F.lit(None).cast(r_type))    

    # Make sure columns are in the same order
    df_left = df_left.select(df_right.columns)
    return df_left.union(df_right)


t360_ls_df=merge_data(t360_df,ls_one_store_daypart_df)

# COMMAND ----------

# DBTITLE 1,Join merged data with all other data sets
t360_onestore_join_keys = ["storenumber", "country_code"]
mop_join_keys = ["businessdate", "daypartcode", "accountid", "storenumber"]


t360_ls_final_df = (
   t360_ls_df.join(mop_final_order_df, mop_join_keys, "left")
    .join(one_store_df, t360_onestore_join_keys, "left")

)



# Join line item with LTO-promo data to get lto_flg
t360_lto_df = (
    t360_ls_final_df.alias("B")
    .join(
        promo_df.alias("A"),
        (F.col("A.itemid") == F.col("B.itemid"))
        & (
            F.col("B.businessdate").between(
                F.col("A.start_date"), F.col("A.lto_8wk_date")
            )
        ),
        "left",
    )
    .drop(F.col("A.itemid"))
)

t360_lto_final_df = t360_lto_df.withColumn(
    "lto_flg", F.when(F.col("lto_name").isNull(), F.lit(0)).otherwise(F.lit(1))
).withColumn(
    "dt_flg",
    F.when(
        (F.col("customerorderchannelname") == "OTW")
        & (
            (F.col("store_type") == "Drive Thru")
            | (F.col("store_type") == "Drive Thru Only")
        ),
        F.lit(1),
    ).otherwise(F.lit(0)),
)

# COMMAND ----------

# DBTITLE 1,group by columns for base data,middle layer data and MOP Session data
# Grouop by cols for the middle layer table
gb_cols_base = [
    "fiscalweekinyearnumber",
    "fiscalyearnumber",
    "week_level",
    "daypartcode",
    "customer_type",
    "accountid",
    "loyaltyprogramname",
    "country_code",
    "storenumber",
    "itemid",
    "moptransactionind",
    "ownership_type",
    "customerorderchannelname",
    "lto_flg",
    "dt_flg"
]


# COMMAND ----------

# DBTITLE 1,Aggregate data and create Base table
t360_final_agg_base_df=(
  t360_lto_final_df
  .withColumn('mop_tras',
    F.when(F.col('moptransactionind') == 1,
           F.col('transactionid'))
  )
  .withColumn('total_modifier_qty',
    F.when(F.col('childlinesequencenumber') > 0,
           F.col('itemquantity'))
  )
  .withColumn('unpaid_modifier_qty',
    F.when((F.col('childlinesequencenumber') > 0) &(F.col('nds')==0),
           F.col('itemquantity'))
  )
  .withColumn('paid_modifier_qty',
    F.when((F.col('childlinesequencenumber') > 0) &(F.col('nds')>0),
           F.col('itemquantity'))
  )
  .groupBy(gb_cols_base)
  .agg(
  F.sum('nds').alias('total_nds'),
  F.round((F.sum("session_count")) / (F.count("itemid")), 0).alias("session_count"),
  F.round((F.sum("app_session_count")) / (F.count("itemid")), 0).alias("app_session_count"),
#   F.countDistinct('accountid').alias('unique_user'),
  F.sum('finallineitempriceamount').alias('total_grosssales'),
  F.sum('itemquantity').alias('total_qty'),
  F.countDistinct('transactionid').alias('total_trans'),
  F.countDistinct('mop_tras').alias('order_ahead_trans'),
  F.sum('total_modifier_qty').alias('total_modifier_qty'),
  F.sum('unpaid_modifier_qty').alias('unpaid_modifier_qty'),
  F.sum('paid_modifier_qty').alias('paid_modifier_qty')

     )
)



# COMMAND ----------

# DBTITLE 1,Creating Array base table to reduce the size 

# base_array_df = (
#                         line_item_final_agg_base_df.groupBy(gbb)
#                         .agg(F.collect_list('ItemNumber').alias('itemsnumber'),
#                         F.collect_list('total_nds').alias('array_nds'),
#                         F.sum('total_nds').alias('array_nds'),
#                         F.collect_list('unique_user').alias('array_unique_user'),
#                         F.sum('unique_user').alias('total_unique_user'),
#                         F.collect_list('total_qty').alias('array_qty'),
#                         F.sum('total_qty').alias('total_qty'),
#                         F.collect_list('total_trans').alias('array_total_trans'),
#                         F.sum('total_trans').alias('total_trans'),
#                         F.collect_list('session_count').alias('array_session_count'),
#                         F.sum('session_count').alias('Total_session_count'),
#                         F.collect_list('total_modifier_qty').alias('array_total_modifier_qty'),
#                         F.sum('total_modifier_qty').alias('total_modifier_qty')
#                    )


# COMMAND ----------

t360_final_agg_base_df.createOrReplaceTempView("my_temp_table1");

spark.sql("create table cma_teams_store360.donotuse_cw_base_table_t360_1month as select * from my_temp_table1"); 

# COMMAND ----------

# DBTITLE 1,Save Dataframe as a Table
# (
#     middle_layer_final_df
#     .write
#     .mode('overwrite')
#     .save(out_tbl_middle)
# )

# # (
# #     base_array_df
# #     .write
# #     .mode('overwrite')
# #     .save(out_tbl_base)
# # )



