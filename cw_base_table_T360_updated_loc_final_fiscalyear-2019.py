# Databricks notebook source
# DBTITLE 1,Imports &Variables
# Importing all the libs
from datetime import date

from pyspark.sql import functions as F
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
spark.conf.set("spark.sql.broadcastTimeout",  2000)

# Input & Output Tablenames

t360_tbl="edap_pub_customersales.customer_sales_transaction360_line_item"
prod_hier_tbl = "edap_pub_productitem.enterprise_product_hierarchy"
one_store_tbl = "cma_teams_store360.s360_one_store_kpi"
store_tbl = "edap_pub_store.store_edw_restricted"
ls_tbl = "edap_pub_customersales.customer_svc_transaction_history_restricted"
svc_ls_tbl = "cma_teams_store360.s360_cw_svc_mrcht_ls_map"
mop_seg_tbl='cdl_prod_intermediate.mobile_clickstream_mop_segmentation'
# cal_tbl = "edap_pub_time.calendar"
# out_tbl_middle = "cma_teams_store360.donotuse_ch_middle_layer_2years"
# out_tbl_base = "cma_teams_store360.donotuse_ch_item_dec_agg_weekly_base_array"
# out_tbl_mop = "cma_teams_store360.donotuse_ch_mop_base_final"


# List of column names
t360_cols=[
    "accountid",
    "loyaltyprogramname",
    "weekdayind",
    "fiscalyearnumber",
    "fiscalweekinyearnumber",
    "fiscalperiodinyearnumber",
    "fiscalquarterinyearnumber",
    "fiscalweekbegindate",
    "daypartcode",
    F.col("salestransactionid").alias("transactionid") ,
    "salestransactionutcdate",
    "storenumber",
     F.col("salestransactionlocaldatetime").alias("datetime"),
    "businessdate",
    "customertransactionind",
    "customerorderchannelname", 
    "voidflag",
    "itemid",
    "revenueitemind",
    "itemquantity", 
    "finallineitempriceamount",
    F.col("netdiscountedsalesrevenueamount").cast(DecimalType(20, 4)).alias("nds"),
    F.col('storecountrycode').alias('country_code')
]

prod_hier_cols = [
    "itemid",
#     "producttypedescription",
#     "productcategorydescription",
    "productstyledescription"
#     "NotionalProductDescription",
]


one_store_cols = [
    "storenumber",
    "ownership_type",
    "country_code",
    "region_nm",
    "district_id",
    "district_name",
  F.col("region_id").cast(IntegerType()).alias("region_id")
]

ls_col = [
    "accountid",
    "businessdate",
    "fiscalyearnumber",
    "fiscalquarterinyearnumber",
    "fiscalperiodinyearnumber",
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

mop_seg_cols=[F.col("guid_id").alias("accountid"),F.col("pt_date").alias("businessdate"),"mop_segment"]


country_list = ["US"]
start_dt = date(2019, 6, 24) 
# start_dt = date(2018, 10, 1)  
# end_dt = date(2019, 9, 29) 
end_dt = date(2019, 6, 24) 


# COMMAND ----------

# DBTITLE 1,Load Tables in Dataframe
# creating Dataframe for T360 table.
# creating new cols like customer_type,week_level
# changing the type of NDS so that it will get merge with LS trasacntion data

# there is Weekdayind to identify weekend or weekday

t360_df = (
    spark.table(t360_tbl)
    .select(t360_cols)
    .filter((F.col("customertransactionind") != 0)
            & F.col("businessdate").between(start_dt, end_dt)
            & F.col("country_code").isin(country_list)
            & (F.col("voidflag") != "Y")
            & (F.col("revenueitemind") == 1))
    .withColumn("customer_type",
                F.when(F.col("accountid").isNotNull()
                       & F.col("loyaltyprogramname").isNotNull(), F.lit("SR"))
                .otherwise(F.lit("NON_SR")))
    .withColumn("moptransactionind",
                (F.col("customerorderchannelname") == 'MOP').cast(IntegerType()))
#     .withColumn("saleschannelcode", F.lit("CO"))
#      .withColumn(
#         "week_level",
#         F.when(F.dayofweek("businessdate").isin(1, 7), F.lit("weekend")).otherwise(
#             F.lit("weekday")
#         ),
#     )
)



prod_hier_df = spark.table(prod_hier_tbl).select(prod_hier_cols)

one_store_df = (
   spark.table(one_store_tbl)
  .select(one_store_cols)
  .filter(F.col("country_code").isin(country_list))
 .withColumn("region",F.concat(F.col("region_id"), F.lit("_"), F.col("region_nm")))
 .withColumn("district",F.concat(F.col("district_id"), F.lit("_"), F.col("district_name")))
  .drop("region_id","region_nm","district_id","district_name")
)

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
  .withColumn("weekdayind",
              F.when(
                F.dayofweek("businessdate").isin(1, 7),
                1).when(F.dayofweek("businessdate").isin(2,3,4,5,6),
                0),
               )
#      .withColumn(
#         "week_level",
#         F.when(F.dayofweek("businessdate").isin(1, 7), F.lit("weekend"))
#        .otherwise(
#             F.lit("weekday")
#         ),
#     )
#     .withColumn("col_name", F.when(col("col_name").isNotNull, col("col_name")).otherwise(lit(null)))
#       .withColumn("col_name", when(col("col_name").isNotNull(), col("col_name")).otherwise(lit(None)))
     .withColumn("channel", F.lit("LS"))
    .withColumn("customerorderchannelname", F.when(F.col("channel").isNotNull(), F.col("channel")).otherwise(F.lit(None)))
    .withColumn("country_code", F.col("salessubchannelcode").substr(-2, 2))
    .withColumn("customer_type", F.lit("SR"))
    .withColumn("nds", F.col("localtransactionamount") * (-1))
    .withColumn("transactionid", F.col("svctransactionNumber").cast(StringType()))
    .drop("salessubchannelcode", "localtransactionamount", "svctransactionNumber", "channel")
)



# # # creating dataframe for external user id table
svc_merchid_df = spark.table(svc_ls_tbl).select(svc_cols)

## creating mop_segment_dataframe
mop_seg_df = (
        spark.table(mop_seg_tbl)
       .select(mop_seg_cols)
       .filter(F.col("businessdate") <=  end_dt)
)

# COMMAND ----------

# DBTITLE 1,Join LS data to SVC_MAP data to get storenumber
# getting the storeno for LS transaction
svc_ls_join_keys = ["transactionalternatemerchantid", "transactionmerchantid"]

ls_one_store_df = ls_df.join(svc_merchid_df, svc_ls_join_keys, "left").drop(
    "transactionalternatemerchantid", "transactionmerchantid"
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
# mop_daypart_df = get_daypart(mop_local_df)
# t360_halfhour_df=get_daypart(t360_df)



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
mop_seg_join_keys_part1= ["businessdate", "accountid",]
mop_seg_join_keys_part2= ["accountid"]

mop_segment_part=(
  mop_seg_df.groupBy("accountid")
  .agg((F.min(F.col("businessdate")).alias("businessdate"))
      )
  .distinct()
)

join_cols=['accountid','businessdate']

mop_segment_part2=(
  mop_segment_part.alias("A").join((mop_seg_df.alias("B")),join_cols,'inner').select("A.accountid","B.mop_segment")
)

t360_ls_final_df = (
   t360_ls_df
    .join(one_store_df, t360_onestore_join_keys, "left")
    .join(prod_hier_df, "itemid", "left")
)

t360_ls_final_part1_df = (
   t360_ls_final_df.filter(F.col("businessdate")>='2019-06-23')
   .join(mop_seg_df,mop_seg_join_keys_part1,'left')
)

t360_ls_final_part2_df = (
   t360_ls_final_df.filter(F.col("businessdate")<'2019-06-23')
   .join(mop_segment_part2,mop_seg_join_keys_part2,'left')
)

t360_ls_final_part_data=merge_data(t360_ls_final_part1_df,t360_ls_final_part2_df)

t360_ls_final_df1 = (
           t360_ls_final_part_data
           .fillna( { 'region':'NA','country_code':'US', 'district':'NA', 'mop_segment':'no_mop_segment'} )
)

t360_ls_final_df2 = (
  t360_ls_final_df1.withColumn("productstyledescription",
                               F.when(F.col("customerorderchannelname")=='LS', F.col("productstyledescription")
                                     )
                               .when((F.col("customerorderchannelname")!='LS')
                                     & (F.col("productstyledescription").isNull()), F.lit('NA')
                                    )
                               .otherwise(F.col("productstyledescription"))
                              )
  .drop("datetime","fiscalweekbegindate","voidflag","customertransactionind",
        "revenueitemind",'salestransactionutcdate')
)

# COMMAND ----------

t360_ls_final_df2.createOrReplaceTempView("my_tempmop_table39");

spark.sql("create table cma_teams_store360.dce_base_2019 as select * from my_tempmop_table39");
