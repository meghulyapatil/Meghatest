# Databricks notebook source
# DBTITLE 1,Imports &Variables

# Importing all the libs
from pyspark.sql import functions as F
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
line_item_tbl = "edap_pub_customersales.legacy_pos_line_item"
prod_hier_tbl = "edap_pub_productitem.enterprise_product_hierarchy"
one_store_tbl = "cma_teams_store360.s360_one_store_kpi"
store_tbl = "edap_pub_store.store_edw_restricted"
tender_tbl = "edap_pub_customersales.legacy_pos_tender"
half_hour_tbl = "edap_pub_time.half_hour_dimension"
cust_tbl = "cma_teams_store360.donotuse_customer_segment_all"
promo_tbl = "cdl_prod_intermediate.product_workbench_ingest_promo_ingest"
ls_tbl = "edap_pub_customersales.customer_svc_transaction_history_restricted"
mop_tbl = "cdl_prod_publish.mobile_clickstream_hits"
xid_tbl = "edap_pub_customer.enterprise_customer_external_id_mapping_restricted"
cus_tbl = "edap_pub_customer.customer360_behavior_weekly_snapshot_restricted"
cal_tbl = "edap_pub_time.calendar"
svc_ls_tbl = "cma_teams_store360.s360_cw_svc_mrcht_ls_map"
out_tbl_middle = "cma_teams_store360.donotuse_ch_middle_layer_2years"
out_tbl_base = "cma_teams_store360.donotuse_ch_item_dec_agg_weekly_base_array"
out_tbl_mop = "cma_teams_store360.donotuse_ch_mop_base_final"


# List of column names

line_item_cols = [
    "businessdate",
    "fiscalyearnumber",
    "fiscalweekinyearnumber",
    "storenumber",
    F.col("storecountrycode").alias("country_code"),
    F.col("POSTransactionHQDTM").alias("datetime"),
    F.col("POSTransactionUTCDTM").alias("utc_datetime"),
    "transactionid",
    "posorderchannelcode",
    "transactionnumber",
    "customertransactionind",
    "accountid",
    "loyaltyprogramname",
    "loyaltyprogramlevelname",
    "grosssaleslocalamount",
    "grosslineitemqty",
    "netdiscountedsalesamount",
    "starbucksmobileappscanind",
    "otherscanind",
    "moptransactionind",
    "itemnumber",
    "parentitemmodifiedind",
    "modifierind",
    "operatingsystemname",
]

prod_hier_cols = [
    F.col("itemid").alias("itemnumber"),
    "producttypedescription",
    "productcategorydescription",
    "productstyledescription",
    "NotionalProductDescription",
]


one_store_cols = [
    "storenumber",
    "cbsa_code",
    "store_type",
    "state_code",
    "long_num",
    "lat_num",
    "area_id",
    "region_id",
    "division_id",
    "district_name",
    "city_name",
    "county_name",
    "ownership_type",
    "country_code",
]

store_cols = [
  "storenumber",
  "StoreTimeZoneDescription"
]
tender_cols = [
    "businessdate",
    "storenumber",
    F.col("storecountrycode").alias("country_code"),
    "transactionnumber",
    F.col("transactionhalfhoursid").alias("halfhoursid"),
    "transactionhoursid",
]

half_hour_cols = ["halfhoursid", "retaildaypartname"]


promo_cols = ["lto_name", "start_date", F.col("item_id").alias("itemnumber")]

ls_col = [
    "accountid",
    "businessdate",
    "fiscalyearnumber",
    "fiscalweekinyearnumber",
    "transactionmerchantid",
    "transactionalternatemerchantid",
    "svctransactionNumber",
    "localtransactionamount",
    "mobiledeviceid",
    "salessubchannelcode",
    "loyaltyprogramname",
    "loyaltyprogramlevelname",
    F.col("touchpayind").alias("starbucksmobileappscanind"),
    F.col("mobileorderpayind").alias("moptransactionind"), 
    F.col("registerlocaldatetime").alias("datetime"),
    F.col("deviceplatformname").alias("operatingsystemname"),
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
    "hit_timestamp",
    "visit_number",
]

xid_col = [F.col("externaluserid").alias("xid_top"), "accountid"]

cus_cols = [
    F.col("fiscalweekbegindate").alias("calendardate"),
    "accountid",
    "customerlastMOPtransactiondate",
    "loyaltymembertenuredays",
    "loyaltyprogramname",
]
cal_cols = ["calendardate", "fiscalweekinyearnumber", "fiscalyearnumber"]


lpn = ["MSR_USA"]

country_list = ["US"]

# year_list=[2020]

# week_list=[9]

start_dt = '2020-07-12'
end_dt =   '2020-07-12'

# start_dt = "2019-12-02"
# end_dt = "2020-02-02"

# COMMAND ----------

# DBTITLE 1,Load Tables in Dataframe
# Creating dataframe for promotional data
# Adding new col which is 8 weeks in future from the start date of LTO

promo_df = (
    spark.table(promo_tbl)
    .select(promo_cols)
    .filter((F.col("country").isin(country_list)))
    .withColumn("lto_8wk_date", F.date_add(F.col("start_date"), 56))
)

# creating Dataframe for LEGACY_POS_LINE_ITEM table.
# creating new cols like customer_type,week_level and sales channel code.
# sales Channel code is added to make the distinction between LS and CO trsanction.
# changing the type of NDS so that it will get merge with LS trasacntion data

line_item_df = (
    spark.table(line_item_tbl)
    .select(line_item_cols)
    .filter(
        (F.col("customertransactionind") != 0)
        & (F.col("businessdate") >= start_dt)
        & (F.col("businessdate") <= end_dt)
        & (F.col("country_code").isin(country_list))
    )
    .withColumn(
        "customer_type",
        F.when(
            (F.col("accountid").isNotNull())
            & (F.col("loyaltyprogramname").isNotNull()),
            F.lit("SR"),
        ).otherwise(F.lit("NON_SR")),
    )
    .withColumn(
        "week_level",
        F.when(F.dayofweek("businessdate").isin(1, 7), F.lit("weekend")).otherwise(
            F.lit("weekday")
        ),
    )
    .withColumn("saleschannelcode", F.lit("CO"))
    .withColumn("nds", F.col("netdiscountedsalesamount").cast(DecimalType(20, 4)))
    .drop(F.col("netdiscountedsalesamount"))
)


prod_hier_df = spark.table(prod_hier_tbl).select(prod_hier_cols)

one_store_df = spark.table(one_store_tbl).select(one_store_cols)

store_edw_df = (
        spark.table(store_tbl)
        .select(store_cols)
        .withColumn(
            'StoreTimeZone',F.split(F.col('StoreTimeZoneDescription'),' ').getItem(1))
           )
# creating dataframe for tender table

gby_cols = ["businessdate", "storenumber", "transactionnumber", "country_code"]

tender_df = (
    spark.table(tender_tbl)
    .select(tender_cols)
    .filter(
        (F.col("businessdate") >= start_dt)
        & (F.col("businessdate") <= end_dt)
        & (F.col("transactionhoursid").isNotNull())
        & (F.col("country_code").isin(country_list))
        & (F.col("halfhoursid").isNotNull())
    )
    .groupBy(gby_cols)
    .agg(
        F.first("halfhoursid").alias("halfhoursid"),
        F.first("transactionhoursid").alias("transactionhoursid"),
    )
)

# creating dataframe for half hour dimension table

half_hour_df = spark.table(half_hour_tbl).select(half_hour_cols)

# creating dataframe for customer segment table
cust_df = spark.table(cust_tbl)

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
    .withColumn("saleschannelcode", F.lit("LS"))
    .withColumn("nds", F.col("localtransactionamount") * (-1))
    .withColumn("transactionid", F.col("svctransactionNumber").cast(StringType()))
    .withColumn("transactionnumber", F.col("svctransactionNumber").cast(StringType()))
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

# creating dataframe for customer behaviour weekly snapshot and calendar

cus_df = (
    spark.table(cus_tbl)
    .select(cus_cols)
    .filter((F.col("loyaltyprogramname").isin(lpn)))
    .withColumn("mopdate", F.date_add(F.col("customerlastMOPtransactiondate"), 30))
)


cal_df = spark.table(cal_tbl).select(cal_cols)


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
      mop_df.join(store_edw_df, "storenumber", "left")
)


# def get_timezone(longitude, latitude):
#     from timezonefinder import TimezoneFinder

#     tzf = TimezoneFinder()
#     return tzf.timezone_at(lng=longitude, lat=latitude)


# udf_timezone = F.udf(get_timezone, StringType())
# # udf_timezone = F.udf(lambda ts: get_timezone(ts) if ts is not None else None, StringType())


mop_local_df = (
          mop_store_df
         .withColumn("datetime", F.from_utc_timestamp(F.col("utc_datetime"), F.col("StoreTimeZone")))
)

# COMMAND ----------

# DBTITLE 1,Create Halfhoursid function
def get_half_hour_id(df):

    halfhour_df = df.withColumn(
        "halfhoursid",
        F.when(
            (F.hour(df.datetime) >= 0)
            & (F.hour(df.datetime) < 1)
            & (F.minute(df.datetime) < 30),
            0,
        )
        .when(
            (F.hour(df.datetime) >= 0)
            & (F.hour(df.datetime) < 1)
            & (F.minute(df.datetime) >= 30),
            3000,
        )
        .when(
            (F.hour(df.datetime) >= 1)
            & (F.hour(df.datetime) < 2)
            & (F.minute(df.datetime) < 30),
            10000,
        )
        .when(
            (F.hour(df.datetime) >= 1)
            & (F.hour(df.datetime) < 2)
            & (F.minute(df.datetime) >= 30),
            13000,
        )
        .when(
            (F.hour(df.datetime) >= 2)
            & (F.hour(df.datetime) < 3)
            & (F.minute(df.datetime) < 30),
            20000,
        )
        .when(
            (F.hour(df.datetime) >= 2)
            & (F.hour(df.datetime) < 3)
            & (F.minute(df.datetime) >= 30),
            23000,
        )
        .when(
            (F.hour(df.datetime) >= 3)
            & (F.hour(df.datetime) < 4)
            & (F.minute(df.datetime) < 30),
            30000,
        )
        .when(
            (F.hour(df.datetime) >= 3)
            & (F.hour(df.datetime) < 4)
            & (F.minute(df.datetime) >= 30),
            33000,
        )
        .when(
            (F.hour(df.datetime) >= 4)
            & (F.hour(df.datetime) < 5)
            & (F.minute(df.datetime) < 30),
            40000,
        )
        .when(
            (F.hour(df.datetime) >= 4)
            & (F.hour(df.datetime) < 5)
            & (F.minute(df.datetime) >= 30),
            43000,
        )
        .when(
            (F.hour(df.datetime) >= 5)
            & (F.hour(df.datetime) < 6)
            & (F.minute(df.datetime) < 30),
            50000,
        )
        .when(
            (F.hour(df.datetime) >= 5)
            & (F.hour(df.datetime) < 6)
            & (F.minute(df.datetime) >= 30),
            53000,
        )
        .when(
            (F.hour(df.datetime) >= 6)
            & (F.hour(df.datetime) < 7)
            & (F.minute(df.datetime) < 30),
            60000,
        )
        .when(
            (F.hour(df.datetime) >= 6)
            & (F.hour(df.datetime) < 7)
            & (F.minute(df.datetime) >= 30),
            63000,
        )
        .when(
            (F.hour(df.datetime) >= 7)
            & (F.hour(df.datetime) < 8)
            & (F.minute(df.datetime) < 30),
            70000,
        )
        .when(
            (F.hour(df.datetime) >= 7)
            & (F.hour(df.datetime) < 8)
            & (F.minute(df.datetime) >= 30),
            73000,
        )
        .when(
            (F.hour(df.datetime) >= 8)
            & (F.hour(df.datetime) < 9)
            & (F.minute(df.datetime) < 30),
            80000,
        )
        .when(
            (F.hour(df.datetime) >= 8)
            & (F.hour(df.datetime) < 9)
            & (F.minute(df.datetime) >= 30),
            83000,
        )
        .when(
            (F.hour(df.datetime) >= 9)
            & (F.hour(df.datetime) < 10)
            & (F.minute(df.datetime) < 30),
            90000,
        )
        .when(
            (F.hour(df.datetime) >= 9)
            & (F.hour(df.datetime) < 10)
            & (F.minute(df.datetime) >= 30),
            93000,
        )
        .when(
            (F.hour(df.datetime) >= 10)
            & (F.hour(df.datetime) < 11)
            & (F.minute(df.datetime) < 30),
            100000,
        )
        .when(
            (F.hour(df.datetime) >= 10)
            & (F.hour(df.datetime) < 11)
            & (F.minute(df.datetime) >= 30),
            103000,
        )
        .when(
            (F.hour(df.datetime) >= 11)
            & (F.hour(df.datetime) < 12)
            & (F.minute(df.datetime) < 30),
            110000,
        )
        .when(
            (F.hour(df.datetime) >= 11)
            & (F.hour(df.datetime) < 12)
            & (F.minute(df.datetime) >= 30),
            113000,
        )
        .when(
            (F.hour(df.datetime) >= 12)
            & (F.hour(df.datetime) < 13)
            & (F.minute(df.datetime) < 30),
            120000,
        )
        .when(
            (F.hour(df.datetime) >= 12)
            & (F.hour(df.datetime) < 13)
            & (F.minute(df.datetime) >= 30),
            123000,
        )
        .when(
            (F.hour(df.datetime) >= 13)
            & (F.hour(df.datetime) < 14)
            & (F.minute(df.datetime) < 30),
            130000,
        )
        .when(
            (F.hour(df.datetime) >= 13)
            & (F.hour(df.datetime) < 14)
            & (F.minute(df.datetime) >= 30),
            133000,
        )
        .when(
            (F.hour(df.datetime) >= 14)
            & (F.hour(df.datetime) < 15)
            & (F.minute(df.datetime) < 30),
            140000,
        )
        .when(
            (F.hour(df.datetime) >= 14)
            & (F.hour(df.datetime) < 15)
            & (F.minute(df.datetime) >= 30),
            143000,
        )
        .when(
            (F.hour(df.datetime) >= 15)
            & (F.hour(df.datetime) < 16)
            & (F.minute(df.datetime) < 30),
            150000,
        )
        .when(
            (F.hour(df.datetime) >= 15)
            & (F.hour(df.datetime) < 16)
            & (F.minute(df.datetime) >= 30),
            153000,
        )
        .when(
            (F.hour(df.datetime) >= 16)
            & (F.hour(df.datetime) < 17)
            & (F.minute(df.datetime) < 30),
            160000,
        )
        .when(
            (F.hour(df.datetime) >= 16)
            & (F.hour(df.datetime) < 17)
            & (F.minute(df.datetime) >= 30),
            163000,
        )
        .when(
            (F.hour(df.datetime) >= 17)
            & (F.hour(df.datetime) < 18)
            & (F.minute(df.datetime) < 30),
            170000,
        )
        .when(
            (F.hour(df.datetime) >= 17)
            & (F.hour(df.datetime) < 18)
            & (F.minute(df.datetime) >= 30),
            173000,
        )
        .when(
            (F.hour(df.datetime) >= 18)
            & (F.hour(df.datetime) < 19)
            & (F.minute(df.datetime) < 30),
            180000,
        )
        .when(
            (F.hour(df.datetime) >= 18)
            & (F.hour(df.datetime) < 19)
            & (F.minute(df.datetime) >= 30),
            183000,
        )
        .when(
            (F.hour(df.datetime) >= 19)
            & (F.hour(df.datetime) < 20)
            & (F.minute(df.datetime) < 30),
            190000,
        )
        .when(
            (F.hour(df.datetime) >= 19)
            & (F.hour(df.datetime) < 20)
            & (F.minute(df.datetime) >= 30),
            193000,
        )
        .when(
            (F.hour(df.datetime) >= 20)
            & (F.hour(df.datetime) < 21)
            & (F.minute(df.datetime) < 30),
            200000,
        )
        .when(
            (F.hour(df.datetime) >= 20)
            & (F.hour(df.datetime) < 21)
            & (F.minute(df.datetime) >= 30),
            203000,
        )
        .when(
            (F.hour(df.datetime) >= 21)
            & (F.hour(df.datetime) < 22)
            & (F.minute(df.datetime) < 30),
            210000,
        )
        .when(
            (F.hour(df.datetime) >= 21)
            & (F.hour(df.datetime) < 22)
            & (F.minute(df.datetime) >= 30),
            213000,
        )
        .when(
            (F.hour(df.datetime) >= 22)
            & (F.hour(df.datetime) < 23)
            & (F.minute(df.datetime) < 30),
            220000,
        )
        .when(
            (F.hour(df.datetime) >= 22)
            & (F.hour(df.datetime) < 23)
            & (F.minute(df.datetime) >= 30),
            223000,
        )
        .when(
            (F.hour(df.datetime) >= 23)
            & (F.hour(df.datetime) < 24)
            & (F.minute(df.datetime) < 30),
            230000,
        )
        .when(
            (F.hour(df.datetime) >= 23)
            & (F.hour(df.datetime) < 24)
            & (F.minute(df.datetime) >= 30),
            233000,
        )
        .otherwise(999999),
    )
    return halfhour_df

# COMMAND ----------

# DBTITLE 1,Define halfhoursid for LS transactions and MOP session
ls_one_store_halfhour_df = get_half_hour_id(ls_one_store_df)
mop_halfhour_df = get_half_hour_id(mop_local_df)

# COMMAND ----------

# DBTITLE 1,Get retaildaypart for all the transactions 
tender_join_df = tender_df.join(half_hour_df, "halfhoursid", "left")

lnitem_tender_join_keys = [
    "storenumber",
    "country_code",
    "businessdate",
    "transactionnumber",
]

# Join line item with tender data to get retaildaypart

line_item_tender_df = line_item_df.join(
    tender_join_df, lnitem_tender_join_keys, "left"
).drop("transactionhoursid")

# Join ls data  with half_hour data to get retaildaypart

ls_one_store_final_df = ls_one_store_halfhour_df.join(
    half_hour_df, "halfhoursid", "left"
)

# Join MOP data  with half_hour data to get retaildaypart
# Join MOP data with XID table to get accountid


mop_final_df = mop_halfhour_df.join(xid_df, "xid_top", "left").join(
    half_hour_df, "halfhoursid", "left"
)


gb_cols_mop = ["businessdate", "retaildaypartname", "accountid", "storenumber"]

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


line_item_ls_final_df=merge_data(line_item_tender_df,ls_one_store_final_df)

# COMMAND ----------

# DBTITLE 1,Join merged data with all other data sets
lnitem_onestore_join_keys = ["storenumber", "country_code"]
mop_join_keys = ["businessdate", "retaildaypartname", "accountid", "storenumber"]


line_item_final_df = (
    line_item_ls_final_df.join(mop_final_order_df, mop_join_keys, "left")
    .join(one_store_df, lnitem_onestore_join_keys, "left")
    .join(cust_df, "accountid", "left")
    .join(prod_hier_df, "itemnumber", "left")
)

# line_item_final_df = (
#                       line_item_ls_final_df
#                       .join(one_store_df, lnitem_onestore_join_keys, 'left')
#                       .join(cust_df, 'accountid', 'left')
#                       .join(prod_hier_df, 'itemnumber', 'left')
#                     )


# Join line item with LTO-promo data to get lto_flg
line_item_lto_df = (
    line_item_final_df.alias("B")
    .join(
        promo_df.alias("A"),
        (F.col("A.itemnumber") == F.col("B.itemnumber"))
        & (
            F.col("B.businessdate").between(
                F.col("A.start_date"), F.col("A.lto_8wk_date")
            )
        ),
        "left",
    )
    .drop(F.col("A.itemnumber"))
)

line_item_lto_final_df = line_item_lto_df.withColumn(
    "lto_flg", F.when(F.col("lto_name").isNull(), F.lit(0)).otherwise(F.lit(1))
).withColumn(
    "dt_flg",
    F.when(
        (F.col("posorderchannelcode") == "OTW")
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
gb_cols_middle = [
    "fiscalweekinyearnumber",
    "fiscalyearnumber",
    "week_level",
    "retaildaypartname",
    "customer_type",
    "loyaltyprogramname",
    "loyaltyprogramlevelname",
    "operatingsystemname",
    "mop_segment",
    "routine_segment",
    "temperature_segment",
    "country_code",
    "cbsa_code",
    "state_code",
    "county_name",
    "city_name",
    "area_id",
    "region_id",
    "division_id",
    "ownership_type",
    "posorderchannelcode",
    "NotionalProductDescription",
    "producttypedescription",
    "productcategorydescription",
    "productstyledescription",
    "lto_flg",
    "dt_flg",
]


# # Grouop by cols for the base layer table
# gb_cols_base = [
#  'accountid','customerid','customer_type',
#  'fiscalweekinyearnumber','fiscalyearnumber','week_level',
#  'retaildaypartname','storenumber','country_code',
#  'saleschannelcode','posorderchannelcode',
#  'loyaltyprogramname','loyaltyprogramlevelname',
#  'operatingsystemname','itemnumber','modifierind','lto_flg','dt_flg']

# COMMAND ----------

# DBTITLE 1,aggregate data and create middle layer table
line_item_final_agg_middle_df = (
    line_item_lto_final_df
      .withColumn('mop_trans',
        F.when(F.col('moptransactionind') == 1,
               F.col('transactionid'))
      )
      .withColumn('mop_nds',
        F.when(F.col('moptransactionind') == 1,
               F.col('nds'))
      )
      .withColumn('mop_qty',
        F.when((F.col('moptransactionind') == 1) & (F.col('modifierind')==0),
               F.col('grosslineitemqty'))
      )
      .withColumn('mop_customer',
        F.when(F.col('moptransactionind') == 1,
               F.col('accountid'))
      )
      .withColumn('mobilescan_trans',
        F.when(F.col('starbucksmobileappscanind') == 1,
               F.col('transactionid'))
      )
       .withColumn('mobilescan_nds',
        F.when(F.col('starbucksmobileappscanind') == 1,
               F.col('nds'))
      )
      .withColumn('mobilescan_qty',
        F.when((F.col('starbucksmobileappscanind') == 1) & (F.col('modifierind')==0),
               F.col('grosslineitemqty'))
      )
      .withColumn('mobilescan_customer',
        F.when(F.col('starbucksmobileappscanind') == 1,
               F.col('accountid'))
      )
    .withColumn(
        "total_modifier_qty",
        F.when(F.col("modifierind") == 1, F.col("grosslineitemqty")),
    )
    .withColumn(
        "unpaid_modifier_qty",
        F.when(
            (F.col("modifierind") == 1) & (F.col("nds") == 0), F.col("grosslineitemqty")
        ),
    )
    .withColumn(
        "paid_modifier_qty",
        F.when(
            (F.col("modifierind") == 1) & (F.col("nds") > 0), F.col("grosslineitemqty")
        ),
    )
    .withColumn(
        "LS_trans", F.when(F.col("SalesChannelCode") == "LS", F.col("transactionid"))
    )
    .withColumn(
        "SR_customer", F.when(F.col("customer_type") == "SR", F.col("accountid"))
    )
    .withColumn(
        "SR_MOP_customer",
        F.when(
            (F.col("customer_type") == "SR") & (F.col("moptransactionind") == "1"),
            F.col("accountid"),
        ),
    )
    .withColumn(
        "USCO_transaction",
        F.when(F.col("saleschannelcode") == "CO", F.col("transactionid")),
    )
    .withColumn(
        "USCO_MOP_transaction",
        F.when(
            (F.col("saleschannelcode") == "CO") & (F.col("moptransactionind") == "1"),
            F.col("transactionid"),
        ),
    )
    .groupBy(gb_cols_middle)
    .agg(
        F.sum("nds").alias("total_spend"),
        F.countDistinct("accountid").alias("unique_user"),
        F.round(
            ((F.sum("total_modifier_qty") / F.sum("grosslineitemqty")) * 100), 2
        ).alias("customization_percentage"),
        F.round((F.sum("nds")) / (F.countDistinct("transactionid")), 2).alias(
            "average_ticket"
        ),
        F.round(
            (F.countDistinct("transactionid")) / (F.countDistinct("accountid")), 2
        ).alias("frequency"),
        F.round(
            (F.sum("session_count")) / (F.count("itemnumber")), 0
        ).alias("session_count"),
        F.round(
            (F.sum("app_session_count")) / (F.count("itemnumber")), 0
        ).alias("app_session_count"),
        F.sum("grosssaleslocalamount").alias("total_grosssales"),
        F.sum("grosslineitemqty").alias("total_qty"),
        F.countDistinct("transactionid").alias("total_trans"),
        F.sum("total_modifier_qty").alias("total_modifier_qty"),
        F.sum("unpaid_modifier_qty").alias("unpaid_modifier_qty"),
        F.sum("paid_modifier_qty").alias("paid_modifier_qty"),
        F.countDistinct("LS_trans").alias("LS_trans"),
        F.round(
            (F.countDistinct("LS_trans")) / (F.countDistinct("transactionid")), 2
        ).alias("LS_percentage"),
        F.round(
            (F.countDistinct("SR_MOP_customer")) / (F.countDistinct("SR_customer")), 2
        ).alias("SR_MOP_Percentage"),
        F.round(
            (F.countDistinct("USCO_MOP_transaction"))
            / (F.countDistinct("USCO_transaction")),
            2,
        ).alias("USCO_MOP_Percentage"),
      (F.countDistinct('mobilescan_trans').alias('scan_trans')),
      (F.sum('mobilescan_nds').alias('scan_spend')),
      (F.sum('mobilescan_nds')) / (F.countDistinct('mobilescan_customer').alias('avg_scan_ticket')),
      (F.sum('mobilescan_qty')) / (F.countDistinct('mobilescan_trans').alias('scan_UPT')),
      (F.countDistinct('mop_trans').alias('order_ahead_trans')),
      (F.sum('mop_nds').alias('order_ahead_spend')),
      (F.sum('mop_nds')) / (F.countDistinct('mop_customer').alias('order_ahead_ticket')),
      (F.sum('mop_qty')) / (F.countDistinct('mop_trans').alias('order_ahead_UPT'))
    )
)

# COMMAND ----------

# DBTITLE 1,Joined lapsed Customer and TSD KPI
# Joining customer weekly behaviour data with calender

cus_cal_df = cus_df.join(cal_df, "calendardate", "left")

groupbydate = ["fiscalweekinyearnumber", "fiscalyearnumber"]


cus_cal_final_df = (
    cus_cal_df.withColumn(
        "lapsed_customer",
        F.when(
            (F.col("loyaltymembertenuredays") > 30)
            & ((F.col("calendardate") > F.col("mopdate")) | F.col("mopdate").isNull()),
            F.col("accountid"),
        ),
    )
    .groupBy(groupbydate)
    .agg(F.countDistinct("lapsed_customer").alias("lapsed_customer"))
)


groupbytsd = ["fiscalweekinyearnumber", "fiscalyearnumber", "storenumber", "city_name"]
groupbytsdcity = ["fiscalweekinyearnumber", "fiscalyearnumber", "city_name"]

tsd_df = line_item_lto_final_df.groupBy(groupbytsd).agg(
    F.round(
        (F.countDistinct("transactionid")) / (F.countDistinct("businessdate")), 2
    ).alias("average_tsd")
)

tsd_city_df = tsd_df.groupBy(groupbytsdcity).agg(
    F.round((F.sum("average_tsd")) / (F.countDistinct("storenumber")), 2).alias(
        "average_city_tsd"
    )
)

tsd_join_keys = ["fiscalweekinyearnumber", "fiscalyearnumber", "city_name"]
lapsed_join_keys = ["fiscalweekinyearnumber", "fiscalyearnumber"]

middle_layer_final_df = line_item_final_agg_middle_df.join(
    tsd_city_df, tsd_join_keys, "left"
).join(cus_cal_final_df, lapsed_join_keys, "left")

# COMMAND ----------

display(middle_layer_final_df)

# COMMAND ----------

# DBTITLE 1,Aggregate data and create Base table
# line_item_final_agg_base_df=(
#   line_item_lto_final_df
#   .withColumn('mop_tras',
#     F.when(F.col('moptransactionind') == 1,
#            F.col('transactionid'))
#   )
#   .withColumn('total_modifier_qty',
#     F.when(F.col('modifierind') == 1,
#            F.col('grosslineitemqty'))
#   )
#   .withColumn('unpaid_modifier_qty',
#     F.when((F.col('modifierind') == 1) &(F.col('nds')==0),
#            F.col('grosslineitemqty'))
#   )
#   .withColumn('paid_modifier_qty',
#     F.when((F.col('modifierind') == 1) &(F.col('nds')>0),
#            F.col('grosslineitemqty'))
#   )
#    .withColumn('mobilescan_trans',
#     F.when(F.col('starbucksmobileappscanind') == 1,
#            F.col('transactionid'))
#   )
#    .withColumn('mobilescan_qty',
#     F.when(F.col('starbucksmobileappscanind') == 1,
#            F.col('grosslineitemqty'))
#   )
#   .withColumn('otherscan_trans',
#     F.when(F.col('otherscanind') == 1,
#            F.col('transactionid'))
#   )
#    .withColumn('otherscan_qty',
#     F.when(F.col('otherscanind') == 1,
#            F.col('grosslineitemqty'))
#   )
#   .groupBy(gb_cols_base)
#   .agg(
#   F.sum('nds').alias('total_nds'),
#   F.countDistinct('accountid').alias('unique_user'),
#   F.sum('grosssaleslocalamount').alias('total_grosssales'),
#   F.sum('grosslineitemqty').alias('total_qty'),
#   F.countDistinct('transactionid').alias('total_trans'),
#   F.countDistinct('mop_tras').alias('order_ahead_trans'),
#   F.sum('total_modifier_qty').alias('total_modifier_qty'),
#   F.sum('unpaid_modifier_qty').alias('unpaid_modifier_qty'),
#   F.sum('paid_modifier_qty').alias('paid_modifier_qty'),
#   F.countDistinct('mobilescan_trans').alias('mobilescan_trans'),
#   F.sum('mobilescan_qty').alias('mobilescan_qty'),
#   F.countDistinct('otherscan_trans').alias('otherscan_trans'),
#   F.sum('otherscan_qty').alias('otherscan_qty')

#      )
# )



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


middle_layer_final_df.createOrReplaceTempView("my_tempmiddlepart2_table2");

spark.sql("create table cma_teams_store360.donotuse_cw_middle_table_dec_jan as select * from my_tempmiddlepart2_table2"); 

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



