# Databricks notebook source
# MAGIC %sql
# MAGIC describe cma_teams_store360.donotuse_cw_base_table_2months

# COMMAND ----------

# MAGIC %sql
# MAGIC select  fiscalyearnumber,fiscalweekinyearnumber,sum(total_nds) as total_nds
# MAGIC from cma_teams_store360.donotuse_cw_base_table_2months where ownership_type='CO'
# MAGIC group by fiscalweekinyearnumber, fiscalyearnumber
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select  fiscalyearnumber,fiscalweekinyearnumber ,loyaltyprogramname,loyaltyprogramlevelname,round(sum(total_nds)) as total_nds
# MAGIC from cma_teams_store360.donotuse_cw_base_table_2months where ownership_type='CO' and loyaltyprogramname='MSR_USA'
# MAGIC group by fiscalyearnumber,fiscalweekinyearnumber ,loyaltyprogramname,loyaltyprogramlevelname

# COMMAND ----------

# MAGIC %sql
# MAGIC describe cma_teams_store360.donotuse_cw_middle_layer_table_2months

# COMMAND ----------

# MAGIC %sql
# MAGIC select  fiscalyearnumber,fiscalweekinyearnumber ,sum(total_spend) as total_nds
# MAGIC from cma_teams_store360.donotuse_cw_middle_layer_table_2months where  ownership_type='CO' and 
# MAGIC group by fiscalweekinyearnumber, fiscalyearnumber
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select  fiscalyearnumber,fiscalweekinyearnumber ,round(sum(total_spend)) as total_nds
# MAGIC
# MAGIC from cma_teams_store360.donotuse_cw_middle_layer_table_2months where ownership_type='CO' and loyaltyprogramname='MSR_USA'
# MAGIC
# MAGIC group by fiscalyearnumber,fiscalweekinyearnumber
# MAGIC -- ,loyaltyprogramname,loyaltyprogramlevelname
