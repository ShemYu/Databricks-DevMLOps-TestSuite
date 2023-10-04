# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.feature_store import feature_table
from databricks.feature_store import FeatureStoreClient


# COMMAND ----------

# MAGIC %md
# MAGIC #### read data

# COMMAND ----------

def read_people():
    ## read people test data from S3
    return spark.read.format('parquet').option('header','true').load('s3://john-workshop-bucket/john-raw-zone/people-test').select('distinct_id','age','gender','yyyymm')


def read_points():
    ## read points test data from S3
    return spark.read.format('parquet').option('header','true').load('s3://john-workshop-bucket/john-raw-zone/points')\
            .select('total_points','storename','productname',"distinct_id",'yyyymmdd')

def read_mr():
    ## read points test data from S3
    return spark.read.format('parquet').option('header','true').load('s3://john-workshop-bucket/john-raw-zone/mr').select('distinct_id','yyyymmdd','region')

def read_main():
    ## read points test data from S3
    return spark.read.format('csv').option('header','true').load('s3://john-workshop-bucket/john-raw-zone/main').select('distinct_id','yyyymmdd','main_event')


# COMMAND ----------

# MAGIC %md
# MAGIC #### convert date to yyyymm

# COMMAND ----------

## date convert to year and month
def date2yearmont(x):
    # from pyspark.sql.functions import year,month, to_date
    # import pyspark.sql.functions as F

    ## create new columns 
    x=x.withColumn("year", year(to_date(x.yyyymmdd, 'yyyyMMdd')).alias('year'))
    x=x.withColumn("month", month(to_date(x.yyyymmdd, 'yyyyMMdd')).alias('month'))
    x = x.withColumn(
        "yyyymm",
        date_format(expr("make_date(year, month, 1)"), "yyyyMM"))
    return x 

# COMMAND ----------

## date convert to year and month
def points_cleaned(x):
    ## create new col 
    point_temp = date2yearmont(x)
    ## change data type 
    point_temp=point_temp.withColumn("total_points",col("total_points").cast("int"))


    ## aggregate  with group 
    point_temp = point_temp \
    .groupBy(["distinct_id",'yyyymm']) \
    .agg(sum("total_points").alias("sum_total_points"),
        count(when(col('storename').isNotNull(),col('storename'))).alias('count_consumption'),
        countDistinct(when(col('storename').isNotNull(),col('storename'))).alias('count_store_distinct'),
        countDistinct(when(col('productname').isNotNull(),col('productname'))).alias('count_product_distinct'))

    return point_temp.filter(point_temp.distinct_id.isNotNull() & point_temp.yyyymm.isNotNull()).cache()



def main_cleaned(x):
    
    x=x.withColumn("yyyymmdd", date_format(to_date('yyyymmdd', 'yyyy-MM-dd'), 'yyyyMMdd'))
    ## create new columns 
    x = date2yearmont(x)

    ## get main_event == 'notification'
    x=x.filter((x.main_event =='notification') )

    ## aggregate by group and sort df 
    x=x \
    .groupBy(["distinct_id",'yyyymm']) \
    .agg(count(when(col('main_event').isNotNull(),col('main_event'))).alias('count_notify'))
    return  x.filter(x.distinct_id.isNotNull() & x.yyyymm.isNotNull()).cache()





###############################################
############# mr data processing ############
###############################################


def mr_cleaned(x):
    ## create new columns 
    mr = date2yearmont(x)
    ## aggregate by group
    mr=mr \
        .groupBy(["distinct_id",'yyyymm']) \
        .agg(count(when(col('region').isNotNull(),col('region'))).alias('count_distinct_region'))
    return  mr.filter(mr.distinct_id.isNotNull() & mr.yyyymm.isNotNull()).cache()



# COMMAND ----------

def merge_all():
    point = points_cleaned(read_points())
    main = main_cleaned(read_main())
    mr = mr_cleaned(read_mr())
    people =read_people()



    ## left join by keys
    all_temp = point.join(main,['distinct_id','yyyymm'], 'left').cache()
    all_temp = all_temp.join(mr,['distinct_id','yyyymm'], 'left').cache()


    ## left join two df 
    cond = [all_temp.distinct_id==people.distinct_id ,all_temp.yyyymm==people.yyyymm]
    all_temp = people.join(all_temp,cond, 'left').drop(all_temp.distinct_id,all_temp.yyyymm).cache()

    ## fill na with 0 
    all_temp = all_temp.na.fill(0)
    all_temp = all_temp.dropDuplicates(['distinct_id','yyyymm'])


    from pyspark.sql import Window
    import pyspark.sql.functions as fn

    lag_window = Window.partitionBy('distinct_id').orderBy('yyyymm')
    all_temp = all_temp.withColumn(
    'lag_sum_total', fn.lag('sum_total_points', -1).over(lag_window)
    )

    return all_temp.filter(all_temp.lag_sum_total.isNotNull())

# COMMAND ----------


def load_data():

    fs = FeatureStoreClient()

    df = merge_all()
    mr_feature_table = fs.create_table(
        name='databricks_test_mr_df',
        primary_keys=['distinct_id','yyyymm'],
        df = df,
        schema=df.schema,
        description='MR dataframe',
    )
    return mr_feature_table

# COMMAND ----------

df = merge_all()

# COMMAND ----------

df.show(n=5)

# COMMAND ----------

table_name = "databricks_test_mr_df"
fs = FeatureStoreClient()
df = merge_all()
mr_feature_table = fs.create_table(
    name=table_name,
    primary_keys=['distinct_id','yyyymm'],
    df = df,
    schema=df.schema,
    description='MR dataframe',
)

# COMMAND ----------

mr_feature_table = fs.write_table(
    name=table_name,
    df = df,
)

# COMMAND ----------

ft = fs.get_table(table_name)
ft
