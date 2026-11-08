from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dlt
_order_status=spark.conf.get("custom.orderStatus")

_order_rules={"valid order status":"o_orderstatus in ('F','O','P')",
              "valid order price":"o_totalprice >0"}
_customer_rules={"valid market segement":"c_mktsegment is not null"}


@dlt.table(
    name="orders_bronze",
    table_properties={
        "quality":"bronze"
    }
)
@dlt.expect_all(_order_rules)
def orders_bronze():
    df=spark.readStream.option("skipChangeCommits","true").table("dlt.etl.orders_raw")
    return df

@dlt.table(
    name="orders_autoloader_bronze",
    table_properties={
        "quality":"bronze"
    }
)
def orders_autoloader_bronze():
    df=(
        spark
        .readStream
        .format("cloudfiles")
        .option("cloudFiles.schemaHints","o_orderkey int, o_custkey int, o_orderstatus string, o_totalprice Decimal(18,2), o_orderdate Date, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string")
        .option("cloudFiles.schemaLocation","/Volumes/dlt/etl/landing/autoloader/schemas/")
        .option("cloudFiles.format","csv")
        .option("cloufFiles.schemaEvolutionMode","none")
        .load("/Volumes/dlt/etl/landing/files/")
    )
    
    return df

dlt.create_streaming_table("orders_union_bronze")
    
@dlt.append_flow(
    target="orders_union_bronze"

)
def order_delta_append():
    df=spark.readStream.table("LIVE.orders_bronze")
    return df

@dlt.append_flow(
    target="orders_union_bronze"

)
def order_autoloader_append():
    df=spark.readStream.table("LIVE.orders_autoloader_bronze")
    return df



# @dlt.table(
#     name="customers_bronze",
#     table_properties={
#         "quality":"bronze"
#     }
# )
# def customers_bronze():
#     df=spark.read.table("dlt.etl.customers_raw")
#     return df

@dlt.temporary_view()
@dlt.expect_all(_customer_rules)
def customers_bronze_view():
    df=spark.readStream.table("dlt.etl.customers_raw")
    return df

dlt.create_streaming_table("customers_bronze_scd1")

dlt.apply_changes(
    target="customers_bronze_scd1",
    source="customers_bronze_view",
    keys=['c_custkey'],
    stored_as_scd_type=1,
    apply_as_deletes=expr("__src_action='delete'"),
    apply_as_truncates=expr("__src_action='truncate'"),
    sequence_by="__src_insert_dt"
 )   
dlt.create_streaming_table("customers_bronze_scd2")
dlt.apply_changes(
    target="customers_bronze_scd2",
    source="customers_bronze_view",
    keys=['c_custkey'],
    stored_as_scd_type=2,
    except_column_list=["__src_action","__src_insert_dt"],
    sequence_by="__src_insert_dt"
 )   

    
@dlt.temporary_view(
    name="temp_view",
)
def temp_view():
    df_o=spark.read.table("LIVE.orders_union_bronze")
    df_c=spark.read.table("LIVE.customers_bronze_scd2").where("__END_AT is null")
    df_join=df_o.join(df_c,df_o.o_custkey==df_c.c_custkey,"left_outer")
    return df_join
 
    
    
@dlt.table(
    name="joined_silver",
    table_properties={
        "quality":"silver"
    }

)
def joined_silver():
    df=spark.read.table("LIVE.temp_view").withColumn("__insert_date",current_timestamp())
    return df
@dlt.table(
    name="orders_agg_gold",
    table_properties={
        "quality":"gold"
    }

)
def orders_agg_gold():
    df=spark.read.table("LIVE.joined_silver")
    df_final=df.groupBy(col("c_mktsegment")).agg(
        count("o_orderkey").alias("total_orders"),
        sum("o_totalprice").alias("total_revenue")
        ).withColumn("__insert_date",current_timestamp())                      
    return df_final


for i in _order_status.split(","):
    @dlt.table (
    name=f"orders_agg_gold_{i}",
    table_properties={
        "quality":"gold"})
    
    
    def orders_agg_gold():
        df=spark.read.table("LIVE.joined_silver")
        df_final=df.where(f"o_orderstatus='{i}'").groupBy(col("c_mktsegment")).agg(
        count("o_orderkey").alias("total_orders"),
        sum("o_totalprice").alias("total_revenue")
        ).withColumn("__insert_date",current_timestamp())                      
        return df_final