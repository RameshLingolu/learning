import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType, DoubleType

#Initialize SparkSession
spark=SparkSession.builder.appName("CustomerOrderPypeline").getOrCreate()

#Setting Up Logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger("CustomerOrderPypeline")

#user defined exception
class InvalidInputError(Exception):
  def __init__(self,message="Invalid Amount"):
      self.message=message
      super().__inti__(self.message)

#Read from CSV

try:
    customer_df=spark.read.csv("/content/sample_data/customers.csv",header=True,inferSchema=True)
    order_df = spark.read.csv("/content/sample_data/orders.csv",header=True,inferSchema=True)

    #udf to define order values
    def order_category(amount):
        try:
            if amount<0:
                raise InvalidInputError(f"Invalid order amount: {amount}")
            elif amount>200:
              return "High Value"
            elif amount>100:
              return "Medium Value"
            else:
              return "Low Value"
        except InvalidInputError as e:
            logger.warning(f"Order amount is not valid {str(e)}")
            return "Invalid"

    #registering udf
    order_category_udf=udf(order_category,StringType())
    order_df=order_df.withColumn("order_category",order_category_udf(col("amount")))
    order_df.show()

    joined_df=customer_df.join(order_df,customer_df.customer_id==order_df.customer_id,"inner")

    joined_df.show()

    #Sample transformation
    transformed_df=joined_df.select(customer_df.customer_id, customer_df.name, order_df.amount, order_df.order_category)\
    .withColumn("amount_in_inr",col("amount")* 85)

    transformed_df.show()
    logger.info("Pipeline Completed Successfully")

    # Database Connection SF
    sf_options = {
        "sfURL": "sample.snowflakecomputing.com",
        "sfUser": "test123",
        "sfPassword": "*******",
        "sfDatabase": "DEMO_DB",
        "sfSchema": "NOT_USED",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "SYSADMIN"
    }

     # writing final df to Snowflake table
    transformed_df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "customer_orders_inr") \
        .mode("overwrite") \
        .save()
    logger.info("Data written to Snowflake.")


    #Exception Block
except Exception as e:
    print(f"Error in Pipeline execution: {str(e)}")
    logger.error(f"Pipeline Execution Failed: {str(e)}")
finally:
    spark.stop()
    logger.info("Spark Session ended")
