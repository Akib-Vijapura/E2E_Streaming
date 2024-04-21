import logging
import json
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
        return None

def connect_to_kafka(spark):
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users_created") \
            .load()
        return df
    except Exception as e:
        logging.error(f"Couldn't connect to Kafka due to exception {e}")
        return None

def create_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname="your_database_name",
            user="your_username",
            password="your_password",
            host="postgres",  # Docker service name
            port="5432"  # Default PostgreSQL port
        )
        return conn
    except Exception as e:
        logging.error(f"Could not create PostgreSQL connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    selection_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return selection_df

def insert_data_postgres(conn, **kwargs):
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (kwargs['id'], kwargs['first_name'], kwargs['last_name'], kwargs['gender'], kwargs['address'],
              kwargs['post_code'], kwargs['email'], kwargs['username'], kwargs['dob'], kwargs['registered_date'],
              kwargs['phone'], kwargs['picture']))
        conn.commit()
        logging.info(f"Data inserted for {kwargs['first_name']} {kwargs['last_name']} in PostgreSQL!")
    except Exception as e:
        logging.error(f'Could not insert data into PostgreSQL due to {e}')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = create_spark_session()
    if spark:
        kafka_df = connect_to_kafka(spark)
        if kafka_df:
            selection_df = create_selection_df_from_kafka(kafka_df)
            conn = create_postgres_connection()
            if conn:
                while True:
                    try:
                        selection_df.writeStream \
                            .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(lambda row: insert_data_postgres(conn, **row.asDict()))) \
                            .start() \
                            .awaitTermination()
                    except Exception as e:
                        logging.error(f"Error processing batch: {e}")
                        continue
