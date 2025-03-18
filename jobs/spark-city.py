from pyspark.sql import SparkSession
from config import config
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

def main():
    spark = (
        SparkSession.builder.appName("SmartCityStreaming")
        .config(
            'spark.jars.packages', 
            'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,'
            'org.apache.hadoop:hadoop-aws:3.3.1,'
            'com.amazonaws:aws-java-sdk:1.11.469'
        )
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.access.key', config.get('AWS_ACCESS_KEY'))
        .config('spark.hadoop.fs.s3a.secret.key', config.get('AWS_SECRET_ACCESS_KEY'))
        .config(
            'spark.hadoop.fs.s3a.aws.credentials.provider',
            'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('WARN')



    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),  
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuel_type", StringType(), True)
    ])


    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),  # Use TimestampType() if timestamp is datetime
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True)
    ])


    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),  
        StructField("snapshot", StringType(), True)
    ])



    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True),
        StructField("timestamp", TimestampType(), True),  

        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("AQI", DoubleType(), True)
    ])


    emergencySchema = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("incidentId", StringType(), True),
    StructField("type", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("location", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True)
    
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark(eventTime='timestamp', delayThreshold='2 minutes')
            )



    def streamWriter(input, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    
    vehicleDF = read_kafka_topic(topic='vehicle_data', schema=vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic(topic='gps_data', schema=gpsSchema).alias('gps')
    trafficDF = read_kafka_topic(topic='traffic_data', schema=trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic(topic='weather_data', schema=weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic(topic='emergency_data', schema=emergencySchema).alias('emergency')

    query1 = streamWriter(vehicleDF, 
             checkpointFolder='s3a://spark-streaming-data-smart-city-de-project/checkpoints/vehicle_data',
             output='s3a://spark-streaming-data-smart-city-de-project/data/vehicle_data')

    query2 = streamWriter(gpsDF, 
                checkpointFolder='s3a://spark-streaming-data-smart-city-de-project/checkpoints/gps_data',
                output='s3a://spark-streaming-data-smart-city-de-project/data/gps_data')

    query3 = streamWriter(trafficDF, 
                checkpointFolder='s3a://spark-streaming-data-smart-city-de-project/checkpoints/traffic_data',
                output='s3a://spark-streaming-data-smart-city-de-project/data/traffic_data')

    query4 = streamWriter(weatherDF, 
                checkpointFolder='s3a://spark-streaming-data-smart-city-de-project/checkpoints/weather_data',
                output='s3a://spark-streaming-data-smart-city-de-project/data/weather_data')

    query5 = streamWriter(emergencyDF, 
                checkpointFolder='s3a://spark-streaming-data-smart-city-de-project/checkpoints/emergency_data',
                output='s3a://spark-streaming-data-smart-city-de-project/data/emergency_data')

    query5.awaitTermination()


if __name__ == "__main__":
    main()

    

