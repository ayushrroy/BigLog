import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col

object TopicTestDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("scala-kafka-streaming")
      //.master("spark://spark-master:7077")
      .master("local[*]")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
      .config("spark.executor.memory", "512m")
      .getOrCreate()

    val dfStreamedRaw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9093")
      .option("subscribe", "topic_test")
      .option("startingOffsets", "earliest")
      .load()

    // Convert byte stream to string
    val dfStreamedKv = dfStreamedRaw
      .withColumn("key", col("key").cast(StringType))
      .withColumn("value", col("value").cast(StringType))

    val testQuery = dfStreamedKv
      .writeStream
      .format("memory") // Output to memory
      .outputMode("update") // Only write updated rows to the sink
      .queryName("test_query_table") // Name of the in-memory table
      .start()

    // Keep running until termination
    testQuery.awaitTermination()

  }

}
