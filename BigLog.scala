import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object BigLog {
  def main(args: Array[String]): Unit = {
    val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    val KAFKA_TOPIC = "test_topic"

    val spark = SparkSession.builder
      .appName("scala-kafka-streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df_streamed_raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load()

    import spark.implicits._

    val df_streamed_kv = df_streamed_raw
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val test_query = df_streamed_kv.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    Thread.sleep(20000)

    test_query.stop()
  }
}
