import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
 * Time-skewness-based Anomaly Detection
 *
 * @author Ajinkya Fotedar
 * @version v1
 */
object AnomalyDetection {
  def main(args: Array[String]): Unit = {
    val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    val KAFKA_TOPIC = "log_topic"

    val spark = SparkSession.builder
      .appName("anomaly-detection")
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
    Thread.sleep(10000)
    test_query.stop()

    val logSchema = StructType(Array(
      StructField("LineId", StringType, nullable = false),
      StructField("DateTime", StringType, nullable = false),
      StructField("Level", StringType, nullable = false),
      StructField("Component", StringType, nullable = false),
      StructField("Content", StringType, nullable = false)
    ))

    val logs = df_streamed_raw
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", logSchema).as("logs"))
      .select("logs.*")

    val query1 = logs
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(10000)
    query1.stop()

    var stageAndTask = logs
      .withColumn("Stage", regexp_extract($"Content", """in stage ([0-9.]+)""", 1))
      .withColumn("Task", regexp_extract($"Content", """task ([0-9.]+)""", 1))
      .withColumn("Timestamp", $"DateTime".cast(TimestampType))
      .filter($"Stage" =!= "" && $"Task" =!= "") // Keep only logs with non-empty Stage and Task IDs
      .select($"Stage", $"Task", $"Timestamp", window($"Timestamp", "10 seconds", "10 seconds").as("Window"))

    val query2 = stageAndTask
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(10000)
    query2.stop()

    stageAndTask = stageAndTask.withColumn("Timestamp", $"Timestamp".cast("double"))

    // Calculate the mean and standard deviation time for each stage and task
    var avgAndSdev = stageAndTask
      .groupBy("Stage", "Task", "Window")
      .agg(
        collect_list("Timestamp").as("Timestamps"),
        mean("Timestamp").as("Average"),
        stddev("Timestamp").as("SDev"),
      )

    val query3 = avgAndSdev
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(10000)
    query3.stop()

    // Set a threshold for outliers
    val factor = 1

    avgAndSdev = avgAndSdev
      .select($"Stage", $"Task", $"Window", explode($"Timestamps").as("Timestamp"), $"Average", $"SDev")

    val outliers = avgAndSdev
      .withColumn("Outlier", expr(s"abs(Timestamp - Average) > $factor * SDev"))
      .select($"Stage", $"Task", $"Window", $"Timestamp", $"Average", $"SDev", $"Outlier")

    val query4 = outliers
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(10000)
    query4.stop()
  }
}
