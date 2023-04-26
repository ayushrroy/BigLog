import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
 * Simple time-based Anomaly Detection
 *
 * @author Ajinkya Fotedar
 * @version v1
 */
object SimpleAnomalyDetection {
  def main(args: Array[String]): Unit = {
    val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    val KAFKA_TOPIC = "log_topic"

    val spark = SparkSession.builder
      .appName("simple-anomaly-detection")
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

    val test_query = df_streamed_kv
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    Thread.sleep(10000)
    test_query.stop()

    val logSchema = StructType(Array(
      StructField("LineId", StringType, nullable = false),
      StructField("Date_Time", StringType, nullable = false),
      StructField("Level", StringType, nullable = false),
      StructField("Component", StringType, nullable = false),
      StructField("Content", StringType, nullable = false)
    ))

    val logs = df_streamed_raw
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", logSchema).as("logs"))
      .select("logs.*")

    val logQuery = logs
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    Thread.sleep(10000)
    logQuery.stop()

    val stageAndTask = logs
      .withColumn("Stage", regexp_extract($"Content", """in stage ([0-9.]+)""", 1))
      .withColumn("Task", regexp_extract($"Content", """task ([0-9.]+)""", 1))
      .withColumn("Timestamp", $"Date_Time".cast(TimestampType))
      .filter($"Stage" =!= "" && $"Task" =!= "") // Keep only logs with non-empty Stage and Task IDs
      .select($"Stage", $"Task", $"Timestamp", window($"Timestamp", "10 seconds", "10 seconds").as("Window"))

    val stageAndTaskInfoQuery = stageAndTask
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    Thread.sleep(10000)
    stageAndTaskInfoQuery.stop()

    val durationAndOutlier = stageAndTask
      .groupBy("Stage", "Task", "Window")
      .agg(
        collect_list("Timestamp").as("Timestamps")
      )
      .withColumn("StartTime", $"Timestamps".getItem(0))
      .withColumn("EndTime", $"Timestamps".getItem(1))
      .withColumn("Duration", $"EndTime".cast("long") - $"StartTime".cast("double"))
      .withColumn("Outlier", $"Duration" > 1)
      .drop("Timestamps")

    //    val outlierRows = meanAndStdDevByStageAndTask.filter($"Outlier" === true)

    val query = durationAndOutlier
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    Thread.sleep(10000)
    query.stop()
  }
}
