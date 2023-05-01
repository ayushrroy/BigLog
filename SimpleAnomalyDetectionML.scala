import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
 * ML-based Anomaly Detection
 *
 * @author Ajinkya Fotedar
 * @version v1
 */
object SimpleAnomalyDetectionML {
  def main(args: Array[String]): Unit = {
    val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    val KAFKA_TOPIC = "log_ml_topic"

    val spark = SparkSession.builder
      .appName("simple-anomaly-detection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Load the saved ML model
    val model = DecisionTreeClassificationModel.load("lrm_model.model")

    // Define UDF for applying the model
    import org.apache.spark.ml.linalg.Vectors
    val predictOutlier = udf((duration: Double) => model.predict(Vectors.dense(duration)))

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
      StructField("DateTime", StringType, nullable = false),
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

    val runningAndFinishedLogs = logs
      .filter($"Content".like("%Running task%") || $"Content".like("%Finished task%"))

    val stageAndTask = runningAndFinishedLogs
      .withColumn("Stage", regexp_extract($"Content", """in stage (\d+\.\d+)""", 1))
      .withColumn("Task", regexp_extract($"Content", """task (\d+\.\d+)""", 1))
      .withColumn("Status", when($"Content".like("%Running task%"), "Running").otherwise("Finished"))
      .withColumn("Timestamp", $"DateTime".cast(TimestampType))
      .filter($"Stage" =!= "" && $"Task" =!= "") // Keep only logs with non-empty Stage and Task IDs
      .select($"Stage", $"Task", $"Status", $"Timestamp")

    val stageAndTaskQuery = stageAndTask
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(10000)
    stageAndTaskQuery.stop()

    val durationAndOutlier = stageAndTask
      .groupBy(window($"Timestamp", "15 seconds", "15 seconds").as("Window"), $"Stage", $"Task")
      .agg(
        collect_list("Timestamp").as("Timestamps")
      )
      .withColumn("StartTime", $"Timestamps".getItem(0))
      .withColumn("EndTime", $"Timestamps".getItem(1))
      .withColumn("Duration", $"EndTime".cast("double") - $"StartTime".cast("double"))
      .withColumn("Outlier", $"Duration" > 1)
      .withColumn("PredictedOutlier", predictOutlier($"Duration").cast("boolean")) // Apply the model using the UDF

    val query = durationAndOutlier
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(20000)
    query.stop()

    // Update the outlierRows DataFrame to filter based on PredictedOutlier instead of Outlier
    val outlierRows = durationAndOutlier.filter($"PredictedOutlier" === true)

    val outlierRowsQuery = outlierRows
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    Thread.sleep(20000)
    outlierRowsQuery.stop()
  }
}