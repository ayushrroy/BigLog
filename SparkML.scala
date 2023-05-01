import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.feature.VectorAssembler

object SparkML {
  def main(args : Array[String]): Unit ={

    Logger.getRootLogger.setLevel(Level.INFO)
    val spark = SparkSession.builder
      .appName("SparkML")
      .master("local[*]")
      .getOrCreate
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/Vinay/Documents/anomalies_dataset.csv")

    val df2 = df
      .withColumn("Outlier_Encoded", col("Outlier").cast("Int"))


    val df3=df2.select("Duration","Outlier_Encoded")

    //Fill in null values with null duration as 0 (note that means that the outlier will be considered as false here)
    val df_cleaned=df3.na.fill(0)

   val assembler = new VectorAssembler()
      .setInputCols(Array("Duration"))
      .setOutputCol("features")

    val input = assembler.transform(df_cleaned).select("Outlier_Encoded","features")

    val Array(train, test) = input.randomSplit(Array(0.8, 0.2))


    val dt = new DecisionTreeClassifier()
      .setLabelCol("Outlier_Encoded")
      .setFeaturesCol("features")

    val model = dt.fit(train)
    val predictions = model.transform(test)
    predictions.show(100)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Outlier_Encoded")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

//    Saves the trained model for the later to local disk (HADOOP_HOME) and hadoop.home.dir needs to be set up
//    model.write.overwrite.save("lrm_model.model")


  }


}
