import org.apache.spark.{SparkConf, SparkContext}

object firstdemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("firstDemo")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(5, 10, 30))
    println(rdd.reduce(_ + _))
    sc.stop()
  }
}

