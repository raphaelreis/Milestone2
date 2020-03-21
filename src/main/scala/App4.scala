import org.apache.spark.{SparkConf, SparkContext}

object App4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App4").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1").
      set("spark.driver.memory", "10g")

    val sc = SparkContext.getOrCreate(conf)


    val file = "hdfs:///cs449/data3_"+args(0)+".csv"
    val data = sc.textFile(file, 200)
    val rows = data.mapPartitions(iter => {
      val list = iter.toList
      list.map(elem => {
        val c = elem.split(",")
        (c(0), c(1).toInt, c(2).toInt)
      }).iterator
    })

    val t1 = rows.map(p => p._2 -> p._3/10)
    val t2 = t1.reduceByKey(_+_)
    t2.foreach(println)
  }
}