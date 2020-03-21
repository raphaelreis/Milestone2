import org.apache.spark.{SparkConf, SparkContext}

object App2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App2").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)


    val file = "hdfs:///cs449/data3_"+args(0)+".csv"
    val data = sc.textFile(file,200)

    /* Using map parititions allows for applying the function only once per partition
    and not for each single element. */
    val rows = data.mapPartitions(iter => {
      val list = iter.toList
      list.map(c => (c(0), c(1).toInt, c(2).toInt)).iterator
    })

    // val t1 = rows.map(p => p._2 -> p._3/10)
    val t1 = rows.mapPartitions(iter => {
      val list = iter.toList
      list.map(p => p._2 -> p._3/10).iterator
    })
    val t2 = t1.reduceByKey((a, b) => a + b)
  
    // val t2 = t1.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum).collect
    t2.foreach(println)
  }
}