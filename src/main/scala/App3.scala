import org.apache.spark.{SparkConf, SparkContext}

object App3 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App3").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    /* Data1 */
    val lines = sc.textFile("hdfs:///cs449/data1_" + args(0) +".csv")
    // val lines = sc.textFile("hdfs:///cs449/data1_small.csv")
    
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt
      (k, v)
    })

    /* Same problem than previousely */
    // val sum = rows.groupBy(_._1).map { case (k, l) => k -> l.map(_._2).sum }.collect
    val sum = rows.reduceByKey(_+_)
    val extendedSum = sum.flatMap(x => (2 to 100).map(i => x._1 -> x._2 % i))

    /* Cannot use foldLeft when data is partitionned (here lines.partitions.length = 2) */
    // val res = extendedSum.foldLeft(0){case (acc, cur) => acc + cur._1 * cur._2}
    val res = extendedSum.aggregate(0)((acc, curr) => acc + curr._1 * curr._2,_+_)

    /* Result send to the driver cluster */
    println("Result = " + res)
  }
}