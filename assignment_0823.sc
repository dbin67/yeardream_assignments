// 5.3.1 Spark RDD 퀴즈 1
val filteredRdd = rddCsv.filter(_(15) != "NA").filter(_(15).toInt > 0)

val aggregatedData = filteredRdd.map({ case a => 
  (a(16), (a(15).toDouble, 1))
}).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

val avgDepartureDelay = aggregatedData.map({ case (key, (sum, count)) => 
  (key, sum/count)
})

avgDepartureDelay.collect()

// 5.3.1 Spark RDD 퀴즈 2
import scala.math.sqrt

val filteredRdd = rddCsv.filter(_(15) != "NA").filter(_(15).toInt > 0)

val aggregatedData = filteredRdd.map({ case a => 
  (a(16), (a(15).toDouble, a(15).toDouble * a(15).toDouble, 1))
}).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

val stdDepartureDelay = aggregatedData.map({ case (key, (sum, ssum, count)) => 
  (key, sqrt(ssum/count - (sum/count)*(sum/count)))
})

stdDepartureDelay.collect()
