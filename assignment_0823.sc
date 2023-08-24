val path="spark-data/input/2008.csv"
val rdd = spark.sparkContext.textFile(path)
val rddCsv = rdd.map(f=>{
    f.split(",")
  })
val filteredRdd = rddCsv.filter(_(15) != "NA").filter(_(15).toInt > 0)

// 5.3.1 Spark RDD 퀴즈 1
val aggregatedData = filteredRdd.map({ case a => 
  (a(16), (a(15).toDouble, 1))
}).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

val avgDepartureDelay = aggregatedData.map({ case (key, (sum, count)) => 
  (key, sum/count)
})

avgDepartureDelay.collect()


// 5.3.1 Spark RDD 퀴즈 2
import scala.math.sqrt

val aggregatedData = filteredRdd.map({ case a => 
  (a(16), (a(15).toDouble, a(15).toDouble * a(15).toDouble, 1))
}).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

val stdDepartureDelay = aggregatedData.map({ case (key, (sum, ssum, count)) => 
  (key, sqrt(ssum/count - (sum/count)*(sum/count)))
})

stdDepartureDelay.collect()


// 5.3.3 Spark RDD 퀴즈 3
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

val planeDataPath = "spark-data/metadata/plane-data.csv"
val planeRdd = spark.sparkContext.textFile(planeDataPath)
val planeData = planeRdd.map(_.split(",")).filter(a => a.length == 9 && a(8) != "" && a(8) != "None").map(a => (a(0), a(8)))

val delayAndAge = filteredRdd.map(a => (a(10), a(15).toDouble)).join(planeData).map({case (_, (delay, age)) => (delay, age.toDouble)})

val r1RDD = sc.parallelize(delayAndAge.map(_._1).collect(), 5)
val r2RDD = sc.parallelize(delayAndAge.map(_._2).collect(), 5)


val corr = Statistics.corr(r1RDD, r2RDD, "pearson")

corr
