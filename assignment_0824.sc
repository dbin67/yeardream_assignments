// 7.1 Spark SQL 퀴즈 1

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._

val rdd = spark.sparkContext.textFile("spark-data/input/2008.csv")

val rddCsv = rdd.map(f=>{f.split(",")}).map(a => 
    Row(
        if (a(15) == "NA") 0.0 else a(15).toDouble,
        a(16),
        if (a(24) == "NA") 0.0 else a(24).toDouble,
        if (a(25) == "NA") 0.0 else a(25).toDouble,
        if (a(26) == "NA") 0.0 else a(26).toDouble,
        if (a(27) == "NA") 0.0 else a(27).toDouble,
        if (a(28) == "NA") 0.0 else a(28).toDouble
    ))

val schema = StructType(
    Array(
        StructField("depdelay", DoubleType, true),
        StructField("origin", StringType, true),
        StructField("carrierdelay", DoubleType, true),
        StructField("weatherdelay", DoubleType, true),
        StructField("nasdelay", DoubleType, true),
        StructField("securitydelay", DoubleType, true),
        StructField("lateaircraftdelay", DoubleType, true)
    ))

val df = spark.createDataFrame(rddCsv, schema)

val aggregatedDf = df.groupBy(col("origin")).agg(
        corr(col("depdelay"), col("carrierdelay")),
        corr(col("depdelay"), col("weatherdelay")),
        corr(col("depdelay"), col("nasdelay")),
        corr(col("depdelay"), col("securitydelay")),
        corr(col("depdelay"), col("lateaircraftdelay"))
    ).withColumn(
        "maxCorr", greatest(
        col("corr(depdelay, carrierdelay)"),
        col("corr(depdelay, weatherdelay)"),
        col("corr(depdelay, nasdelay)"),
        col("corr(depdelay, securitydelay)"),
        col("corr(depdelay, lateaircraftdelay)"))
    ).withColumn(
        "maxDelayType",
        when(col("maxCorr") === col("corr(depdelay, carrierdelay)"), "carrierdelay")
        .when(col("maxCorr") === col("corr(depdelay, weatherdelay)"), "weatherdelay")
        .when(col("maxCorr") === col("corr(depdelay, nasdelay)"), "nasdelay")
        .when(col("maxCorr") === col("corr(depdelay, securitydelay)"), "securitydelay")
        .otherwise("lateaircraftdelay")
    ).orderBy(desc("maxCorr"))

aggregatedDf.select("origin", "maxDelayType", "maxCorr").show()

// 7.1 Spark SQL 퀴즈 2

import org.apache.spark.sql.functions._

val airlineRdd = spark.sparkContext.textFile("spark-data/input/2008.csv")
val airlineCsv = airlineRdd.map(f=>{f.split(",")}
    ).map(a => (a(10),
    a(0) + "-" +
    (if (a(1).length == 1) "0" + a(1) else a(1)) + "-" + 
    (if (a(2).length == 1) "0" + a(2) else a(2))
    ))

val planeRdd = spark.sparkContext.textFile("spark-data/metadata/plane-data.csv")
val planeCsv = planeRdd.map(f=>{f.split(",")}
    ).filter(a => a.length == 9
    ).map(a => (a(0), (a(2), a(3), a(4))))

val columns = Seq("tailnum", "flight_date", "manufacturer", "issue_date", "model")
val df = airlineCsv.join(planeCsv
    ).map({case (tailnum, (flight_date, (manufacturer, issue_date, model))) => 
    (tailnum, flight_date, manufacturer, issue_date, model)}
    ).toDF(columns: _*)

val aggregatedDf = df.withColumn("flight_date", to_date(col("flight_date"))
    ).withColumn("issue_date", to_date(col("issue_date"), "MM/dd/yyyy")
    ).groupBy("tailnum", "manufacturer", "model", "issue_date"
    ).agg(max("flight_date").alias("last_flight_date")
    ).withColumn("days_in_use", datediff(col("last_flight_date"), col("issue_date"))
    ).orderBy(desc("days_in_use"))

aggregatedDf.show()
