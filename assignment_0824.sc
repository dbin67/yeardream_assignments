// 7.1 Spark SQL 퀴즈 1

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._

val rdd = spark.sparkContext.textFile("spark-data/input/2008.csv")
val rddCsv = rdd.map(f=>{f.split(",")})
val rowRdd = rddCsv.map(a => Row(
  if (a(15) == "NA") 0.0 else a(15).toDouble,
  a(16),
  if (a(24) == "NA") 0.0 else a(24).toDouble,
  if (a(25) == "NA") 0.0 else a(25).toDouble,
  if (a(26) == "NA") 0.0 else a(26).toDouble,
  if (a(27) == "NA") 0.0 else a(27).toDouble,
  if (a(28) == "NA") 0.0 else a(28).toDouble
))

val schema = StructType(Array(
  StructField("depdelay", DoubleType, true),
  StructField("origin", StringType, true),
  StructField("carrierdelay", DoubleType, true),
  StructField("weatherdelay", DoubleType, true),
  StructField("nasdelay", DoubleType, true),
  StructField("securitydelay", DoubleType, true),
  StructField("lateaircraftdelay", DoubleType, true)
))
val df = spark.createDataFrame(rowRdd, schema)

val groupByOrigin = df.groupBy(col("origin"))

val aggregatedDf = groupByOrigin.agg(
  corr(col("depdelay"), col("carrierdelay")),
  corr(col("depdelay"), col("weatherdelay")),
  corr(col("depdelay"), col("nasdelay")),
  corr(col("depdelay"), col("securitydelay")),
  corr(col("depdelay"), col("lateaircraftdelay"))
)

val maxCorrelationDf = aggregatedDf.withColumn(
  "maxCorr", greatest(
    col("corr(depdelay, carrierdelay)"),
    col("corr(depdelay, weatherdelay)"),
    col("corr(depdelay, nasdelay)"),
    col("corr(depdelay, securitydelay)"),
    col("corr(depdelay, lateaircraftdelay)")
  )
).withColumn(
  "maxDelayType",
  when(col("maxCorr") === col("corr(depdelay, carrierdelay)"), "carrierdelay")
    .when(col("maxCorr") === col("corr(depdelay, weatherdelay)"), "weatherdelay")
    .when(col("maxCorr") === col("corr(depdelay, nasdelay)"), "nasdelay")
    .when(col("maxCorr") === col("corr(depdelay, securitydelay)"), "securitydelay")
    .otherwise("lateaircraftdelay")
)

maxCorrelationDf.select("origin", "maxCorr", "maxDelayType").show()
