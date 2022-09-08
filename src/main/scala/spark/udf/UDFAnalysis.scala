package spark.udf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, split, lit}
import org.apache.spark.sql.{Column, SparkSession}
import spark.readdf.ReadFile


object UDFAnalysis extends Logging {

  def splitIp(ipaddress: Column): Column = {

    split(ipaddress, "\\.")

  }


  def main(args: Array[String]): Unit = {

    val isLocalMode = if (args.length == 0) true else false
    val sparkMaster = if (isLocalMode) "local[*]" else "yarn"

    val spark = SparkSession
      .builder
      .master(sparkMaster)
      .appName("UDF Test Application")
      .getOrCreate()

    val input = if (isLocalMode ) "input/udf/data.json" else args(0)
    val jsonData = ReadFile.readJsonFile(spark, input)
    jsonData.printSchema()
    jsonData.show(20, false)

    val result = jsonData.select("first_name", "ip_address")
      .withColumn("ip_split", splitIp(col("ip_address")))
    result.show(20,false)
    result.count()

  }
}







