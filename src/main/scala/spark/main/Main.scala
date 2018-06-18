package spark.main
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.internal.Logging
import spark.likhai.WriteParquet
import spark.padhai.ReadJson


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Test App")  // Application Name
      .enableHiveSupport()  // to enable HiveContext
      .getOrCreate()



    println(spark.sessionState)
    val data = ReadJson.readJsonFile(spark,"input/json/*.json")
    data.printSchema()
    WriteParquet.writeParquetFile(spark,data,"output/parquet/test",SaveMode.Append)

}}
