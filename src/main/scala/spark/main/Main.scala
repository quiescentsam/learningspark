package spark.main
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.internal.Logging
import spark.likhai.WriteParquet
import spark.padhai.ReadJson
import org.apache.hadoop.fs.{FileSystem, Path}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Test App")  // Application Name
      .enableHiveSupport()  // to enable HiveContext
      .getOrCreate()


//
//    println(spark.sessionState)
//    val data = ReadJson.readJsonFile(spark,"input/json/*.json")
//    data.printSchema()
//    WriteParquet.writeParquetFile(spark,data,"output/parquet/test",SaveMode.Append)


    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    fileSystem.rename("part-00000-2d68a51e-efc3-449d-8f98-e5e85bc9e513-c000.snappy.parquet","part-00000-2d68a51e-efc3-449d-8f98-e5e85bc9e513-c000.snappy.parquet2")
    //fileSystem.rename(new Path(existinghdfs_dirpath+oldname), new Path(newhdfs_dirPath+newname))
}}
