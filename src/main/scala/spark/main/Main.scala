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
     // .config(spark.sql.parquet.mergeSchema,true)
      .getOrCreate()



    println(spark.sessionState)
    val data = ReadJson.readJsonFile(spark,"input/json/sar2.json")
    data.printSchema()
    WriteParquet.writeParquetFile(spark,data,"output/parquet/test",SaveMode.Append)
    spark.read.option("mergeSchema", "true").parquet("output/parquet/test").printSchema()
    println(spark.read.parquet("output/parquet/test").count())

//
//    val conf = new Configuration()
//    val fileSystem = FileSystem.get(conf)
//    fileSystem.copyFromLocalFile('/input/json','/user/sameer')
//    fileSystem.rename(org.apache.hadoop.fs.Path,org.apache.hadoop.fs.Path)
//    //fileSystem.rename(new Path(existinghdfs_dirpath+oldname), new Path(newhdfs_dirPath+newname))
}}




