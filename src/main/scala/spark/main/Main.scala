package spark.main

import java.io.InputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.internal.Logging
import spark.likhai.WriteParquet
import spark.padhai.ReadJson
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Test App") // Application Name
      .enableHiveSupport() // to enable HiveContext
      // .config(spark.sql.parquet.mergeSchema,true)
      .getOrCreate()


    println(spark.sessionState)
    val data = ReadJson.readJsonFile(spark, "input/json/sar2.json")
    data.printSchema()
    /*    WriteParquet.writeParquetFile(spark,data,"output/parquet/test",SaveMode.Append)
        spark.read.option("mergeSchema", "true").parquet("output/parquet/test").printSchema()
        println(spark.read.parquet("output/parquet/test").count())*/

    val conf = new Configuration()
    //conf.addResource("/usr/local/hadoop/etc/hadoop/core-site.xml")
    //conf.addResource("/usr/local/hadoop/etc/hadoop/hdfs-site.xml")
    //val fs = FileSystem.get(URI.create(uri), conf)

    logInfo("configuration resource added " +conf)




    import org.apache.hadoop.fs.FileSystem
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf)
    val sourcePath = new Path("/Users/ssiddiqui/Desktop/SAMEER2.0/WORK/CODE/learningspark/input/json/")
    val destPath = new Path("hdfs://localhost:9000/user/test22/")

    logWarning("source path " +sourcePath.toString + " destpath : "+destPath)
    logError("source path " +sourcePath.toString + " destpath : "+destPath)



     hdfs.mkdirs(destPath)
       hdfs.listFiles(destPath, false)
       hdfs.copyFromLocalFile(sourcePath, destPath)
      /* //fileSystem.listStatus(new Path("file:///user/json/test"))

       //fileSystem.copyFromLocalFile("",'/user/sameer')
       hdfs.rename(new Path("/user/test/json/sar2.json"),new Path("/user/test/json/sameer.json"))    //fileSystem.rename(new Path(existinghdfs_dirpath+oldname), new Path(newhdfs_dirPath+newname))
   */  }
}




