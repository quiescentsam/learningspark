package spark.config

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration

object GetHdfs {
  def getHdfsFile(spark: SparkSession): Unit = {
    // Get HDFS fs
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val conf = new Configuration()
    conf.addResource("/usr/local/hadoop/etc/hadoop/core-site.xml")
    conf.addResource("/usr/local/hadoop/etc/hadoop/hdfs-site.xml")
    //val fs = FileSystem.get(URI.create(uri), conf)

    //    val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  }
}
