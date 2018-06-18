package spark.likhai

import org.apache.spark.sql._

object WriteParquet {


  def writeParquetFile(spark: SparkSession, df: DataFrame,filename: String,writemode: SaveMode = SaveMode.ErrorIfExists): Unit = {
//    val mode = if (writemode == null) "append" else writemode
    df.write.mode(writemode).parquet(filename)
  }

//  val tab = spark.read.parquet("output/parquet/sample.parq")
//  tab.printSchema()
//  println(tab.count)
//  tab.show()

}
