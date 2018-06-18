package spark.padhai

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object ReadJson {


  def readJsonFile(spark: SparkSession, filename: String): DataFrame = {
    spark.read.json(filename)
  }


}

