package spark.readdf

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object ReadFile {


  def readJsonFile(spark: SparkSession, filename: String): DataFrame = {
    spark.read.json(filename)
  }

  def readCsvFile(spark: SparkSession, filename: String): DataFrame = {
    spark.read.option("inferschema",true).option("delimiter","|").option("header","true").csv(filename)
  }

}

