package spark.padhai
import org.apache.spark.sql.SparkSession

object ReadJson {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate()
//    val jobj = spark.read.json("input/json/*.json")
//    jobj.printSchema()
//    jobj.write.mode("append").parquet("output/parquet/sample.parq")
    val tab = spark.read.parquet("output/parquet/sample.parq")
    tab.printSchema()
    println(tab.count)
    tab.show()
  }


}

