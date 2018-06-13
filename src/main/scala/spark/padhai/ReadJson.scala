package spark.padhai
import org.apache.spark.sql.SparkSession

object ReadJson {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate()
    val jobj = spark.read.json("input/json/*")
    val name = jobj.select(explode(jobj("restaurant")))
  }


}

