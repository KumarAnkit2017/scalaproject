import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object ConvertJsonDataToDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JSONtoDF").master("local").getOrCreate()

   /* val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("grandtotal", DoubleType, true)
    ))*/

    val readJson = spark.read.json("E://Scala/dataset.json")


    //readJson.show()

    // Stop the SparkSession
    spark.stop()
  }
}
