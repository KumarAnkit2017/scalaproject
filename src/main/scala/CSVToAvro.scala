import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object CSVToAvro {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("CSVToAVRO").master("local").getOrCreate()
    ss.conf.set("spark.sql.avro.compression.codec", "deflate")
    ss.conf.set("spark.sql.avro.deflate.level","5")


    val schemas = StructType(List(
      StructField("emp_id", IntegerType, true),
      StructField("emp_name", StringType, true),
      StructField("emp_age", IntegerType, true),
      StructField("emp_state", StringType, true),
      StructField("emp_salary", StringType, true),
      StructField("emp_country", StringType, true)
    ))

    val df = ss.read.format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .schema(schemas)
      .csv("E://Scala/my_dataset.csv")

    df.write.mode("overwrite").format("avro").save("E://Scala//output//outputavro")

    ss.stop()
  }
}