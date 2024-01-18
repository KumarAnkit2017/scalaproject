import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object CSVToAvro {
        def main(arg:Array[String]): Unit = {
         val ss= SparkSession.builder().appName("CSVToAVRO").master("local").getOrCreate();

          val schemas = StructType(List(
            StructField("emp_id", IntegerType, true),
            StructField("emp_name", StringType, true),
            StructField("emp_age", IntegerType, true),
            StructField("emp_state", StringType, true),
            StructField("emp_salary", DoubleType, true),
            StructField("emp_country", StringType, true)
          ))

         val df= ss.read.format("CSV").option("header",true).schema(schemas).csv("E://Scala/my_dataset.csv")

          df.write.format("avro").mode(SaveMode.Overwrite).save("E:\\Scala\\output\\output_avro_directory")

          ss.stop();

  }
}
