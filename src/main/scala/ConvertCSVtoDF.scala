
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object ConvertCSVtoDF {

  def main(arg: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("CSVToRead").master("local").getOrCreate();

    val schemas = StructType(List(
      StructField("emp_id", IntegerType, true),
      StructField("emp_name", StringType, true),
      StructField("emp_age", IntegerType, true),
      StructField("emp_state", StringType, true),
      StructField("emp_salary", DoubleType, true),
      StructField("emp_country", StringType, true)
    ))


    val loadRdd = ss.read.format("csv").schema(schemas).csv("E://Scala/my_dataset.csv")
    loadRdd.show();

    loadRdd.select("*").where("emp_id=1").show();
  }
}


