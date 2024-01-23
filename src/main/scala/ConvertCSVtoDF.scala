
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object ConvertCSVtoDF {

  def main(arg: Array[String]): Unit = {


    val ss = SparkSession.builder().appName("CSVToRead").master("local").getOrCreate();
    //ss.conf.set("spark.sql.sfuffle.partition","5")


    val schemas = StructType(List(
      StructField("emp_id", IntegerType, true),
      StructField("emp_name", StringType, true),
      StructField("emp_age", IntegerType, true),
      StructField("emp_state", StringType, true),
      StructField("emp_salary", DoubleType, true),
      StructField("emp_country", StringType, true)
    ))


    val loadRdd = ss.read.format("csv").schema(schemas).csv("E://Scala/my_dataset.csv")
    loadRdd.createOrReplaceTempView("MyView"); /// We can easily create view of my datafram and used like same as a sql view
    //ss.sql("select * from MyView").show();

  loadRdd.groupBy("emp_state").count().withColumnRenamed("count","mycount").sort( desc("mycount")).show();

    //loadRdd.explain();
   // loadRdd.show();
   // loadRdd.sort("emp_age").explain()
    loadRdd.select("*").where("emp_id=1").show();
  }
}


