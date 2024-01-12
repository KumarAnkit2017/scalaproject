import com.services.{DataFrameServices, DataFrameServicesImpl}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadCSVAndConvertToAvro {

  def main(args: Array[String]): Unit = {

    val csvPath = "E:/Scala/forbes_2640_billionaires.csv"

    val spark = SparkSession.builder()
      .appName("CSVToAvroConversion")
      .master("local")
      .getOrCreate()

    try {
      val df: DataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath)

      val dataFrameServices: DataFrameServices = new DataFrameServicesImpl()

      dataFrameServices.totalWealthByBillionarsOfWorldUS(df);

      dataFrameServices.totalWealthByBillionarsOfWorldINR(df);

      dataFrameServices.averAgeOfBillionarsHavingNetworthGt18(df);

      dataFrameServices.industryHavingMorebillionars(df);

      


    } catch {
      case e: Exception =>
        println(s"Error during CSV to Avro conversion: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}