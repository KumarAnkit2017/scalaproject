import org.apache.spark.sql.{SparkSession, DataFrame}

object ReadCSVAndConvertToAvro {

  def main(args: Array[String]): Unit = {

    val csvPath = "E:/Scala/forbes_2640_billionaires.csv"

    val spark = SparkSession.builder()
      .appName("CSVToAvroConversion")
      .master("local")
      .getOrCreate()

    try {
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath)
        df.show();

    } catch {
      case e: Exception =>
        println(s"Error during CSV to Avro conversion: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}