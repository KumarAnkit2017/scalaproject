import com.schemas.MyDataSetSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum}

object ReadCSVInDataset {
  def main(arg: Array[String]): Unit = {
    val csvPath = "E:/Scala/forbes_2640_billionaires.csv"

    val spark= SparkSession.builder().appName("ReadCSVInDATASET").master("local").getOrCreate();

    val loadCSV=spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").csv(csvPath);

    import spark.implicits._
    val convertCsvIntoDataSet= loadCSV.as[MyDataSetSchema];
    convertCsvIntoDataSet.show();

  //1. Total wealth accumulated by all billionaires (USD):

    convertCsvIntoDataSet.select(sum(col("net_worth")).as("total_net_worth(USD)")).show();

 //2. What is the total wealth accumulated by all the billionaires of the world (in INR, assuming 1 USD = 82.14 INR)

    convertCsvIntoDataSet.select(sum(col("net_worth")*82.14).as("total_net_worth(INR)")).show();

 //3. What is the average age of a billionaire with networth > 18 billion

    convertCsvIntoDataSet.filter("net_worth>18").agg(avg(col("age4")).as("Average Age Of Billi>18")).show()

 //4. In which Industry do we have the most number of billionaires?

    convertCsvIntoDataSet.groupBy("industry").count().orderBy(col("count").desc).select(col("industry").
      as("List of Industries"),col("count").as("No of Industries")).show();
  }
}
