import com.schemas.MyDataSetSchema
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}



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

   val networthInUSD= convertCsvIntoDataSet.map(ds=>ds.net_worth).reduce((a,b)=>a+b);


   println(s"Total wealth accumulated by all billionaires (USD) :$networthInUSD");

    //2. What is the total wealth accumulated by all the billionaires of the world (in INR, assuming 1 USD = 82.14 INR)

    val networthInINR= convertCsvIntoDataSet.map(ds=>ds.net_worth * 82.14).reduce((a,b)=>a+b);

    println(s"total wealth accumulated by all the billionaires of the world in INR :$networthInINR");

    //3. What is the average age of a billionaire with networth > 18 billion

    val networthG18= convertCsvIntoDataSet.filter("net_worth > 18")

    val avgAge = networthG18.map(ag => ag.age4.toInt).reduce((a, b) => a + b)/networthG18.count()

    println(s"The average age of a billionaire with net worth > 18 billion: $avgAge")

    //4. In which Industry do we have the most number of billionaires?

   // val industryBillionaireCount = convertCsvIntoDataSet.map(b => (b.industry,1)).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)

   val industryBillionaireCount = convertCsvIntoDataSet.map(x=>(x.industry,1)).groupByKey((x)=>x.productElement(0).toString).count().orderBy(desc("count(1)")).first().productElement(0)




   //industryBillionaireCount.printSchema()
   //industryBillionaireCount.show()

    println(s" In which Industry do we have the most number of billionaires : $industryBillionaireCount")
  }
}
