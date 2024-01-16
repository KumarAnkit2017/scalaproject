import com.schemas.MyEmpSchema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.Console.println

object ConvertCSVtoDF {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("MyCSVtoDF").master("local").getOrCreate()

    // 1st way to create schema for my csv file
    val readFile = ss.sparkContext.textFile("E://Scala/my_dataset.txt")

    val rddDF = readFile.map(x => x.split(",")).map(d =>

      if(d.length==6)
        {

          MyEmpSchema(d(0).toInt, d(1), d(2), d(3), d(4).toDouble, d(5))
        }else
        {

          MyEmpSchema(0,"","","",0,"");
        }

    )


    import ss.implicits._
    val df = rddDF.toDF()

    // Display the DataFrame
    df.show()
  }
}
