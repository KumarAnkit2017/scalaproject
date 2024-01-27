import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object MethodPractices {

  def main(args:Array[String]): Unit = {

   val ss= SparkSession.builder().appName("MethodPractices").master("local").getOrCreate();



    val listData=Array(1,2,3,4,5,6,7,8,9)

    val rdd1=ss.sparkContext.parallelize(listData)

    val Rdd2=rdd1.map(x=>x+2).reduce((x,y)=>x+y)

    println(Rdd2)


  }

}
