import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

case class Person(id:Int,name:String,age:Int)
object SparkSql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSql").master("local").getOrCreate()

    val rdd=spark.sparkContext.parallelize(Array(Row(1,"Ankit",21),Row(2,"Shubhi",22),Row(3,"Aishu",21)))
   // rdd.collect().foreach(println);

   //val rddPerson =List(Person(1,"Ankit",22),Person(3,"Shubhi",19),Person(3,"Akriti",15),Person(5,"Vikash",30)) //--It is easily converted into DF

    //val rddData =List(1,2,3,4,5,6,7,8,9) //--this is not coverting in DF

    //design own Schema
    val rddPersonSchema=StructType(
      List(
        StructField("Person_Id",IntegerType,true),
        StructField("Person_Name",StringType,true),
        StructField("Person_Age",IntegerType,true)
      ))

    // Convert RDD to DataFrame
   val df= spark.createDataFrame(rdd,rddPersonSchema);

    df.show();

     //df.select("id").groupBy("id").count().show();

    //2. df.selectExpr(("SUM(id) as myID") ).show();

    //3. df.filter("id > 3").show();

    //4. df.select("id","name").show();

    //5. df.select("id","name").where("id > 3").show();

    //df.select("*").show();

    //df.select("*").where("id < 5").orderBy(desc("id")).show();

    //df.select("*").agg(avg("id")).show();



    // Show the DataFrame


    // Stop the SparkSession

  }
}
