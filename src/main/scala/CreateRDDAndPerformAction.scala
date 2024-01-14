import org.apache.spark.{SparkConf, SparkContext}


object CreateRDDAndPerformAction {

  def main(args : Array[String]):Unit={
    val sparksConfig=new SparkConf().setAppName("RDDCreateAndExecution").setMaster("local");
    val sc=new SparkContext(sparksConfig);

    val listOfData=List(1,2,3,4,5,4,3,6,7,8,5)
    val createDataset=sc.parallelize(listOfData); /// parallelize- It distributes the data into the different nodes

    ///1.map transformation

    //val   multiplyEachElement= createDataset.map(x=>x*2)
    //val computedData=multiplyEachElement.collect();
    //println(computedData.map(x=>println(x)));


    ///2. x.to method of element
    //val MapRDD= createDataset.map(x=>x.to(10))
    //MapRDD.collect().foreach(MapRDD=>println(MapRDD))


  //3. flatMap
    //val MapRDDFlat= createDataset.flatMap(x=>x.to(10))
    //MapRDDFlat.collect().foreach(MapRDDFlat=>println(MapRDDFlat))

    //4. Join two RDD
    val RDD1=sc.parallelize(listOfData); //1,2,3,4,5,4,3,6,7,8,5
    val RDD2= sc.parallelize(List(3,4,9,8,7,8));
    //val joinRDD1RDD2= RDD1.union(RDD2);
   // joinRDD1RDD2.collect().foreach(x=>print(x)); //// Join both RDD DATA and Returns All DATA

    //4.1 intersection
    //val intersectRDD1RDD2= RDD1.intersection(RDD2);
   // intersectRDD1RDD2.collect().foreach(x=>print(x)); //// Return common Data from the both RDD

    ///4.2 top (Actions) ----to get top 2 element from the Data in Descending order
    //val RDD1FirstTwoElem= RDD1.top(20);
    //RDD1FirstTwoElem.foreach(x=>println(x));


    //4.3 Subtract ---it returns only the element, It remove the element from RDD1 to RDD2
    //val subtractedElement= RDD1.subtract(RDD2);
    //subtractedElement.collect().foreach(x=>println(x));

    ///4.4 taken() ---It is an action to take first 3 element from RDD2
    //RDD2.take(3).foreach(x=>println(x));















  }
}
