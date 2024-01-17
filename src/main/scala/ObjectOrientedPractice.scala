object ObjectOrientedPractice extends App{

  class Persion(val name:String)
  {
    val names:String="Kumar"
    def getPersonName():String={
      return names
    }
  }
  val stud_object=new Persion("Ankit")
  println(stud_object.name);

  class Student extends Persion("Kumar Ankit")
  {

  }

  val objectOfStudent:Persion=new Student
  println(objectOfStudent.getPersonName());




}
