object Main extends App{
    val numb1:Int=20
    val numb2:Int=20
    println("Sum of "+numb1+" and "+numb2+"="+(numb1+numb2))

    val ifExpression=
        if (numb2>20)
            true
        else
            false
    println(ifExpression);


    ////define function
    def myFunction(x:Int,y:String):String={
      return  x+" "+y
    }

    println(myFunction(3,"Ankit"))


    ///define function to find the factorial
    def fact(n:Int):Int={
        if(n<=1)
            1
        else
            return n*fact(n-1)
    }

    println(fact(5))



}