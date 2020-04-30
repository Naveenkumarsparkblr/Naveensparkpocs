def sum(x:Int, y:Int) = x+y
sum(4,8)
// recursive functions
//a function calling itself is --recursive functions
// a factorial  6*5*4*3*2*1

//fact(4)
//4*3*2*1
//-----comment
//def fact(x:Int) = x match {
//  case x if (x <= 1) => 1
//  case _ => x* fact(x-1)
//}
//----
def power(b:Int,p:Int):Int = p match{
  case p if(p<1) => 1
  case _ => b * power(b,p-1)
}

power(2,4)
//---Nested functions
//a function call in another function
def maximum(x:Int,y:Int,z:Int):Int = {
def max(a:Int,b:Int):Int=if (a>b) a else b
max(x,max(y,z))
}

maximun(2,6,9)

//----anonymus function

//very important function-->
//only in scala function
//--HIHER ORDER FUNCTION
//higher order function
// a function called as parameter in another function

//higher order function
// a function called as parameter in another function

def sqr(x:Int):Int = x * x
def cub(x:Int):Int = x * x * x
fact(5)

// higher order function hof
def hof(f:Int=>, x:Int):Int ={
  f(x)}

hof(sqr,4)  //--res Int = 16
hof(cub,4)  //--res Int = 64