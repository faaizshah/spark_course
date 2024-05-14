
// Hello
val hello = "Hello, world"


//Functional Programming Overview

// assign a function to variable
val myVal = (x : Int) => x + 1
myVal(7)

// passing a function as parameter
(0 to 5) map (myVal)

// take even number, multiply each value by 2 and sum them up
(1 to 7) filter (_ % 2 == 0 ) map (_ * 2) reduce (_ + _)


// Scala
val name = "Scala"
val hasUpperCase = name.exists(_.isUpper)

//**** Mutable and Immutable Variables ****//
// mutable variables
var counter:Int = 10
var d = 0.0
var f = 0.3f

// immutable variables
val message = "Hello Scala"
println(message)

val ? = scala.math.Pi
println(?)

//**** String Interpolation ****//
// string interpolation
val course = "Spark With Scala"
println(s"I am taking course $course.")

// support arbitrary expressions
println(s"2 + 2 = ${2 + 2}")

val year = 2024
println(s"Next year is ${year + 1}")

//**** Looping Constructs ****//
var i = 0
do {
  println(s"Hello, world #$i")
  i = i + 1
} while (i <= 5)
println()


for (j<- 1 to 5) {
  println(s"Hello, world #$j")
}
println()

// what will be printed?
for (i <- 1 to 3) {
  println(i)
}

// what will be printed?
for (i <- 1 to 3) {
  var i = 2
  println(i)
}

//**** Functions ****//
// defining functions
def hello(name:String) : String = { "Hello " + name }
def hello1() = { "Hi there!" }
def hello2() = "Hi there!"
def hello3 = "Hi there!"

def max(a:Int, b:Int) : Int = if (a > b) a else b
max(4,6)
max(8,3)

//**** Function Literals ****//
// function literals
(x: Int, y: Int) => x + y
val sum = (x: Int, y: Int) => x + y
val prod = (x: Int, y: Int) => x * y
def doIt(msg:String, x:Int, y:Int, f: (Int, Int) => Int) = {
  println(msg + f(x,y))
}
doIt("sum: ", 1, 80, sum)
doIt("prod: ", 2, 33, prod)

//**** Tuples ****//
// tuple
val pair1 = ("Scala", 1)
println(pair1._1)
println(pair1._2)

val pair2 = ("Scala", 1, 2017)
println(pair2._3)


//**** Classes ****//

// constructor with two private instance variables
class Movie(name:String, year:Int)

// With two getter methods
class Movie1(val name:String, val year:Int)
val m1 = new Movie1("Star Wars", 1977)
println(m1.name + " " + m1.year)

// With two getter and setter methods
class Movie2(var name:String, var year:Int, var rating:String)
val m2 = new Movie2("Alien", 1979, "R")
m2.name = "Alien: Director's Edition"
println(m2.name + " Released: " + m2.year + " Rated: " + m2.rating)

//**** Case Classes ****//
// case class
case class Movie(name:String, year:Int)
val m = Movie("Avatar", 2009)
m.toString
println(m.name + " " + m.year)

//**** Pattern Matching with Case Class ****//

// pattern matching with case class
abstract class Shape
case class Rectangle(h:Int, w:Int) extends Shape
case class Circle(r:Int) extends Shape

def area(s:Shape) = s match {
  case Rectangle(h,w) => h * w
  case Circle(r) => r * r * 3.14
}

println("Area of Rectangle: " + area(Rectangle(4,5)))
println("Area of Circle: " + area(Circle(5)))

//**** Arrays ****//

Array(1,2,3,4,5).foreach(println)

println()
Array("Java","Scala","Python","R","Spark").foreach(println)

val myArray = Array(1,2,3,4,5);
myArray.foreach(a => print(a + " "))
println
myArray.foreach(println)

//**** Lists ****//

val l = List(1,2,3,4);
l.foreach(println)
println()

println("head of List: " +l.head)
println("tail of List: " +l.tail)
println("last of List: " +l.last)
println("init of List: " +l.init)
println()

val table: List[List[Int]] = List (
  List(1,0,0),
  List(0,1,0),
  List(0,0,1)
)

// Working with Lists

val list = List(2,3,4);

// cons operator – prepend a new element to the beginning
val m = 1::list

// appending
val n = list :+ 5

// to find out whether a list is empty or not
println("empty list? " + m.isEmpty)

// take the first n elements
list.take(2)

// drop the first n elements
list.drop(2)

// High Order List Methods

val n = List(1,2,3,4)
val s = List("LNKD", "GOOG", "AAPL")
val p = List(265.69, 511.78, 108.49)
var product = 1;

n.foreach(product *= _) //==> 24

n.filter(_ % 2 != 0)

n.partition(_ % 2 != 0)

n.find(_ % 2 != 0)

n.find(_ < 0)

p.takeWhile(_ > 200.00)
p.dropWhile(_ > 200.00)
p.takeWhile(_ < 500.00)
p.dropWhile(_ < 500.00)

// high order list methods
val n = List(1,2,3,4)
val s = List("LNKD", "GOOG", "AAPL")

n.map(_ + 1)

s.flatMap(_.toList)

n.reduce((a,b) => { a + b} )
n.contains(3)

val sumUsingReduce = n.reduce(_ + _)
println(s"The sum of elements using reduce is: $sumUsingReduce")



//Using reduce to find the maximum element
val maxElement = n.reduce(_ max _)
println(s"The maximum element in the list is: $maxElement")

//Using reduce to find the product of element
val productOfElements = n.reduce(_ * _)
println(s"The product of elements in the list is: $productOfElements")

// Using built-in sum method
val sumOfElements = n.sum
println(s"The sum of elements in the list is: $sumOfElements")