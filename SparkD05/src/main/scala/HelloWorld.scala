package polytech.umontpellier.fr

object HelloWorld {

  def printSquare(x: Int) = println(x*x)
  def printCube(x:Int) = println(x*x*x)
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    printSquare(3)
    printCube(5)
  }
}