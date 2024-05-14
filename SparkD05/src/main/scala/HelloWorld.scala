package polytech.umontpellier.fr

object HelloWorld {
  def printSquare(x: Int) = println(x * x)
  def printCube(x: Int) = println(x * x * x)

  def applyFunctionToNumber(x: Int, func: Int => Unit): Unit = {
    func(x)
  }

  def main(args: Array[String]): Unit = {
    printSquare(5)
    printCube(5)

    applyFunctionToNumber(5, printSquare)
    applyFunctionToNumber(5, printCube)
  }
}
