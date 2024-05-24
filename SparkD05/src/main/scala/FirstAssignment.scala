package polytech.umontpellier.fr

object FirstAssignment {
  def square(x: Int): Int = {
    println(s"[square] Square of $x is ${x * x}")
    x * x
  }

  def cube(x: Int): Int = {
    println(s"[cube] Cube of $x is ${x * x * x}")
    x * x * x
  }

  def applyMath(x: Int, f: Int => Int): Int = f(x)

  def main(args: Array[String]): Unit = {
      val x = 5
      val squareResult = applyMath(x, square)
      val cubeResult = applyMath(x, cube)
      println(s"[main] Square of $x is $squareResult")
      println(s"[main] Cube of $x is $cubeResult")
  }
}
