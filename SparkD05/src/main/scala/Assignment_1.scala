package polytech.umontpellier.fr

object Assignement_1 {
  def square(x: Int): Int = {
    x * x
  }

  def cube(x: Int): Int = {
    x * x * x
  }

  def higherOrderFunction(x: Int, f: Int => Int): Int = {
    f(x)
  }

  def main(args: Array[String]): Unit = {
    // Print the square of the input integer
    println(square(5))

    // Print the cube of the input integer
    println(cube(3))

    // Print the square of the input integer using the higher order function
    println(higherOrderFunction(4, square))

    // Print the cube of the input integer using the higher order function
    println(higherOrderFunction(2, cube))
  }
}
