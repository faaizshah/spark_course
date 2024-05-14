object Square {
  def main(args: Array[String]): Unit = {
    val a = chainFunctions(2, square, cube)
    System.out.println(a)
  }

  def square(x: Int): Int = {
    x * x
  }

  def cube(x: Int): Int = {
    x * x * x
  }

  def chainFunctions(x: Int, f: Int => Int, g: Int => Int): Int = {
    g(f(x))
  }
}
