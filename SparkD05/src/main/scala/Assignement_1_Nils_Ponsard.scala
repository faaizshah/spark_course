object assignement1 {
  def square(x: Int): Int = {
    x * x
  }

  def cube(x: Int): Int = x * x * x

  private def print_result(pre: String, f: Int => Int, v: Int): Unit = {
    println(pre + f(v))
  }

  def main(args: Array[String]): Unit = {

    print_result("Square of 5 is ", square, 5)
    print_result("Cube of 6 is ", cube, 6)

  }
}