object Main extends App {
  def square: Double => Double = scala.math.pow(_,2)
  
  def cube: Double => Double = Math.pow(_,3)

  println(s"square(2): ${square(2)}")
  println(s"cube(2): ${cube(2)}")

  def pipeline: (Double => Double, Double => Double) => Double = (one, two) => 
    two(one(2))

  println(s"pipeline(square, cube): ${pipeline(square, cube)}")
}
