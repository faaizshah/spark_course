package polytech.umontpellier.fr
import scala.math.pow

object Assignment1 extends App {
  def square: Double => Double = pow(_,2)
  
  def cube: Double => Double = pow(_,3)

  println(s"square(2): ${square(2)}")
  println(s"cube(2): ${cube(2)}")

  def pipeline: (Double => Double, Double => Double) => Double = (one, two) => 
    two(one(2))

  println(s"pipeline(square, cube): ${pipeline(square, cube)}")
}
