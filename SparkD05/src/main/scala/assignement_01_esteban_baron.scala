package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object assignement_01_esteban_baron {
  def square(x: Int): Int = x * x

  def cube(x: Int): Int = x * x * x

  def applySquareOperation(value: Int, operation: Int => Int): Int = operation(value)

  def applyCubeOperation(value: Int, operation: Int => Int): Int = operation(value)

  def main(args: Array[String]): Unit = {
    val resultSquare = applySquareOperation(2, square)
    val resultCube = applyCubeOperation(2, cube)

    println("Résultat du carré : " + resultSquare)
    println("Résultat du cube : " + resultCube)
  }
}

