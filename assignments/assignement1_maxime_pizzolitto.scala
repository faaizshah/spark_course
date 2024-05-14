object Main {

    // Define a high-order function that takes an input and a function to apply
    def applyOperation(value: Double, operation: Double => Double): Double = operation(value)

    // Define the square function
    def square(x: Double): Double = x * x

    // Define the cube function
    def cube(x: Double): Double = x * x * x



    def main(args: Array[String]): Unit = {
        // Usage examples:
        val resultSquare = applyOperation(5, square) // 25.0
        val resultCube = applyOperation(7.5, cube) // 421.875

        println(resultSquare)
        println(resultCube)
    }
}
