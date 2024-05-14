// Function to return the square of an integer
def square(n: Int): Int = {
  n * n
}

// Function to return the cube of an integer
def cube(n: Int): Int = {
  n * n * n
}

// Higher order function that takes an integer and a function, and prints the result of applying the function to the integer
def applyFunction(n: Int, func: Int => Int): Unit = {
  println(func(n))
}

// Example usage
val num = 5
applyFunction(num, square) // prints 25
applyFunction(num, cube)   // prints 125
