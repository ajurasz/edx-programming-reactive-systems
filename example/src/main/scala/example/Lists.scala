package example

import scala.annotation.tailrec


object Lists {

  /**
   * This method computes the sum of all elements in the list xs. There are
   * multiple techniques that can be used for implementing this method, and
   * you will learn during the class.
   *
   * For this example assignment you can use the following methods in class
   * `List`:
   *
   *  - `xs.isEmpty: Boolean` returns `true` if the list `xs` is empty
   *  - `xs.head: Int` returns the head element of the list `xs`. If the list
   *    is empty an exception is thrown
   *  - `xs.tail: List[Int]` returns the tail of the list `xs`, i.e. the the
   *    list `xs` without its `head` element
   *
   *  ''Hint:'' instead of writing a `for` or `while` loop, think of a recursive
   *  solution.
   *
   * @param xs A list of natural numbers
   * @return The sum of all elements in `xs`
   */
//    def sum(xs: List[Int]): Int = {
//      @tailrec
//      def innerSum(xs: List[Int], acc: Int): Int = xs match {
//        case head :: tail => innerSum(tail, head + acc)
//        case Nil => acc
//      }
//      innerSum(xs, 0)
//    }

    def sum(xs: List[Int]): Int = xs.foldLeft(0)(_ + _)
  
  /**
   * This method returns the largest element in a list of integers. If the
   * list `xs` is empty it throws a `java.util.NoSuchElementException`.
   *
   * You can use the same methods of the class `List` as mentioned above.
   *
   * ''Hint:'' Again, think of a recursive solution instead of using looping
   * constructs. You might need to define an auxiliary method.
   *
   * @param xs A list of natural numbers
   * @return The largest element in `xs`
   * @throws java.util.NoSuchElementException if `xs` is an empty list
   */
//    def max(xs: List[Int]): Int = {
//      if (xs.isEmpty) {
//        throw new NoSuchElementException
//      }
//      @tailrec
//      def innerMax(xs: List[Int], max: Int): Int = xs match {
//        case head :: tail => innerMax(tail, if (head > max) head else max)
//        case Nil => max
//      }
//      val x :: xss = xs
//      innerMax(xss, x)
//    }

    def max(xs: List[Int]): Int = xs match {
      case Nil => throw new NoSuchElementException
      case _ => xs.reduce((a, b) => if (a > b) a else b)
    }
  }
