package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 2
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceHelper(chars: Array[Char], leftUnbalanced: Int): Boolean =
    if (leftUnbalanced < 0) false
    else if (chars.isEmpty) leftUnbalanced == 0
    else {
      val newLeftUnbalanced = if (chars.head == '(') leftUnbalanced + 1 else if (chars.head == ')') leftUnbalanced - 1 else leftUnbalanced
      balanceHelper(chars.tail, newLeftUnbalanced)
    }

    balanceHelper(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, leftUnbalanced: Int, rightUnbalanced: Int): (Int, Int) = {
      if (idx < until) {
        chars(idx) match {
          case '(' => traverse(idx + 1, until, leftUnbalanced + 1, rightUnbalanced)
          case ')' => if (leftUnbalanced > 0) traverse(idx + 1, until, leftUnbalanced - 1, rightUnbalanced)
                      else traverse(idx + 1, until, leftUnbalanced, rightUnbalanced + 1)
          case _ => traverse(idx + 1, until, leftUnbalanced, rightUnbalanced)
        }
      } else (leftUnbalanced, rightUnbalanced)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))
        val matching = if (left._1 <= right._2) left._1 else right._2
        (left._1 + right._1 - matching, left._2 + right._2 - matching)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!
}