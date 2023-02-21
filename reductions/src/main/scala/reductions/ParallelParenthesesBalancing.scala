package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer (Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 10000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing
    extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean =
    def balanceStep(chars: List[Char], state: Int): Boolean =
      if state < 0 then false
      else
        chars match
          case Nil         => state == 0
          case '(' :: tail => balanceStep(tail, state + 1)
          case ')' :: tail => balanceStep(tail, state - 1)
          case _ :: tail   => balanceStep(tail, state)

    balanceStep(chars.toList, 0)

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean =

    @tailrec
    def traverse(idx: Int, until: Int, min: Int, end: Int): (Int, Int) = {
      if (idx == until) then (min, end)
      else
        chars(idx) match
          case '(' => traverse(idx + 1, until, min, end + 1)
          case ')' => traverse(idx + 1, until, min min (end - 1), end - 1)
          case _   => traverse(idx + 1, until, min, end)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from < threshold) then traverse(from, until, 0, 0)
      else
        val mid = (until + from) / 2
        val ((min1, end1), (min2, end2)) =
          parallel(reduce(from, mid), reduce(mid, until))
        (min1 min (end1 + min2), end1 + end2)
    }

    reduce(0, chars.length) == (0, 0)

  // For those who want more:
  // Prove that your reduction operator is associative!
