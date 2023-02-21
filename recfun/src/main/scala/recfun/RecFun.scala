package recfun

object RecFun extends RecFunInterface:

  def main(args: Array[String]): Unit =
    println("Pascal's Triangle")
    for row <- 0 to 10 do
      for col <- 0 to row do print(s"${pascal(col, row)} ")
      println()

  /** Exercise 1
    */
  def pascal(c: Int, r: Int): Int =
    if c == 0 || c == r then 1 else pascal(c - 1, r - 1) + pascal(c, r - 1)

  /** Exercise 2
    */
  def balance(chars: List[Char]): Boolean =
    def balanceStep(chars: List[Char], state: Int): Boolean =
      if state < 0 then false
      else
        chars match
          case Nil         => true
          case '(' :: tail => balanceStep(tail, state + 1)
          case ')' :: tail => balanceStep(tail, state - 1)
          case _ :: tail   => balanceStep(tail, state)

    balanceStep(chars, 0)

  /** Exercise 3
    */
  def countChange(money: Int, coins: List[Int]): Int =
    money match
      case 0          => 1
      case x if x < 0 => 0
      case _ =>
        if coins.isEmpty then 0
        else countChange(money - coins.head, coins) + countChange(money, coins.tail)
