package funsets

object Main extends App:
  import FunSets.*
  println(contains(union(singletonSet(1), singletonSet(2)), 1))
