package quickcheck

import org.scalacheck.*
import Arbitrary.*
import Gen.*
import Prop.forAll

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap:
  lazy val genHeap: Gen[H] = Gen.frequency(
    (1, const(empty)),
    (
      4,
      for el <- arbitrary[A]; tail <- genHeap
      yield meld(insert(el, empty), tail)
    )
  )
  given Arbitrary[H] = Arbitrary(genHeap)

  def extract(heap: H): List[A] =
    if isEmpty(heap) then Nil else findMin(heap) :: extract(deleteMin(heap))

  property("gen1") = forAll { (h: H) =>
    val m = if isEmpty(h) then 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("min of 2") = forAll { (a: A, b: A) =>
    findMin(insert(a, insert(b, empty))) == (if a < b then a else b)
  }

  property("inserting preserves the same elements") = forAll {
    (a: A, b: A, c: A) =>
      val heap = insert(c, insert(a, insert(b, empty)))
      extract(heap).sorted == List(a, b, c).sorted
  }

  property("empty") = forAll { (a: A) =>
    deleteMin(insert(a, empty)) == empty
  }

  property("sorted") = forAll { (h: H) =>
    def reinsert(heap: H): H =
      if isEmpty(heap) then empty
      else insert(findMin(heap), reinsert(deleteMin(heap)))
    def fromList(list: List[A]): H =
      list match
        case Nil        => empty
        case el :: tail => insert(el, fromList(tail))
    reinsert(h) == fromList(extract(h).sorted)
  }

  property("meld with empty") = forAll { (h: H) =>
    meld(h, empty) == h
  }

  property("empty is empty") = forAll { () =>
    isEmpty(empty)
  }

  property("inserting min leaves it min") = forAll { (h: H) =>
    if isEmpty(h) then true
    else
      val min = findMin(h)
      findMin(insert(min, deleteMin(h))) == min
  }

  property("min of meld") = forAll { (h1: H, h2: H) =>
    if !isEmpty(h1) && !isEmpty(h2) then
      val min1 = findMin(h1)
      val min2 = findMin(h2)
      findMin(meld(h1, h2)) == (if min1 < min2 then min1 else min2)
    else true
  }
