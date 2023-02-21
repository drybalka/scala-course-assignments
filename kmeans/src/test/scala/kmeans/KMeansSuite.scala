package kmeans

import java.util.concurrent.*
import scala.collection.{mutable, Map, Seq}
import scala.collection.parallel.{ParMap, ParSeq}
import scala.collection.parallel.CollectionConverters.*
import scala.math.*
import scala.collection.parallel.immutable.ParVector
import scala.collection.parallel.mutable.ParArray

class KMeansSuite extends munit.FunSuite:
  object KM extends KMeans
  import KM.*

  def checkParClassify(
      points: ParSeq[Point],
      means: ParSeq[Point],
      expected: ParMap[Point, ParSeq[Point]]
  ): Unit =
    assertEquals(
      classify(points, means),
      expected,
      s"classify($points, $means) should equal to $expected"
    )

  test("'classify' should work for empty 'points' and empty 'means'") {
    val points: ParSeq[Point] = IndexedSeq().par
    val means: ParSeq[Point] = IndexedSeq().par
    val expected = ParMap[Point, ParSeq[Point]]()
    checkParClassify(points, means, expected)
  }

  // test("'classify' should work for empty 'points' and non-empty 'means'") {
  //   val points: ParSeq[Point] = IndexedSeq().par
  //   val means: ParSeq[Point] = IndexedSeq(Point(1, 1, 1)).par
  //   val expected = ParMap[Point, ParSeq[Point]](Point(1, 1, 1) -> ParSeq.empty)
  //   checkParClassify(points, means, expected)
  // }

  test("'kmeans' should work for non-empty 'points' and non-empty 'means'") {
    val points: ParSeq[Point] = IndexedSeq(
      Point(0, 0, 1),
      Point(0, 0, -1),
      Point(0, 1, 0),
      Point(0, 10, 0)
    ).par
    val means: ParSeq[Point] = IndexedSeq(Point(0, -1, 0), Point(0, 2, 0)).par
    val eta = 12.25
    assertEquals(kMeans(points, means, eta), ParVector(Point(0.0, 0.0, 0.0), Point(0.0, 5.5, 0.0)))
  }

  import scala.concurrent.duration.*
  override val munitTimeout = 10.seconds
