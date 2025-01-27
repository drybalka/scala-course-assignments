/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  def receive = normal

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation =>
      // println(s"$op received")
      root ! op

    case GC =>
      // println("GC received")
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive =
    case CopyFinished =>
      // println("Copy finished")
      root ! PoisonPill
      root = newRoot 
      context.become(normal)
      pendingQueue.foreach(op => root ! op)

    case op: Operation =>
      // println(s"$op saved to queue")
      pendingQueue = pendingQueue :+ op


object BinaryTreeNode:
  trait Position
  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op @ Insert(requester, id, new_elem) =>
      if new_elem == elem then
        removed = false
        requester ! OperationFinished(id)
      else
        val pos = if new_elem > elem then Right else Left
        if subtrees.isDefinedAt(pos) then
          subtrees(pos) ! op
        else
          subtrees += pos -> context.actorOf(BinaryTreeNode.props(new_elem, false))
          requester ! OperationFinished(id)

    case op @ Remove(requester, id, new_elem) =>
      if new_elem == elem then
        removed = true
        requester ! OperationFinished(id)
      else
        val pos = if new_elem > elem then Right else Left
        if subtrees.isDefinedAt(pos) then
          subtrees(pos) ! op
        else
          requester ! OperationFinished(id)

    case op @ Contains(requester, id, new_elem) =>
      if new_elem == elem && !removed then
        requester ! ContainsResult(id, true)
      else
        val pos = if new_elem > elem then Right else Left
        if subtrees.isDefinedAt(pos) then
          subtrees(pos) ! op
        else
          requester ! ContainsResult(id, false)

    case op @ CopyTo(newTree) =>
      // println(s"CopyTo received to ${context.props}")
      val children = subtrees.values.toSet
      if removed && children.isEmpty then context.parent ! CopyFinished
      else
        if !removed then newTree ! Insert(context.self, 0, elem)
        children.foreach(_ ! op)
        context.become(copying(children, removed))
    }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = 
    case OperationFinished(0) =>
      // println(s"Value $elem confirmed copied")
      context.become(copying(expected, true))
      if expected.isEmpty then context.parent ! CopyFinished

    case op @ CopyFinished =>
      // println(s"Child ${sender()} of $elem confirmed copied")
      val new_expected = expected - sender()
      if new_expected.isEmpty && insertConfirmed then context.parent ! CopyFinished
      else context.become(copying(new_expected, insertConfirmed))



