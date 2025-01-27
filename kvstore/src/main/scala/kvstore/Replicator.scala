package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import scala.concurrent.duration.*
import scala.collection.immutable.Queue
import akka.actor.Cancellable

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor:
  import Replicator.*
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq() =
    val ret = _seqCounter
    _seqCounter += 1
    ret

  
  def receive: Receive = normal

  def normal: Receive = 
    case op @ Replicate(key, valueOption, id) =>
      val seq = nextSeq()
      acks += seq -> (sender(), op)
      scheduleCall(seq)

  def waiting(currentSeq: Long, scheduledCall: Cancellable): Receive =
    case SnapshotAck(key, seq) if seq == currentSeq =>
      scheduledCall.cancel()

      val (sender, Replicate(key, _, id)) = acks(seq)
      sender ! Replicated(key, id)

      if acks.isDefinedAt(currentSeq + 1) then
        scheduleCall(currentSeq + 1)
      else
        context.become(normal)
        
    case op @ Replicate(key, valueOption, id) =>
      val seq = nextSeq()
      acks += seq -> (sender(), op)


  def scheduleCall(seq: Long): Unit =
    val (_, Replicate(key, valueOption, _)) = acks(seq)
    val scheduledCall = context.system.scheduler.scheduleAtFixedRate(Duration.Zero, 100.milliseconds, replica, Snapshot(key, valueOption, seq))
    context.become(waiting(seq, scheduledCall))
