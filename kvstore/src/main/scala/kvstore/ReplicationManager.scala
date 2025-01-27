package kvstore

import akka.actor.{Props, Actor, actorRef2Scala}
import scala.util.Random
import akka.actor.ActorRef
import scala.concurrent.duration.*
import akka.actor.Cancellable
import akka.actor.PoisonPill
import kvstore.Replicator.Replicate
import kvstore.Replicator.Replicated
import kvstore.Replica.OperationAck
import kvstore.Replica.OperationFailed
import kvstore.Arbiter.Replicas

object ReplicationManager:
  def props(persistence: ActorRef, secondaries: Set[ActorRef], operation: Replicate): Props = Props(ReplicationManager(persistence, secondaries, operation))

class ReplicationManager(persistence: ActorRef, secondaries: Set[ActorRef], operation: Replicate) extends Actor:
  import Persistence.*
  import context.dispatcher

  var isPersisted = false
  var secondariesPending = secondaries

  val Replicate(key, valueOption, id) = operation
  context.actorOf(PersistenceManager.props(persistence)) ! Persist(key, valueOption, id)
  secondaries.foreach(_ ! Replicate(key, valueOption, id))

  val failingCall = context.system.scheduler.scheduleOnce(1.second) {
    self ! PersistFailed(id)
  }

  def receive: Receive =
    case Persisted(_, _) =>
      isPersisted = true
      checkFullfilled()

    case PersistFailed(_) => 
      context.parent ! OperationFailed(id)
      context.self ! PoisonPill

    case Replicated(_, _) =>
      secondariesPending -= sender()
      checkFullfilled()

    case Replicas(new_secondaries: Set[ActorRef]) =>
      secondariesPending = secondariesPending.filter(new_secondaries.contains)
      checkFullfilled()


  def checkFullfilled() =
    if isPersisted && secondariesPending.isEmpty then
      context.parent ! OperationAck(id)
      context.self ! PoisonPill
    
