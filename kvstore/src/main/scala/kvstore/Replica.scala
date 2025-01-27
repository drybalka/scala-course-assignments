package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import kvstore.Arbiter.*
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration.*
import akka.util.Timeout
import akka.actor.Cancellable
import akka.actor.Kill

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Exception => SupervisorStrategy.Restart
  }
  val persistence: ActorRef = context.actorOf(persistenceProps)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  type Sender = ActorRef
  type Manager = ActorRef
  var operations = Map.empty[Long, (Sender, Manager)]

  arbiter ! Join

  def receive =
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0))

  val leader: Receive =
    case Insert(key, value, id) =>
      kv += key -> value 
      val manager = context.actorOf(ReplicationManager.props(persistence, secondaries.values.toSet, Replicate(key, Some(value), id)))
      operations += id -> (sender(), manager)

    case Remove(key, id) =>
      kv -= key
      val manager = context.actorOf(ReplicationManager.props(persistence, secondaries.values.toSet, Replicate(key, None, id)))
      operations += id -> (sender(), manager)

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case OperationAck(id) =>
      operations.get(id).map((sender, _) => sender ! OperationAck(id))
      operations -= id
      
    case OperationFailed(id) =>
      operations.get(id).map((sender, _) => sender ! OperationFailed(id))
      operations -= id

    case Replicas(replicas) =>
      replicas
        .filter(_ != self)
        .filter(replica => !secondaries.keySet.contains(replica))
        .foreach { replica =>
          val replicator = context.actorOf(Replicator.props(replica))
          secondaries += replica -> replicator
          kv.foreachEntry { (key, value) => replicator ! Replicate(key, Some(value), 0)}
        }
      secondaries
        .filter((replica, _) => !replicas.contains(replica))
        .foreach {(_, replicator) =>
          replicator ! PoisonPill
        }
      secondaries = secondaries.filter((replica, _) => replicas.contains(replica))
      operations.values.foreach { (_, manager) =>
        manager ! Replicas(replicas)
      }


  var senders = Map.empty[Long, Sender]
  def replica(currentSeq: Long): Receive =
    case Snapshot(key, valueOption, seq) =>
      if seq < currentSeq then
        sender() ! SnapshotAck(key, seq)
      else if seq == currentSeq then
        kv = kv.updatedWith(key)(_ => valueOption)
        senders += seq -> sender()
        context.actorOf(PersistenceManager.props(persistence)) ! Persist(key, valueOption, seq)
        context.become(replica(seq + 1))

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Persisted(key, id) =>
      senders(id) ! SnapshotAck(key, id)
      senders -= id

    case PersistFailed(id) =>
      senders -= id
