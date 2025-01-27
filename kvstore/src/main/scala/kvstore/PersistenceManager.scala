package kvstore

import akka.actor.{Props, Actor, actorRef2Scala}
import scala.util.Random
import akka.actor.ActorRef
import scala.concurrent.duration.*
import akka.actor.Cancellable
import akka.actor.PoisonPill

object PersistenceManager:
  def props(persistence: ActorRef): Props = Props(PersistenceManager(persistence))

class PersistenceManager(persistence: ActorRef) extends Actor:
  import Persistence.*
  import context.dispatcher

  def receive: Receive =
    case Persist(key, valueOption, id) => 
      val scheduledCall = context.system.scheduler.scheduleAtFixedRate(Duration.Zero, 100.milliseconds, persistence, Persist(key, valueOption, id))
      val failingCall = context.system.scheduler.scheduleOnce(1.second) {
        self ! PersistFailed(id)
      }
      context.become(waitingForConfirmation(scheduledCall))

  def waitingForConfirmation(scheduledCall: Cancellable): Receive = 
    case Persisted(key, id) =>
      context.parent ! Persisted(key, id)
      scheduledCall.cancel()
      context.self ! PoisonPill

    case PersistFailed(id) => 
      context.parent ! PersistFailed(id)
      scheduledCall.cancel()
      context.self ! PoisonPill
