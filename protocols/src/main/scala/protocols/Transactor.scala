package protocols

import akka.actor.typed.*
import akka.actor.typed.scaladsl.*

import scala.concurrent.duration.*

object Transactor:

  sealed trait PrivateCommand[T] extends Product with Serializable
  final case class Committed[T](session: ActorRef[Session[T]], value: T) extends PrivateCommand[T]
  final case class RolledBack[T](session: ActorRef[Session[T]]) extends PrivateCommand[T]

  sealed trait Command[T] extends PrivateCommand[T]
  final case class Begin[T](replyTo: ActorRef[ActorRef[Session[T]]]) extends Command[T]

  sealed trait Session[T] extends Product with Serializable
  final case class Extract[T, U](f: T => U, replyTo: ActorRef[U]) extends Session[T]
  final case class Modify[T, U](f: T => T, id: Long, reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Commit[T, U](reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Rollback[T]() extends Session[T]

  /**
    * @return A behavior that accepts public [[Command]] messages. The behavior
    *         should be wrapped in a [[SelectiveReceive]] decorator (with a capacity
    *         of 30 messages) so that beginning new sessions while there is already
    *         a currently running session is deferred to the point where the current
    *         session is terminated.
    * @param value Initial value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    */
  def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] =
      SelectiveReceive(30, idle(value, sessionTimeout).narrow)

  /**
    * @return A behavior that defines how to react to any [[PrivateCommand]] when the transactor
    *         has no currently running session.
    *         [[Committed]] and [[RolledBack]] messages should be ignored, and a [[Begin]] message
    *         should create a new session.
    *
    * @param value Value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    *
    * Note: To implement the timeout you have to use `ctx.scheduleOnce` instead of `Behaviors.withTimers`, due
    *       to a current limitation of Akka: https://github.com/akka/akka/issues/24686
    *
    * Hints:
    *   - When a [[Begin]] message is received, an anonymous child actor handling the session should be spawned,
    *   - In case the child actor is terminated, the session should be rolled back,
    *   - When `sessionTimeout` expires, the session should be rolled back,
    *   - After a session is started, the next behavior should be [[inSession]],
    *   - Messages other than [[Begin]] should not change the behavior.
    */
  private def idle[T](value: T, sessionTimeout: FiniteDuration): Behavior[PrivateCommand[T]] =
    // println(s"Starting idle with value $value")
    Behaviors.receive {
      case (ctx, Begin(replyTo)) =>
    //     println("idle received Begin")
        val session = ctx.spawnAnonymous(sessionHandler(value, ctx.self, Set.empty))
        replyTo ! session
        ctx.watchWith(session, RolledBack(session))
        ctx.scheduleOnce(sessionTimeout, ctx.self, RolledBack(session))
        inSession(value, sessionTimeout, session)
      case _ => Behaviors.same
    }

  /**
    * @return A behavior that defines how to react to [[PrivateCommand]] messages when the transactor has
    *         a running session.
    *         [[Committed]] and [[RolledBack]] messages should commit and rollback the session, respectively.
    *         [[Begin]] messages should be unhandled (they will be handled by the [[SelectiveReceive]] decorator).
    *
    * @param rollbackValue Value to rollback to
    * @param sessionTimeout Timeout to use for the next session
    * @param sessionRef Reference to the child [[Session]] actor
    */
  private def inSession[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
    // println(s"Starting inSession with rollback $rollbackValue")
    Behaviors.setup {ctx =>
      Behaviors.receiveMessagePartial {
        case Committed(session, value) if session == sessionRef =>
    //       println(s"inSession received Commited $value")
          idle(value, sessionTimeout)

        case RolledBack(session) if session == sessionRef =>
    //       println(s"inSession received RolledBack")
          ctx.stop(session)
          idle(rollbackValue, sessionTimeout)
      }
    }

  /**
    * @return A behavior handling [[Session]] messages. See in the instructions
    *         the precise semantics that each message should have.
    *
    * @param currentValue The session’s current value
    * @param commit Parent actor reference, to send the [[Committed]] message to
    * @param done Set of already applied [[Modify]] messages
    */
  private def sessionHandler[T](currentValue: T, commit: ActorRef[Committed[T]], done: Set[Long]): Behavior[Session[T]] =
    // println(s"Starting handler with value $currentValue")
    Behaviors.setup {ctx =>
      Behaviors.receiveMessagePartial {
        case Extract(f, replyTo) =>
    //       println(s"handler received Extract")
          replyTo ! f(currentValue)
          Behaviors.same

        case Modify(f, id, reply, replyTo) =>
    //       println(s"handler received Modify")
          if !done.contains(id) then
            val newValue = f(currentValue)
            val newDone = done + id
            replyTo ! reply
            sessionHandler(newValue, commit, newDone)
          else
            replyTo ! reply
            Behaviors.same
          
        case Commit(reply, replyTo) =>
    //       println(s"handler received Commit")
          commit ! Committed(ctx.self, currentValue)
          replyTo ! reply
          Behaviors.stopped

        case Rollback() =>
    //       println(s"handler received Rollback")
          Behaviors.stopped
      }      
    }
