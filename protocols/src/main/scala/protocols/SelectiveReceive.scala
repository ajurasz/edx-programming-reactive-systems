package protocols

import akka.actor.typed.{ActorContext, _}
import akka.actor.typed.scaladsl._

object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
      *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                   always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
      * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = {
        val buffer = StashBuffer[T](bufferSize)
        new Stasher[T](initialBehavior, buffer, bufferSize)
    }

    private class Stasher[T](behavior: Behavior[T], buffer: StashBuffer[T], bufferSize: Int) extends ExtensibleBehavior[T] {
        override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
            val started = Behavior.validateAsInitial(Behavior.start(behavior, ctx))
            val next = Behavior.interpretMessage(started, ctx, msg)
            val canonical = Behavior.canonicalize(next, behavior, ctx)
            if (Behavior.isUnhandled(next)) {
                buffer.stash(msg)
                new Stasher[T](canonical, buffer, bufferSize)
            } else if (buffer.nonEmpty) {
                buffer.unstashAll(ctx.asScala, new Stasher[T](canonical, StashBuffer[T](bufferSize), bufferSize))
            } else {
                new Stasher[T](canonical, buffer, bufferSize)
            }
        }

        override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = ???
    }
}
