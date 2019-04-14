/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

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

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case op: Operation => root forward op
    case GC =>
      val newRoot = createRoot
      pendingQueue = Queue.empty[Operation]
      context become garbageCollecting(newRoot)
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished =>
      pendingQueue.foreach(newRoot ! _)
      root = newRoot
      context unbecome()
    case GC => // ignore
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {
    case Insert(requester, id, e) =>
      if (e == elem && !removed) {
        removed = false
        requester ! OperationFinished(id)
      } else {
        val nextPos = resolvePosition(e)
        subtrees.get(nextPos) match {
          case Some(ref) =>
            ref ! Insert(requester, id, e)
          case None =>
            subtrees += (nextPos -> context.actorOf(props(e, initiallyRemoved = false)))
            requester ! OperationFinished(id)
        }
      }

    case Contains(requester, id, e) =>
      if (e == elem && !removed) {
        requester ! ContainsResult(id, result = true)
      } else {
        val nextPos = resolvePosition(e)
        subtrees.get(nextPos) match {
          case Some(ref) =>
            ref ! Contains(requester, id, e)
          case None =>
            requester ! ContainsResult(id, result = false)
        }
      }

    case Remove(requester, id, e) =>
      if (e == elem && !removed) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        val nextPos = resolvePosition(e)
        subtrees.get(nextPos) match {
          case Some(ref) =>
            ref ! Remove(requester, id, e)
          case None =>
            requester ! OperationFinished(id)
        }
      }

    case CopyTo(treeNode) =>
      val nodes = subtrees.values.toSet
      if (removed && nodes.isEmpty) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        context become copying(nodes, removed)
        if (!removed) treeNode ! Insert(self, -1, elem)
        nodes.foreach(_ ! CopyTo(treeNode))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        context become copying(expected, insertConfirmed = true)
      }

    case CopyFinished =>
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        context become copying(newExpected, insertConfirmed)
      }
  }

  private def resolvePosition(e: Int): Position = if (e > elem) Right else Left
}
