package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

import scala.language.postfixOps

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  sealed trait UpdateOperation extends Operation

  case class Insert(key: String, value: String, id: Long) extends UpdateOperation

  case class Remove(key: String, id: Long) extends UpdateOperation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class RetryPersist(key: String, valueOption: Option[String], id: Long)

  case class CheckPersist(id: Long, client: ActorRef)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the map of persistence seq to replicator
  var persistenceAcks = Map.empty[Long, ActorRef]
  // the map of operation id to acks count
  var replicationAcks = Map.empty[Long, Long]

  var currentSeqNo = 0L

  var shutdownInitiated= false

  var persistence: ActorRef = _

  override def preStart(): Unit = {
    arbiter ! Join
    persistence = context.actorOf(persistenceProps)
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case message @ Insert(key, value, id) =>
      kv += (key -> value)
      replicate(replicators, message)
      val client = sender()
      acksGlogalTimeout(id, client)
      persist(key, Some(value), id)
    case message @ Remove(key, id) =>
      kv -= key
      replicate(replicators, message)
      val client = sender()
      acksGlogalTimeout(id, client)
      persist(key, None, id)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) =>
      handleRemovedReplicas(replicas)
      handleNewReplicas(replicas)
    case Replicated(key, id) =>
      for (count <- replicationAcks.get(id)) {
        if (count <= 1) {
          replicationAcks -= id
        } else {
          replicationAcks = replicationAcks.updated(id, replicationAcks(id) - 1)
        }
      }
    case RetryPersist(key, valueOption, seq) if persistenceAcks.get(seq).nonEmpty =>
      retryPersist(key, valueOption, seq)
    case Persisted(_, id) if replicationAcks.withDefaultValue(0)(id) == 0 =>
      persistenceAcks.get(id).foreach(_ ! OperationAck(id))
      persistenceAcks -= id
    case CheckPersist(id, client) =>
      if (persistenceAcks.get(id).nonEmpty || replicationAcks.get(id).nonEmpty) {
        client ! OperationFailed(id)
      }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Snapshot(key, value, seq) if seq == currentSeqNo =>
      updateKV(key, value)
      persist(key, value, seq)
    case Snapshot(key, value, seq) if seq > currentSeqNo => // ignore
    case Snapshot(key, value, seq) if seq < currentSeqNo =>
      sender() ! SnapshotAck(key, seq)
    case RetryPersist(key, valueOption, seq) if persistenceAcks.get(seq).nonEmpty =>
      retryPersist(key, valueOption, seq)
    case Persisted(key, id) if persistenceAcks.get(id).nonEmpty  =>
      persistenceAcks.get(id).foreach(_ ! SnapshotAck(key, id))
      persistenceAcks -= id
      incSeq()
    case _ =>
  }

  private def handleRemovedReplicas(replicas: Set[ActorRef]): Unit = {
    secondaries
      .filterKeys(!replicas.contains(_))
      .keySet
      .foreach(replica => {
        replica ! PoisonPill
        secondaries.get(replica).foreach(replicator => {
          replicator ! PoisonPill
          replicators -= replicator
        })
        secondaries -= replica
      })
  }

  private def handleNewReplicas(replicas: Set[ActorRef]): Unit = {
    replicas
      .filter(_ != self)
      .filter(!secondaries.contains(_))
      .foreach(replica => {
        val replicatorRef = context.actorOf(Replicator.props(replica))
        replicateStore(replicatorRef)
        replicators += replicatorRef
        secondaries += (replica -> replicatorRef)
      })
  }

  private def replicate(replicators: Set[ActorRef], message: UpdateOperation): Unit = {
    message match {
      case Insert(key, value, id) => replicators.foreach(_ ! Replicate(key, Some(value), id))
      case Remove(key, id) => replicators.foreach(_ ! Replicate(key, None, id))
    }
    replicationAcks += (message.id -> replicators.size)
  }

  private def replicateStore(replica: ActorRef): Unit = {
    var id = 0
    kv.foreach {
      case (key, value) =>
        replica ! Replicate(key, Some(value), id)
        id += 1
    }
  }

  private def persist(key: String, valueOption: Option[String], id: Long): Unit = {
    persistence ! Persist(key, valueOption, id)
    persistenceAcks += (id -> sender)
    context.system.scheduler.scheduleOnce(100 milliseconds) {
      self ! RetryPersist(key, valueOption, id)
    }
  }

  private def retryPersist(key: String, valueOption: Option[String], id: Long): Unit = {
    persistence ! Persist(key, valueOption, id)
    context.system.scheduler.scheduleOnce(100 milliseconds) {
      self ! RetryPersist(key, valueOption, id)
    }
  }

  private def acksGlogalTimeout(id: Long, client: ActorRef): Unit = {
    context.system.scheduler.scheduleOnce(1 seconds) {
      self ! CheckPersist(id, client)
    }
  }

  private def updateKV(key: String, value: Option[String]): Unit = {
    value match {
      case Some(v) =>
        kv += (key -> v)
      case None =>
        kv -= key
    }
  }

  private def incSeq(): Unit = currentSeqNo += 1
}

