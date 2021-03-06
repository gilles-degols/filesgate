package net.degols.libs.filesgate.core.pipelinemanager

import akka.actor.{ActorContext, ActorRef}
import javax.inject.Inject
import net.degols.libs.cluster.messages.Communication
import net.degols.libs.filesgate.core._
import net.degols.libs.filesgate.core.pipelineinstance.PipelineInstanceActor
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, PipelineInstanceMetadata, PipelineMetadata}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

/**
  * Handle all PipelineInstances for a given a pipeline id.
  */
class PipelineManager @Inject()(filesgateConfiguration: FilesgateConfiguration) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Set by the PipelineManagerActor when it is started
  var context: ActorContext = _

  // Set by the PipelineManagerActor once it has received a message from the EngineActor. Cannot be changed afterwards
  private var _id: Option[String] = None
  def setId(id: String): Unit = {
    if(_id.isDefined) {
      throw new Exception("The id of a PipelineManager cannot be changed once initialized!")
    } else {
      _id = Option(id)
    }
  }
  def id: Option[String] = _id

  /**
    * Metadata of the current pipeline manager.
    * Must remain a lazy val to be sure that we have the _id. And it should fail if we found no configuration for the pipeline.
    */
  lazy val pipelineMetadata: PipelineMetadata = {
    filesgateConfiguration.pipelines.find(_.id == _id.get).get
  }

  /**
    * Contain the status of every PipelineInstance (are they running or not)
    * New PipelineInstances could be created later on based on the LoadBalancer we used, so we cannot rely on any configuration
    * for that.
    * The key is the actorRef of the PipelineInstance.
    */
  var pipelineInstances: Map[String, PipelineInstanceStatus] = Map.empty[String, PipelineInstanceStatus]

  /**
    * Do we already have the pipelineInstance for a given number id?
    */
  def isPipelineInstanceFulfilledOrWaiting(numberId: Int): Boolean = pipelineInstances.values.exists(inst => {
    inst.numberId.isDefined &&
      inst.numberId.get == numberId &&
      inst.pipelineManagerId.get == id.get &&
      (inst.state == PipelineInstanceWaiting || inst.state == PipelineInstanceRunning)
  })

  /**
    * Check status of every PipelineInstance. From the Communication object we can detect how many PipelineInstance we have,
    * but we cannot use all of them, as other PipelineManagers also want to access a specific amount of PipelineInstance.
    * For now, to ease the work, we have a fix amount of PipelineInstances to start from the configuration.
    *
    * If we still don't have any information from the PipelineInstance we try to contact them
    * to give them their id (simple number), their pipeline manager id, and ask them to start working. If we still didn't get an answer since the last attempt, we send a new message.
    *
    * Some PipelineInstances are used by other PipelineManagers, so we need to be sure that we can use them and avoid sending
    * them messages if we know that we cannot use them.
    *
    * The PipelineInstances will be in charge of verifying if they have enough worker for them to really start working
    */
  def checkEveryPipelineInstanceStatus(): Unit = {
    // TODO: In very specific case, we might use a bit more PipelineInstances than we should (if we sent the message and before receiving a result we sent the message to another actor)

    // We try to detect the available instances based on the Communication system
    val unknownInstances = freePipelineInstanceActors().map(actorRef => {
      val p = PipelineInstanceStatus(None, PipelineInstanceUnreachable, None)
      p.setActorRef(actorRef)
      p
    })

    // We take all instances which we didn't identify yet, or if they didn't reply yet, and we shuffle them
    val unreachableInstances: ListBuffer[PipelineInstanceStatus] = Random.shuffle(pipelineInstances.values
      .filter(status => status.pipelineManagerId.isEmpty || status.pipelineManagerId.get == id.get)
      .filter(_.isUnreachable).toList ::: unknownInstances).to[ListBuffer]

    pipelineMetadata.pipelineInstancesMetadata
      .filter(instanceMetadata => {
        val res = !isPipelineInstanceFulfilledOrWaiting(instanceMetadata.numberId)
        if(!res) {
          logger.debug(s"Seems like the instance ${instanceMetadata.numberId} for ${id.get} is already fullfilled / in progress.")
        }
        res
      })
      .flatMap(instanceMetadata => {
        // We take a first free / unreachable instance and remove it from the listbuffer
        unreachableInstances.headOption match {
          case Some(inst) =>
            unreachableInstances.remove(0)
            Some((instanceMetadata, inst))
          case None =>
            logger.warn("No PipelineInstance available for now.")
            None
        }
    }).foreach{case (instanceMetadata: PipelineInstanceMetadata, instanceStatus: PipelineInstanceStatus) =>
      // The actor ref should always exist based on the code above
      val destActorRef = instanceStatus.actorRef.get

      logger.debug(s"Try to contact a PipelineInstance ($destActorRef) to see if it can handle the pipeline manager '${id.get}' for instance number ${instanceMetadata.numberId}")


      Try {
        val msg = PipelineInstanceToHandle(instanceMetadata.numberId, id.get)
        Communication.sendWithoutReply(context.self, destActorRef, msg)
        // We need to remember to which actor we sent the message, to be sure to not assign the work to anybody else
        instanceStatus.setActorRef(destActorRef)
        context.watch(destActorRef)
      } match {
        case Success(res) => // Nothing to do
        case Failure(err) =>
          logger.warn(s"Impossible to send the work assignment to a PipelineInstance for ${instanceStatus.pipelineManagerId}")
          instanceStatus.removeActorRef()
          context.unwatch(destActorRef)
      }

      // Add the instance status to the map if we didn't have it previously
      pipelineInstances.get(destActorRef.toString()) match {
        case Some(obj) => // Nothing to do, we had a reference
        case None => pipelineInstances = pipelineInstances ++ Map(destActorRef.toString() -> instanceStatus)
      }
    }
  }

  /**
    * When a PipelineManager received its work order, it sent back an acknowledgement. We use it to update the status locally
    * TODO: We should handle concurrency problems if we receive a very old acknowledgement from a PipelineInstance.
    */
  def ackFromPipelineInstance(sender: ActorRef, message: PipelineInstanceWorkingOn): Unit = {
    val pipelineInstance = pipelineInstances.get(sender.toString())

    pipelineInstance match {
      case Some(status) =>
        if(status.pipelineManagerId.isDefined && status.pipelineManagerId.get != message.pipelineManagerId) {
          logger.error("We received an ack for an actor ref already assigned to another PipelineManager. This should never happen, system behavior is unknown in this case.")
        } else {
          // The PipelineInstance can work on another PipelineManager, that's up to another method called frequently to
          // check if we have enough instances
          status.numberId = Option(message.numberId)
          status.pipelineManagerId = Option(message.pipelineManagerId)
          status.state = PipelineInstanceWaiting
        }
      case None => logger.error(s"Got an acknowledgement from $sender for an actor ref not linked to a known PipelineInstance...")
    }
  }

  /**
    * When a PipelineInstance has died we are notified, in that case we remove it from the known PipelineInstances
    */
  def diedActorRef(actorRef: ActorRef): Unit = {
    val pipelineInstance = pipelineInstances.get(actorRef.toString())

    pipelineInstance match {
      case Some(status) => pipelineInstances = pipelineInstances.filterKeys(_ != actorRef.toString())
      case None => logger.error(s"Got a Terminated($actorRef) for an actor ref not linked to a known PipelineInstance...")
    }
  }

  /**
    * Find PipelineInstanceActors having not yet any id based on the known actorref locally
    */
  def freePipelineInstanceActors(): List[ActorRef] = {
    val knownActorRefs: Map[ActorRef, Boolean] = pipelineInstances.values.filter(_.actorRef.isDefined).map(_.actorRef.get -> true).toMap
    val fullName = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, PipelineInstanceActor.NAME)
    Communication.actorRefsForId(fullName).filterNot(knownActorRefs.contains)
  }

}
