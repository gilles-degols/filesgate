package net.degols.filesgate.libs.filesgate.core.pipelineinstance

import akka.NotUsed
import akka.actor.{ActorContext, ActorRef}
import net.degols.filesgate.libs.cluster.messages.Communication
import net.degols.filesgate.libs.filesgate.core._
import net.degols.filesgate.libs.filesgate.pipeline.datasource.{DataSource, DataSourceSeed}
import net.degols.filesgate.libs.filesgate.utils.{FilesgateConfiguration, PipelineMetadata, Step}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Random, Success, Try}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import net.degols.filesgate.libs.filesgate.Tools
import net.degols.filesgate.libs.filesgate.orm.FileMetadata

import scala.concurrent.duration._

/**
  * Work for only one PipelineInstanceActor linked to only one PipelineManager. Handle multiple PipelineStepActors
  */
class PipelineInstance(filesgateConfiguration: FilesgateConfiguration, val pipelineGraph: PipelineGraph) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Set by the PipelineInstanceActor when it is started
  var context: ActorContext = _

  // Set by the PipelineInstanceActor once it has received a message from the EngineActor. Cannot be changed afterwards
  private var _id: Option[String] = None
  def setId(id: String): Unit = {
    if(_id.isDefined) {
      throw new Exception("The id of a PipelineInstance cannot be changed once initialized!")
    } else {
      _id = Option(id)
    }
  }
  def id: Option[String] = _id

  private var _pipelineManagerId: Option[String] = None
  def setPipelineManagerId(pipelineManagerId: String): Unit = {
    if(_pipelineManagerId.isDefined) {
      throw new Exception("The pipelineManagerId of a PipelineInstance cannot be changed once initialized!")
    } else {
      _pipelineManagerId = Option(pipelineManagerId)
    }
  }
  def pipelineManagerId: Option[String] = _pipelineManagerId

  // Temporary
  var _graphIsRunning: Boolean = false

  /**
    * Metadata of the current pipeline.
    * Must remain a lazy val to be sure that we have the _id. And it should fail if we found no configuration for the pipeline.
    */
  lazy val pipelineMetadata: PipelineMetadata = filesgateConfiguration.pipelines.find(_.id == _pipelineManagerId.get).get

  /**
    * Contain the status of every PipelineStep (are they running or not)
    * New PipelineSteps could be created later on based on the LoadBalancer we used, so we cannot rely on any configuration
    * for that.
    * The key is the actorRef of the PipelineStep.
    */
  var pipelineSteps: Map[String, PipelineStepStatus] = Map.empty[String, PipelineStepStatus]


  // Last incremental id for the PipelineStep that we want to use
  var pipelineStepLastId: Long = 0L

  /**
    * Verify if we have a PipelineStatus Waiting/Running for a given step
    * @param step
    * @return
    */
  def isStepFullFilled(step: Step): Boolean = {
    // We filter by PipelineStep working for the current PipelineInstanceId
    // We need to look for pipeline step having the same "full name" as the one in the given "step" attribute
    pipelineSteps.values.filter(_.pipelineInstanceId == id.get).filter(_.fullName == step.name).exists(!_.isUnreachable)
  }

  /**
    * Check status of every PipelineStep. From the Communication object we can detect how many PipelineInstance we have,
    * but we cannot use all of them, as other PipelineInstances also want to access a specific amount of PipelineSteps.
    * So, we use the configuration to now how many actors we would like by pipelineStep.
    * TODO: For now we arbitrarily ask for one PipelineStep of each type for each PipelineInstance. In the future we should
    * allow multiple of them.
    *
    * If we still don't have any information from the PipelineStep we try to contact them
    * to give them their id, their pipeline instance id, their pipeline manager id, and ask them to start working.
    * If we still didn't get an answer since the last attempt, we send a new message.
    *
    * Some PipelineSteps might reply that they are already to busy to work for another pipeline (as they are working for PipelineInstances)
    * so we will receive a message back saying that (=the pipeline instance for which they are working)
    *
    */
  def checkEveryPipelineStepStatus(): Unit = {
    // TODO: In very specific case, we might use a bit more PipelineInstances than we should (if we sent the message and before receiving a result we sent the message to another actor)
    val missingSteps = pipelineMetadata.steps.filterNot(isStepFullFilled)

    if(missingSteps.isEmpty && !isGraphRunning) {
      logger.info("We have all PipelineSteps necessary to start our PipelineInstance, we try to create the graph.")
      launchWork()
    }

    // TODO: We should try to find step in the same node, or even the same jvm if possible, that would reduce the inter-nodes
    // bandwidth quite a lot, reduce the latency, and increase the availability. But sometimes it's not possible to have that,
    // so we should have a fallback, and also be ready to tear down a graph, and use other actors if they pop up
    missingSteps.foreach(step => {
      val missingInstances = pipelineSteps.values.filter(_.pipelineInstanceId == id.get).filter(_.isUnreachable)

      // For now we only want 1 actor for each step
      val instanceToContact = Random.shuffle(freePipelineStepActors(step.name)).headOption
      instanceToContact match {
        case Some(act) =>
          logger.debug(s"Try to contact a PipelineStep to see if it can handle ourselves '${id.get}'")

          // Unique id
          val pipelineStepId: String = s"${id.get}-${pipelineStepLastId}"
          pipelineStepLastId += 1L

          Try {
            val msg = PipelineStepToHandle(pipelineStepId, id.get, pipelineManagerId.get, step.name)
            Communication.sendWithoutReply(context.self, act, msg)
            // We will add the watcher when we receive the reply
          } match {
            case Success(res) => // Nothing to do
            case Failure(err) =>
              logger.warn(s"Impossible to send the work assignment to a PipelineInstance for ${id.get}")
          }

          // To make the code simpler, we don't add the PipelineStep to our local map for the moment, we'll wait for
          // the reply to do that

        case None =>
          logger.warn(s"No PipelineStep available for PipelineInstance ${id.get} and the step: ${step.name}")
      }
    })
  }

  /**
    * Check if the graph is already running, in that case there is no need to re-create it
    */
  def isGraphRunning: Boolean = _graphIsRunning

  /**
    * In charge of launching the pipeline of actors to work together as a streaming process (if not yet done)
    */
  def launchWork(): Unit = {
    _graphIsRunning = true

    pipelineGraph.loadGraph(pipelineMetadata, pipelineSteps)
  }



  /**
    * When a PipelineStep received its work order, it sent back an acknowledgement. We use it to update the status locally.
    * The PipelineStep can refuse to work for us.
    * TODO: We should handle concurrency problems if we receive a very old acknowledgement from a PipelineStep.
    */
  def ackFromPipelineStep(sender: ActorRef, message: PipelineStepWorkingOn): Unit = {
    val pipelineStep = pipelineSteps.get(sender.toString())

    pipelineStep match {
      case Some(status) =>
      case None => logger.error(s"Got an acknowledgement from $sender for an actor ref not linked to a known PipelineInstance (${pipelineSteps.keys.mkString(",")})...")
        if(message.pipelineInstanceId == id.get) {
          Try {
            context.watch(sender)
          } match {
            case Success(res) =>
              logger.debug(s"The PipelineStep ${message.id} has accepted to work for us ($id).")
              val status = PipelineStepStatus(message.name, id.get, PipelineStepWaiting)
              status.setActorRef(sender)
              pipelineSteps = pipelineSteps ++ Map(sender.toString() -> status)
            case Failure(err) => logger.error(s"Impossible to watch a PipelineStep($sender)")
          }
        } else {
          logger.debug(s"The PipelineStep ${message.id} has refused to work for us ($id).")
          val status = PipelineStepStatus(message.name, id.get, PipelineStepUnknown)
          pipelineSteps = pipelineSteps ++ Map(sender.toString() -> status)
          // We do not need to watch it. Rather, when we have a full graph, we should remove all the other entries now useless.
        }
    }
  }

  /**
    * When a PipelineInstance has died we are notified, in that case we remove it from the known PipelineInstances
    */
  def diedActorRef(actorRef: ActorRef): Unit = {
    val pipelineStep = pipelineSteps.get(actorRef.toString())

    pipelineStep match {
      case Some(status) => pipelineSteps = pipelineSteps.filterKeys(_ != actorRef.toString())
      case None => logger.error(s"Got a Terminated($actorRef) for an actor ref not linked to a known PipelineStep...")
    }
  }

  /**
    * Find PipelineStepsActors having not yet any id based on the known actorref locally, for a given pipeline step
    */
  def freePipelineStepActors(pipelineStepFullName: String): List[ActorRef] = {
    val knownActorRefs: Map[ActorRef, Boolean] = pipelineSteps.values.filter(_.actorRef.isDefined).map(_.actorRef.get -> true).toMap
    Communication.actorRefsForId(pipelineStepFullName).filterNot(knownActorRefs.contains)
  }

}
