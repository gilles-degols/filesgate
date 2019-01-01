package net.degols.libs.filesgate.core.pipelineinstance

import akka.actor.{ActorContext, ActorRef}
import net.degols.libs.cluster.balancing.BasicLoadBalancer
import net.degols.libs.cluster.messages.Communication
import net.degols.libs.filesgate.core._
import net.degols.libs.filesgate.utils._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success, Try}

/**
  * Work for only one PipelineInstanceActor linked to only one PipelineManager. Handle multiple PipelineStepActors
  */
class PipelineInstance(filesgateConfiguration: FilesgateConfiguration, val pipelineGraph: PipelineGraph)(implicit val ec: ExecutionContext) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Set by the PipelineInstanceActor when it is started
  var context: ActorContext = _

  def id: Option[String] = {
    numberId match {
      case Some(numId) =>
        Option(s"${pipelineManagerId.get}-$numId")
      case None => None
    }
  }

  // Set by the PipelineInstanceActor once it has received a message from the EngineActor. Cannot be changed afterwards
  private var _numberId: Option[Int] = None
  def setNumberId(numberId: Int): Unit = {
    if(_numberId.isDefined) {
      throw new Exception("The number-id of a PipelineInstance cannot be changed once initialized!")
    } else {
      _numberId = Option(numberId)
    }
  }
  def numberId: Option[Int] = _numberId

  private var _pipelineManagerId: Option[String] = None
  def setPipelineManagerId(pipelineManagerId: String): Unit = {
    if(_pipelineManagerId.isDefined) {
      throw new Exception("The pipelineManagerId of a PipelineInstance cannot be changed once initialized!")
    } else {
      _pipelineManagerId = Option(pipelineManagerId)
    }
  }
  def pipelineManagerId: Option[String] = _pipelineManagerId

  /**
    * To be sure to only start once instance of the pipeline
    */
  var _graphIsRunning: Boolean = false

  /**
    * If the stream finishes successfully, we might not want to restart it (it depends on the developer).
    */
  var _graphIsFinished: Boolean = false

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

  lazy val pipelineInstanceMetadata: PipelineInstanceMetadata = pipelineMetadata.pipelineInstancesMetadata.find(_.numberId == numberId.get).get


  /**
    * Verify if we have a PipelineStatus Waiting/Running for a given step. We do not care to have all the actor instances
    * for a given pipeline step, only one available at a time is enough to start a Pipeline
    * @param step
    * @return
    */
  def isStepFullFilled(step: Step): Boolean = {
    // We filter by PipelineStep working for the current PipelineInstanceId
    // We need to look for pipeline step having the same "full name" as the one in the given "step" attribute

    // We do not use the load balancer information here to find all actor ref, as the load balancer can create new actors
    // at any time. So we simply use any actor available.
    pipelineSteps.values.filter(_.pipelineInstanceId == id.get).filter(_.step.name == step.name).exists(!_.isUnreachable)
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
    val missingSteps = pipelineInstanceMetadata.steps.filterNot(isStepFullFilled)

    if(missingSteps.isEmpty && !isGraphRunning && !isGraphFinished) {
      // Note: we do not care if we do not have all the actors for a specific step, as long as we have 1 of them it's enough to start the graph.
      logger.info(s"We have all PipelineSteps necessary to start our PipelineInstance ${id.get}, we try to create the graph.")
      launchWork()
    } else if(isGraphFinished) {
      logger.info(s"The PipelineInstance ${id.get} is finished successfully, we do not restart it.")
    }

    // We might want some statistics about the running steps
    if(isGraphRunning) {
      pipelineSteps.filter(_._2.pipelineInstanceId == id.get).values.map(stepStatus => {
        Communication.sendWithoutReply(context.self, stepStatus.actorRef.get, GetActorStatistics())
      })
    }

    // TODO: We should try to find step in the same node, or even the same jvm if possible, that would reduce the inter-nodes
    // bandwidth quite a lot, reduce the latency, and increase the availability. But sometimes it's not possible to have that,
    // so we should have a fallback, and also be ready to tear down a graph, and use other actors if they pop up
    pipelineInstanceMetadata.steps.foreach(step => {
      // Each step.name is linked to the appropriate Pipeline instance, and we need to use every available actor (we can have
      // more than 1 actor/step)
      val availableActors = freePipelineStepActors(step.name)
      if(availableActors.isEmpty && !isStepFullFilled(step)) {
        logger.warn(s"No PipelineStep available for PipelineInstance ${id.get} and the step: ${step.name}")
      }

      availableActors.foreach(instanceToContact => {
        logger.debug(s"Try to contact a PipelineStep to see if it can handle ourselves '${id.get}', we want it to work on step: $step")

        // Unique id
        val pipelineStepId: String = s"${id.get}-${pipelineStepLastId}"
        pipelineStepLastId += 1L

        Try {
          val msg = PipelineStepToHandle(pipelineStepId, id.get, pipelineManagerId.get, step)
          Communication.sendWithoutReply(context.self, instanceToContact, msg)
          // We will add the watcher when we receive the reply
        } match {
          case Success(res) => // Nothing to do
          case Failure(err) =>
            logger.warn(s"Impossible to send the work assignment to a PipelineInstance for ${id.get}")
        }

        // To make the code simpler, we don't add the PipelineStep to our local map for the moment, we'll wait for
        // the reply to do that
      })
    })
  }

  /**
    * Check if the graph is already running, in that case there is no need to re-create it.
    */
  def isGraphRunning: Boolean = _graphIsRunning

  /**
    * Check if the graph is already finished
    */
  def isGraphFinished: Boolean = _graphIsFinished

  /**
    * In charge of launching the pipeline of actors to work together as a streaming process (if not yet done)
    */
  def launchWork(): Unit = {
    _graphIsRunning = true

    // We load the graph, then we check the stream to be sure to relaunch it if it fails / finish (the stream is not
    // supposed to finish)
    pipelineGraph.setPipelineStepStatuses(pipelineSteps.values.toList)
    pipelineGraph.loadGraph(pipelineMetadata, pipelineInstanceMetadata)

    val stream = pipelineGraph.stream()
    if(stream == null) {
      logger.error("No stream found after asking PipelineGraph to create one. We assume the stream could not be created at all. We will retry in a short time.")
      _graphIsRunning = false
    } else {
      stream.onComplete{
        case Success(res) =>
          if(pipelineMetadata.restartWhenFinished) {
            logger.warn("Stream finished successfully, but the configuration asks to restart it in this case.")
            // Nothing specific to do in this case
          } else {
            logger.info("Stream finished successfully, and the configuration did not ask to restart it in this case.")
            _graphIsFinished = true
          }
          _graphIsRunning = false
        case Failure(err) =>
          logger.error(s"Stream finished with an error: ${net.degols.libs.cluster.Tools.formatStacktrace(err)}")
          _graphIsRunning = false
      }
    }
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
        // TODO: We should handle the update of actors
        logger.warn(s"We already have a status for the given pipeline step ${message.id}. Do nothing.")
      case None =>
        if(message.pipelineInstanceId == id.get) {
          Try {
            context.watch(sender)
          } match {
            case Success(res) =>
              logger.debug(s"The PipelineStep ${message.id} has accepted to work for us ($id).")
              val status = PipelineStepStatus(message.step, id.get, PipelineStepWaiting)
              status.setActorRef(sender)
              pipelineSteps = pipelineSteps ++ Map(sender.toString() -> status)
              // We need to update the PipelineGraph information, as it needs to use the new actor ref
              pipelineGraph.setPipelineStepStatuses(pipelineSteps.values.toList)

            case Failure(err) => logger.error(s"Impossible to watch a PipelineStep($sender)")
          }
        } else {
          logger.debug(s"The PipelineStep ${message.id} has refused to work for us ($id).")
          val status = PipelineStepStatus(message.step, id.get, PipelineStepUnknown)
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
      case Some(status) =>
        pipelineSteps = pipelineSteps.filterKeys(_ != actorRef.toString())
        pipelineGraph.setPipelineStepStatuses(pipelineSteps.values.toList)
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

  /**
    * When we received the actorStatistics from a PipelineStep
    */
  def storeActorStatistics(sender: ActorRef, actorStatistics: ActorStatistics): Unit = {
    pipelineSteps.get(sender.toString()) match {
      case Some(stepStatus) =>
        stepStatus.setActorStatistics(actorStatistics)
      case None =>
        logger.warn(s"Got statistics from an unknown sender: $sender")
    }
  }

  /**
    * Display instance statistics to ease the debug and increase performance
    */
  def displayInstanceStatistics(): Unit = {
    if(isGraphRunning) {
      // To have the steps ordered correctly we need to start from the step config
      val text = pipelineInstanceMetadata.steps.map(step => {
        pipelineSteps.values.filter(_.pipelineInstanceId == id.get).filter(_.step.name == step.name).map(stepStatus => {
          val startStepText = s"[${id.get}] ${stepStatus.step.name}"
          val endStepText = stepStatus.actorStatistics match {
            case Some(stats) =>
              s"${stats.totalProcessedMessage} messages, ${math.round(stats.averageProcessingTime*100.0)/100.0} ms/message, last message @ ${stats.lastMessageDateTime}"
            case None =>
              "No statistics"
          }
          s"$startStepText - $endStepText"
        }).mkString("\n")
      }).mkString("\n")

      // We display stats that we already have
      logger.debug(s"Statistics for ${id}:\n${text}")
    }
  }
}
