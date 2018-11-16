package net.degols.filesgate.libs.filesgate.core.pipelinemanager

import akka.actor.ActorContext
import net.degols.filesgate.libs.cluster.core.Cluster
import net.degols.filesgate.libs.filesgate.core.{RemotePipelineStep, RemoteStartPipelineStepInstance}
import net.degols.filesgate.service.Tools
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
  * Handle everything related to a specific Pipeline. The related pipeline to handle is given by the EngineActor afterwards.
  * We can have a lot of those PipelineManager started by default as we are not sure how many are needed by the user. They
  * will remain idle. Each PipelineManager is in charge of sending jobs to PipelineInstances.
  * In short:
  *  -> PipelineManager cannot be customized by the developer. Any PipelineManager can be linked to any pipeline defined by the user
  *  -> many PipelineManagers to handle all types of pipeline.
  *  -> one pipelineManager by type of pipeline
  *  -> each PipelineManager, based on the PipelineMetadata, is linked to a specific set of PipelineInstances (to be able to have a specific load balancer for each of them)
  *  -> the PipelineInstances are in charge of downloading the files themselves, in a streaming way. So we can have 1000 actors running at the same time in some cases.
  *  -> Each PipelineInstance will be linked to various actors, to read urls to download, to download them, etc. Sometimes we have 1 actor to read a lot of data, and 10 actors to download, then again 1 actor to write them, all those things linked to one PipelineInstance.
  * @param pipelineMetadata
  */
class PipelineManagerActor(pipelineMetadata: PipelineMetadata, configurationService: ConfigurationService, tools: Tools, cluster: Cluster) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private var _running: Boolean = false
  def setRunning(running: Boolean): Unit = _running = running
  def running: Boolean = _running

  // Set by the EngineActor
  var context: ActorContext = _

  def id: String = pipelineMetadata.id

  /**
    * Do we have all the pipeline steps to have a running cluster?
    * @return
    */
  def hasAllPipelineSteps: Boolean = {
    val existingPipelineSteps: List[PipelineStep] = cluster.nodes.flatMap(node => node.filesGateInstances).flatMap(_.pipelineSteps)
    pipelineMetadata.stepIds.forall(stepId => existingPipelineSteps.exists(_.id == stepId))
  }

  /**
    * Try to start the Engine by asking the creation of the PipelineStepInstances, or to resume them.
    */
  def startPipelineInstances(): Try[Unit] = Try{
    val distribution = cluster.nodesByPipelineStep()
    distribution.map(nodeAndStep => {
      val instances = nodeAndStep._1.pipelineStepInstances
      (nodeAndStep, expectedInstancesForStep(nodeAndStep._1) - instances.size)
    }).filter(_._2 > 0).foreach(nodeAndStep => {
      val step = nodeAndStep._1._1
      val wantedInstances = nodeAndStep._2

      for(i <- 0 to wantedInstances) {
        startPipelineInstance(step)
      }
    })
  }

  def stopPipelineInstances(): Try[Unit] = Try{

  }

  /**
    * Expected instances of a given PipelineStepId. For now, we simply ask always the same number from the configuration.
    * Later on we might want to compute that dynamically based on the queue of every actor.
    */
  def expectedInstancesForStep(pipelineStep: PipelineStep): Int = {
    pipelineMetadata.instancesByStep
  }

  /**
    * Start a PipelineInstance, does not care if it's a good idea or not
    * @param pipelineStep
    */
  private def startPipelineInstance(pipelineStep: PipelineStep): Try[Unit] = Try {
    val bestNode = cluster.bestNodeForStep(pipelineStep)
    logger.debug(s"Start PipelineStepInstances for ${pipelineStep} on node $bestNode")
    bestNode match {
      case Some(node) =>
        val destActor = node.filesGateInstances.head.actorRef
        val remotePipelineStep = RemotePipelineStep.from(pipelineStep)
        Communication.sendWithoutReply(context.self, destActor, RemoteStartPipelineStepInstance(remotePipelineStep)) match {
          case Success(res) =>
            // TODO: Verify the returned type...
            logger.debug("Succeeded to start a PipelineStepInstance.")
          case Failure(err) =>
            logger.error("Impossible to start the PipelineInstance...")
            throw err
        }
      case None => logger.error("We should have a best node available...")
    }
  }
}

object PipelineManagerActor {
  val name: String = "Core.PipelineManagerActor"
}