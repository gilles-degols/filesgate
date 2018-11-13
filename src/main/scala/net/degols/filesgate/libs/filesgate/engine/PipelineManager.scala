package net.degols.filesgate.libs.filesgate.engine.core

import akka.actor.ActorContext
import net.degols.filesgate.engine.cluster.{Cluster, PipelineStep}
import net.degols.filesgate.service.{ConfigurationService, PipelineMetadata, Tools}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
  * Handle everything related to a specific Pipeline
  * @param pipelineMetadata
  */
class PipelineManager(pipelineMetadata: PipelineMetadata, configurationService: ConfigurationService, tools: Tools, cluster: Cluster) {
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
