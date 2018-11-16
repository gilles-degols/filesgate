package net.degols.filesgate.libs.filesgate.core

import java.io.File

import akka.actor.{Actor, ActorRef, Kill, Props}
import net.degols.filesgate.libs.election.{ConfigurationService, ElectionService, ElectionWrapper}
import org.slf4j.LoggerFactory
import javax.inject.{Inject, Singleton}

import scala.concurrent.duration._
import net.degols.filesgate.libs.cluster.{ClusterConfiguration, Tools}
import net.degols.filesgate.libs.cluster.core.Cluster
import net.degols.filesgate.libs.cluster.manager.{Manager, WorkerLeader}
import net.degols.filesgate.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance, JVMInstance, WorkerTypeInfo}
import net.degols.filesgate.libs.filesgate.core.engine.{Engine, EngineActor}
import net.degols.filesgate.libs.filesgate.core.pipelineinstance.{PipelineInstance, PipelineInstanceActor}
import net.degols.filesgate.libs.filesgate.core.pipelinemanager.{PipelineManager, PipelineManagerActor}
import net.degols.filesgate.libs.filesgate.utils.{ClusterConfiguration, FilesgateConfiguration}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try


/**
  * This class must be extended by the developer
  * @param engine
  * @param electionService
  * @param configurationService
  * @param clusterConfiguration
  * @param cluster
  */
@Singleton
abstract class EngineLeader @Inject()(engine: Engine,
                               electionService: ElectionService,
                                      configurationService: ConfigurationService,
                                      clusterConfiguration: ClusterConfiguration,
                                      filesgateConfiguration: FilesgateConfiguration,
                                      cluster: Cluster)
  extends WorkerLeader(electionService, configurationService, clusterConfiguration, cluster){

  context.system.scheduler.schedule(10 seconds, 10 seconds, self, "DEBUG")

  private val logger = LoggerFactory.getLogger(getClass)

  override def receive: Receive = {
    case message: CheckPipelineStatus =>
      logger.info("Check the pipeline status: do we have all steps to start a pipeline / stop it?")
      engine.checkEveryPipelineStatus()

    case message: StartPipelineInstances =>
      logger.info("Start the Pipeline: ask for the creation of various PipelineSteps instances.")
      engine.pipelineManagers.find(_.id == message.pipelineId) match {
        case Some(res) =>
          res.startPipelineInstances()
          res.setRunning(true)
        case None => // Should never happen
      }

    case message: StopPipelineInstances =>
      logger.info("Stop the Pipeline: ask to PipelineSteps to stop processing.")
      engine.pipelineManagers.find(_.id == message.pipelineId) match {
        case Some(res) =>
          res.stopPipelineInstances()
          res.setRunning(false)
        case None => // Should never happen
      }

    case "DEBUG" =>
      logger.debug(s"[EngineLeader] Cluster topology: \n${cluster}")
    case message =>
      logger.debug(s"[EngineLeader] Received unknown message: $message")
  }


  /**
    * Start the default actors used internally by filegates. If the name does not exist, call the developer implementation
    * to launch his tasks
    *
    * @param workerTypeId
    */
  final override def startWorker(workerTypeId: String, actorName: String): ActorRef = {
    workerTypeId match {
      case EngineActor.name =>
        context.actorOf(Props.create(classOf[EngineActor]), name = actorName)
      case PipelineManagerActor.name =>
        context.actorOf(Props.create(classOf[PipelineManagerActor]), name = actorName)
      case PipelineInstanceActor.name =>
        context.actorOf(Props.create(classOf[PipelineInstanceActor]), name = actorName)
      case _ =>
        logger.debug(s"The $workerTypeId is not known in the EngineLeader of filesgate, this is probably a WorkerType from the user.")
        startUserWorker(workerTypeId, actorName)
    }
  }

  /**
    * List of available WorkerActors given by the developer in the current jvm.
    */
  final override def allWorkerTypeInfo: List[WorkerTypeInfo] = {
    // One PipelineManager per pipeline defined by the developer
    val totalPipelines = filesgateConfiguration.pipelines.size

    // Unless the developer overrided the PipelineInstance (for example, to give a specific BalancerType), we
    // need to start the default Core.PipelineInstance in charge of handling files
    val corePipelineInstances = filesgateConfiguration.pipelines.filter(_.instanceName == PipelineInstanceActor.name).map(_.instances.get).sum

    val defaultWorkers = if(corePipelineInstances > 0) {
      List(WorkerTypeInfo(self, PipelineInstanceActor.name, BasicLoadBalancerType(instances = totalPipelines, ClusterInstance)))
    } else {
      List.empty[WorkerTypeInfo]
    }

    List(
      // The Core-EngineActor + Core-PipelineManager can only be handled by Filesgate
      WorkerTypeInfo(self, EngineActor.name, BasicLoadBalancerType(instances = 1, ClusterInstance)),
      WorkerTypeInfo(self, PipelineManagerActor.name, BasicLoadBalancerType(instances = totalPipelines, ClusterInstance)),
    ) ++ defaultWorkers ++ allUserWorkerTypeInfo
  }


  /**
    * To implement by the user
    * @return
    */
  def startUserWorker(workerTypeId: String, actorName: String): ActorRef

  /**
    * To implement by the user
    * @return
    */
  def allUserWorkerTypeInfo: List[WorkerTypeInfo]
}
