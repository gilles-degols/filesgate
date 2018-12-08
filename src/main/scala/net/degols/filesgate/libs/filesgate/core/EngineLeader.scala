package net.degols.filesgate.libs.filesgate.core

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
import net.degols.filesgate.libs.filesgate.utils.FilesgateConfiguration

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

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Start the default actors used internally by filegates. If the name does not exist, call the developer implementation
    * to launch his tasks
    *
    * @param workerTypeId
    */
  final override def startWorker(workerTypeId: String, actorName: String): ActorRef = {
    workerTypeId match {
      case EngineActor.name =>
        context.actorOf(Props.create(classOf[EngineActor], engine, filesgateConfiguration), name = actorName)
      case PipelineManagerActor.name =>
        context.actorOf(Props.create(classOf[PipelineManagerActor], filesgateConfiguration), name = actorName)
      case PipelineInstanceActor.name =>
        context.actorOf(Props.create(classOf[PipelineInstanceActor], filesgateConfiguration), name = actorName)
      case x =>
        logger.debug(s"The $workerTypeId is not known in the EngineLeader of filesgate, this is probably a WorkerType from the user.")
        startUserWorker(workerTypeId, actorName)
    }
  }

  /**
    * List of available WorkerActors given by the developer in the current jvm.
    */
  final override def allWorkerTypeInfo: List[WorkerTypeInfo] = {
    // Set the component values to easily use them
    EngineLeader.COMPONENT = COMPONENT
    EngineLeader.PACKAGE = PACKAGE

    // One PipelineManager per pipeline defined by the developer
    val totalPipelines = filesgateConfiguration.pipelines.size

    // For now we have only one type of PipelineInstance with one balancer as it eases the work a lot. But in the future
    // we should allow customization
    val totalPipelineInstances = filesgateConfiguration.pipelines.map(_.instances).sum
    val defaultWorkers = List(
      WorkerTypeInfo(self, PipelineInstanceActor.name, BasicLoadBalancerType(instances = totalPipelineInstances, ClusterInstance))
    )

    // The final list of workers to start
    List(
      // The Core-EngineActor + Core-PipelineManager can only be handled by Filesgate, and they are only linked to the current COMPONENT et PACKAGE
      WorkerTypeInfo(self, EngineActor.name, BasicLoadBalancerType(instances = 1, ClusterInstance)),
      WorkerTypeInfo(self, PipelineManagerActor.name, BasicLoadBalancerType(instances = totalPipelines, ClusterInstance))
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

object EngineLeader {
  // Those values are set by the EngineLeader actor at boot, there is no risk of concurrency problems
  var COMPONENT: String = _
  var PACKAGE: String = _
}