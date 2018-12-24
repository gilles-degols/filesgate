package net.degols.libs.filesgate.core

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.{Actor, ActorRef, Kill, Props}
import net.degols.libs.election.{ConfigurationService, ElectionService, ElectionWrapper}
import org.slf4j.LoggerFactory
import javax.inject.{Inject, Singleton}

import scala.concurrent.duration._
import net.degols.libs.cluster.{ClusterConfiguration, Tools}
import net.degols.libs.cluster.core.Cluster
import net.degols.libs.cluster.manager.{Manager, WorkerLeader}
import net.degols.libs.cluster.messages._
import net.degols.libs.filesgate.core.engine.{Engine, EngineActor}
import net.degols.libs.filesgate.core.pipelineinstance.{PipelineInstance, PipelineInstanceActor}
import net.degols.libs.filesgate.core.pipelinemanager.{PipelineManager, PipelineManagerActor}
import net.degols.libs.filesgate.pipeline.{PipelineStepActor, PipelineStepService}
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, Tools}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

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

  val threadPool: ExecutorService = Executors.newFixedThreadPool(20)
  implicit val ec: ExecutionContextExecutor =  ExecutionContext.fromExecutor(threadPool)

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
      case PipelineManagerActor.NAME =>
        context.actorOf(Props.create(classOf[PipelineManagerActor], filesgateConfiguration), name = actorName)
      case PipelineInstanceActor.NAME =>
        context.actorOf(Props.create(classOf[PipelineInstanceActor], filesgateConfiguration), name = actorName)
      case x =>
        // We try to find if the workertypeid is linked to a PipelineStep
        if(pipelineWorkerTypeInfo.exists(_.workerTypeId == workerTypeId)) {
          instantiatePipelineStep(workerTypeId, actorName)
        } else {
          logger.debug(s"The $workerTypeId is not known in the EngineLeader of filesgate, this is probably a WorkerType from the user.")
          startUserWorker(workerTypeId, actorName)
        }
    }
  }

  /**
    * Instantiate a PipelineInstanceStep with the related class
    */
  final def instantiatePipelineStep(workerTypeId: String, actorName: String): ActorRef = {
    val service: PipelineStepService = startStepService(workerTypeId)
    context.actorOf(Props.create(classOf[PipelineStepActor], ec, service))
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
      WorkerTypeInfo(self, PipelineInstanceActor.NAME, BasicLoadBalancerType(instances = totalPipelineInstances, ClusterInstance))
    )


    // The final list of workers to start
    List(
      // The Core-EngineActor + Core-PipelineManager can only be handled by Filesgate, and they are only linked to the current COMPONENT et PACKAGE
      WorkerTypeInfo(self, EngineActor.name, BasicLoadBalancerType(instances = 1, ClusterInstance)),
      WorkerTypeInfo(self, PipelineManagerActor.NAME, BasicLoadBalancerType(instances = totalPipelines, ClusterInstance))
    ) ++ defaultWorkers ++ pipelineWorkerTypeInfo ++ allUserWorkerTypeInfo
  }

  /**
    * Create the WorkerTypeInfo for the user pipelines and their steps.
    */
  final lazy val pipelineWorkerTypeInfo: List[WorkerTypeInfo] = {
    filesgateConfiguration.pipelines.flatMap(pipelineMetadata => {
      pipelineMetadata.steps.map(step => {
        // The step name is formatted that way: "Component:Package:PipelineId.StepName" and we need to remove the two first parts to only have a proper local name
        val workerTypeId: String = step.name.split(":").drop(2).mkString(":")
        WorkerTypeInfo(self, workerTypeId, step.loadBalancerType)
      })
    })
  }

  /**
    * Must be implemented by the developer, for the pipeline step
    */
  def startStepService(stepName: String): PipelineStepService

  /**
    * Can be implemented by the user, not mandatory
    * @return
    */
  def startUserWorker(workerTypeId: String, actorName: String): ActorRef = ???

  /**
    * Can be implemented by the user, not mandatory
    * @return
    */
  def allUserWorkerTypeInfo: List[WorkerTypeInfo] = List.empty[WorkerTypeInfo]
}

object EngineLeader {
  // Those values are set by the EngineLeader actor at boot, there is no risk of concurrency problems
  var COMPONENT: String = _
  var PACKAGE: String = _
}