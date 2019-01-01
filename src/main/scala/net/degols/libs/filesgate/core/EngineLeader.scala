package net.degols.libs.filesgate.core

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.{ActorContext, ActorRef, ActorSystem, Props}
import javax.inject.{Inject, Singleton}
import net.degols.libs.cluster.ClusterConfiguration
import net.degols.libs.cluster.core.Cluster
import net.degols.libs.cluster.manager.WorkerLeader
import net.degols.libs.cluster.messages._
import net.degols.libs.election.{ConfigurationService, ElectionService}
import net.degols.libs.filesgate.core.engine.{Engine, EngineActor}
import net.degols.libs.filesgate.core.pipelineinstance.PipelineInstanceActor
import net.degols.libs.filesgate.core.pipelinemanager.PipelineManagerActor
import net.degols.libs.filesgate.pipeline.download.Download
import net.degols.libs.filesgate.pipeline.failurehandling.FailureHandling
import net.degols.libs.filesgate.pipeline.metadata.Metadata
import net.degols.libs.filesgate.pipeline.storage.Storage
import net.degols.libs.filesgate.pipeline.{PipelineStepActor, PipelineStepService}
import net.degols.libs.filesgate.storage.systems.mongo.{MongoContent, MongoMetadata}
import net.degols.libs.filesgate.storage.{StorageContentApi, StorageMetadataApi}
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, PipelineMetadata, Step, Tools}
import org.slf4j.LoggerFactory

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
                                      tools: Tools,
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
    val relatedStepAndPipeline = findStepForWorkerTypeId(workerTypeId)

    // The name for a pipeline step is ${pipelineId}.${pipelineNumberId}.${stepName} even if $stepName is "Core.Metadata"
    relatedStepAndPipeline match {
      case Some(stepAndPipeline) =>
        val pipeline = stepAndPipeline._1
        val step = stepAndPipeline._2
        val rawStep = step.name.split("\\:").last.split("\\.", 3)

        rawStep.last match {
          case Download.DEFAULT_STEP_NAME =>
            // We must create the default Download service
            val service: PipelineStepService = new Download(tools)
            context.actorOf(Props.create(classOf[PipelineStepActor], ec, service).withMailbox("priority-stashed-actor"))
          case Storage.DEFAULT_STEP_NAME =>
            // We must create the default Storage actor + selected service (for now, only MongoDB, but afterwards another one can be created)
            val dbService: StorageContentApi = instantiateStorageContentService(step.dbServiceName.get)
            val service: PipelineStepService = new Storage(dbService)
            context.actorOf(Props.create(classOf[PipelineStepActor], ec, service).withMailbox("priority-stashed-actor"))
          case Metadata.DEFAULT_STEP_NAME =>
            // We must create the default Metadata actor + selected service (for now, only MongoDB, but afterwards another one can be created)
            val dbService: StorageMetadataApi = instantiateStorageMetadataService(step.dbServiceName.get)
            val service: PipelineStepService = new Metadata(dbService)
            context.actorOf(Props.create(classOf[PipelineStepActor], ec, service).withMailbox("priority-stashed-actor"))
          case FailureHandling.DEFAULT_STEP_NAME =>
            // We must create the default FailureHandling actor + selected service (for now, only MongoDB, but afterwards another one can be created)
            // Note that we use the same service as the one for the metadata, there is no reason to have a different one
            val dbService: StorageMetadataApi = instantiateStorageMetadataService(step.dbServiceName.get)
            val service: PipelineStepService = new FailureHandling(dbService)
            context.actorOf(Props.create(classOf[PipelineStepActor], ec, service).withMailbox("priority-stashed-actor"))
          case x =>
            // We try to find if the workertypeid is linked to a PipelineStep
            if(pipelineWorkerTypeInfo.exists(_.workerTypeId == workerTypeId)) {
              instantiatePipelineStep(workerTypeId, actorName)
            } else {
              throw new Exception(s"We could not find the related WorkerTypeInfo for $workerTypeId...")
            }
        }
      case None =>
        // This is a more classical pipeline step
        workerTypeId match {
          case EngineActor.name =>
            context.actorOf(Props.create(classOf[EngineActor], engine, filesgateConfiguration), name = actorName)
          case PipelineManagerActor.NAME =>
            context.actorOf(Props.create(classOf[PipelineManagerActor], filesgateConfiguration), name = actorName)
          case PipelineInstanceActor.NAME =>
            context.actorOf(Props.create(classOf[PipelineInstanceActor], filesgateConfiguration), name = actorName)
          case x =>
            logger.debug(s"The $workerTypeId is not known in the EngineLeader of filesgate, this is probably a WorkerType from the user.")
            startUserWorker(workerTypeId, actorName)
        }
    }
  }

  /**
    * Find the Step for a given WorkerTypeId. Necessary to get the appropriate service for a metadata, storage, ... step
    */
  def findStepForWorkerTypeId(workerTypeId: String): Option[(PipelineMetadata, Step)] = {
    // The workerTypeId we received contains the pipeline instance number, so we need to remove it
    val fullStepName = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, workerTypeId)

    filesgateConfiguration.pipelines.map(pipeline => {
      (pipeline, pipeline.pipelineInstancesMetadata.flatMap(_.steps.find(_.name == fullStepName)).headOption)
    }).filter(_._2.isDefined).map(x => (x._1, x._2.get)).headOption
  }

  /**
    * Instantiate a PipelineInstanceStep with the related class
    */
  final def instantiatePipelineStep(workerTypeId: String, actorName: String): ActorRef = {
    // We need to remove the pipeline instance number in the workerTypeId before giving it to the developer
    var i = 0
    val elems = workerTypeId.split("\\:").last.split("\\.").toList
    val pipelineId = elems.head
    val prettyStepName = pipelineId + "." + elems.drop(2).mkString(".")

    val service: PipelineStepService = startStepService(prettyStepName)
    context.actorOf(Props.create(classOf[PipelineStepActor], ec, service).withMailbox("priority-stashed-actor"))
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
      pipelineMetadata.pipelineInstancesMetadata.flatMap(met => {
        met.steps.map(step => {
          // The step name is formatted that way: "Component:Package:PipelineId.InstanceId.StepName" and we need to remove the two first parts to only have a proper local name
          val stepName: String = step.name.split(":", 3).drop(2).last
          WorkerTypeInfo(self, s"$stepName", step.loadBalancerType)
        })
      })
    })
  }

  /**
    * Start any storage content service
    */
  final def instantiateStorageContentService(dbService: String): StorageContentApi = {
    dbService match {
      case "Core.MongoContent" => new MongoContent(filesgateConfiguration, tools)
      case other => startStorageContentService(dbService)
    }
  }

  /**
    * Start any storage metadata service
    */
  final def instantiateStorageMetadataService(dbService: String): StorageMetadataApi = {
    dbService match {
      case "Core.MongoMetadata" => new MongoMetadata(filesgateConfiguration, tools)
      case other => startStorageMetadataService(dbService)
    }
  }

  /**
    * Can be implemented by the developer, to instantiate a specific db service
    */
  def startStorageContentService(dbService: String): StorageContentApi = ???

  /**
    * Can be implemented by the developer, to instantiate a specific db service
    */
  def startStorageMetadataService(dbService: String): StorageMetadataApi = ???

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