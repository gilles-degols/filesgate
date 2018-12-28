package net.degols.libs.filesgate.utils

import java.io.File

import com.google.inject.Inject
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import javax.inject.Singleton
import net.degols.libs.cluster.messages.{Communication, LoadBalancerType}
import net.degols.libs.cluster.{Tools => ClusterTools}
import net.degols.libs.filesgate.core.EngineLeader
import net.degols.libs.filesgate.pipeline.PipelineStep
import net.degols.libs.filesgate.pipeline.datasource.DataSource
import net.degols.libs.filesgate.pipeline.download.Download
import net.degols.libs.filesgate.pipeline.failurehandling.FailureHandling
import net.degols.libs.filesgate.pipeline.matcher.Matcher
import net.degols.libs.filesgate.pipeline.metadata.Metadata
import net.degols.libs.filesgate.pipeline.postmetadata.PostMetadata
import net.degols.libs.filesgate.pipeline.predownload.PreDownload
import net.degols.libs.filesgate.pipeline.premetadata.PreMetadata
import net.degols.libs.filesgate.pipeline.prestorage.PreStorage
import net.degols.libs.filesgate.pipeline.storage.Storage
import org.slf4j.LoggerFactory
import play.api.libs.concurrent.Futures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

/**
  *
  * @param tpe
  * @param name full name to the actor
  * @param maxInstances
  */
case class Step(tpe: String, name: String, loadBalancerType: LoadBalancerType) {
  /**
    * Maximum timeout for the step processing. This value is overrided by FilesgateConfiguration in any case.
    */
  var processingTimeout: Duration = 1000 millis

  override def toString: String = s"Step($tpe, $name, $loadBalancerType)"
}
case class PipelineMetadata(id: String, steps: List[Step], instances: Int)

/**
  * Play futures are used a bit everywhere in the application, so it's easier to provide it here.
  */
@Singleton
class FilesgateConfiguration @Inject()(val defaultConfig: Config)(implicit val futures: Futures) {
  private val logger = LoggerFactory.getLogger(getClass)
  /**
    * If the library is loaded directly as a subproject, the Config of the subproject overrides the configuration of the main
    * project by default, and we want the opposite.
    */
  private lazy val projectConfig: Config = {
    val projectFile = new File(pathToProjectFile)
    ConfigFactory.load(ConfigFactory.parseFile(projectFile))
  }

  private val pathToProjectFile: String = {
    Try{ConfigFactory.systemProperties().getString("config.resource")}.getOrElse("conf/application.conf")
  }

  private lazy val fallbackConfig: Config = {
    val fileInSubproject = new File("../cluster/src/main/resources/application.conf")
    val fileInProject = new File("main/resources/application.conf")
    if (fileInSubproject.exists()) {
      ConfigFactory.load(ConfigFactory.parseFile(fileInSubproject))
    } else {
      ConfigFactory.load(ConfigFactory.parseFile(fileInProject))
    }
  }
  val config: Config = projectConfig.withFallback(defaultConfig).withFallback(fallbackConfig)

  /**
    * Configuration for the cluster system. We merge multiple configuration files: One embedded, the other one from the project
    * using the cluster library
    */
  val clusterConfig: Config = config.getConfig("cluster")

  lazy val localHostname: String = ClusterTools.runCommand("hostname")

  /**
    * A watcher might not receive any message back from an ElectionActor directly. Or we want to allow a nice switch
    * of ElectionManagers without killing all Workers directly. During that time, we can have duplicate work, so the value
    * should be correctly chosen. 1 minute should be more than enough.
    */
  val watcherTimeoutBeforeSuicide: FiniteDuration = config.getInt("cluster.watcher-timeout-before-suicide-ms") millis

  /**
    * A Soft Distribution message is sent frequently to start missing actors.
    */
  val softWorkDistributionFrequency: FiniteDuration = config.getInt("cluster.soft-work-distribution-ms") millis

  /**
    * A Hard Distribution message is sent from time to time to stop existing actors, and start them elsewhere.
    */
  val hardWorkDistributionFrequency: FiniteDuration = config.getInt("cluster.hard-work-distribution-ms") millis

  /**
    * How much time do we allow to start a WorkerOrder before considering as failing?
    */
  val startWorkerTimeout: FiniteDuration = config.getInt("cluster.start-worker-timeout-ms") millis

  /**
    * Is the default metadata storage activated?
    */
  val isStoreMetadataActivated: Boolean = config.getBoolean("filesgate.storage.metadata.activate")

  /**
    * Is the default failureHandling activated (in metadata)?
    */
  val isStoreFailureActivated: Boolean = config.getBoolean("filesgate.storage.failurehandling.activate")

  /**
    * Is the default content storage activated ?
    */
  val isStoreContentActivated: Boolean = config.getBoolean("filesgate.storage.content.activate")

  /**
    * Is the default download activated ?
    */
  val isDownloadActivated: Boolean = config.getBoolean("filesgate.download.activate")

  /**
    * Default timeout between every pipeline step (it can be overrided step-per-step)
    */
  val timeoutBetweenPipelineSteps: Duration = config.getLong("filesgate.internal.pipeline.timeout-step-ms") millis

  /**
    * It's difficult to get a remote actor path locally. Because of that, we still want to know the current hostname + port
    */
  val akkaLocalHostname: String = config.getString("akka.remote.netty.tcp.hostname")
  val akkaLocalPort: Int = config.getInt("akka.remote.netty.tcp.port")

  val akkaClusterRemoteHostname: String = config.getString("cluster.akka.remote.netty.tcp.hostname")
  val akkaClusterRemotePort: Int = config.getInt("cluster.akka.remote.netty.tcp.port")

  private def toMap(hashMap: AnyRef): Predef.Map[String, AnyRef] = hashMap.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap
  private def toList(list: AnyRef): List[AnyRef] = list.asInstanceOf[java.util.List[AnyRef]].asScala.toList

  /**
    * How often do we check for the state of PipelineManagers (and start them when needed) in the EngineActor ?
    */
  val checkPipelineManagerState: FiniteDuration = config.getInt("filesgate.internal.engine-actor.check-pipeline-manager-state-ms") millis

  /**
    * How often do we check for the state of PipelineInstances (and start them when needed) in every PipelineManagerActor ?
    */
  val checkPipelineInstanceState: FiniteDuration = config.getInt("filesgate.internal.engine-actor.check-pipeline-instance-state-ms") millis

  /**
    * How often do we check for the state of PipelineSteps (and start them when needed) in every PipelineInstanceActor ?
    */
  val checkPipelineStepState: FiniteDuration = config.getInt("filesgate.internal.engine-actor.check-pipeline-step-state-ms") millis


  /**
    * The various pipelines defined in the configuration.
    * This must remain a lazy val as we don't have the EngineLeader.component / EngineLeader.package at boot
    */
  lazy val pipelines: List[PipelineMetadata] = {
    if(EngineLeader.COMPONENT == null || EngineLeader.PACKAGE == null) {
      throw new Exception("EngineLeader has not yet its COMPONENT or PACKAGE.")
    }

    val set: List[(String, ConfigValue)] = config.getObject("filesgate.pipelines").asScala.toList
    val res = set map {
      case (key, value) =>
        val id = key
        val currentPipeline = value.asInstanceOf[ConfigObject].toConfig

        val steps: List[Step] = currentPipeline.getConfigList("steps").asScala.toList
                            .map(rawStep => {
                              val tpe = rawStep.getString("type")
                              val rawName = rawStep.getString("name")
                              // We might want to add the current Component/Package & the pipeline id in the step name
                              val rawNameWithPipeline: String = if(rawName.startsWith(id+".")) rawName else s"$id.$rawName"
                              val name = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, rawNameWithPipeline)

                              val loadBalancerType: LoadBalancerType = LoadBalancerType.loadFromConfig(rawStep.getConfig("balancer"))
                              val step = Step(tpe, name, loadBalancerType)
                              step.processingTimeout = Try{rawStep.getLong("timeout-step-ms") millis}.getOrElse(timeoutBetweenPipelineSteps)
                              step
                            })

        // We add missing pipeline steps between the one defined by the user, and order them correctly
        val allSteps = completePipelineSteps(id, steps)

        val instances = currentPipeline.getInt("pipeline-instance.quantity")

        logger.info(s"PipelineStep '$id' ($instances): $allSteps")
        PipelineMetadata(id, allSteps, instances)
    }

    res
  }


  /**
    * Add missing (mandatory) pipeline steps between the ones defined by the user. If they weren't overrided.
    * We take care of having the correct order at the end.
    */
  private def completePipelineSteps(pipelineId: String, userSteps: List[Step]): List[Step] = {

    // Complete the steps and directly order them correctly
    val completeSteps: List[Step] = FilesgateConfiguration.PIPELINE_STEP_TYPES.flatMap {
      stepType => {
        val matchingSteps = userSteps.filter(_.tpe == stepType.TYPE)
        if (matchingSteps.size > 1) {
          logger.error(s"More than one step for ${pipelineId}: ${stepType.TYPE}. This is not yet supported. Abort.")
          throw new Exception("Invalid pipeline configuration")
          None
        } else if (matchingSteps.nonEmpty) {
          matchingSteps.headOption
        } else if(stepType.MANDATORY) {
          val defaultStep = if(stepType.TYPE == Download.TYPE && Download.defaultStep.isDefined) {
            logger.debug("No specific step for the download phase, use the default one.")
            if(isDownloadActivated) Download.defaultStep
            else None
          } else if(stepType.TYPE == Storage.TYPE && Storage.defaultStep.isDefined) {
            logger.debug("No specific step for the storage phase, use the default one.")
            if(isStoreContentActivated) Storage.defaultStep
            else None
          } else if(stepType.TYPE == Metadata.TYPE && Metadata.defaultStep.isDefined) {
            logger.debug("No specific step for the metadata phase, use the default one.")
            if(isStoreMetadataActivated) Metadata.defaultStep
            else None
          } else if(stepType.TYPE == FailureHandling.TYPE && FailureHandling.defaultStep.isDefined) {
            logger.debug("No specific step for the failure handling phase, use the default one.")
            if(isStoreFailureActivated) FailureHandling.defaultStep
            else None
          } else {
            logger.error(s"Missing mandatory step for ${pipelineId}: ${stepType.TYPE}. Abort.")
            throw new Exception("Invalid pipeline configuration.")
            None
          }

          // We add some additional info in the default step
          if(defaultStep.isDefined) {
            defaultStep.get.processingTimeout = timeoutBetweenPipelineSteps
          }

          defaultStep
        } else { // Normal behavior, nothing to add
          None
        }
      }
    }.toList

    completeSteps
  }

  /**
    * Methods to get data from the embedded configuration, or the project configuration (it can override it)
    */
  private def getStringList(path: String): List[String] = {
    config.getStringList(path).asScala.toList
  }
}

object FilesgateConfiguration {

  /**
    * Return a list of the different pipeline stages
    * As value, we have a boolean saying if the related step is mandatory
    *
    * Only the "source" is mandatory and should be implemented by the developer (unless a mapping is given).
    * The "download" is a mandatory step, but does not need to be provided by the user.
    */
  val PIPELINE_STEP_TYPES: List[PipelineStep] = List(DataSource,
                                                      Matcher,
                                                      PreDownload,
                                                      Download,
                                                      PreStorage,
                                                      Storage,
                                                      PreMetadata,
                                                      Metadata,
                                                      PostMetadata,
                                                      FailureHandling)
}