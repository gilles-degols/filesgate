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
case class Step(tpe: String, name: String, loadBalancerType: LoadBalancerType, private val config: Config) {
  /**
    * Maximum timeout for the step processing. This value is overrided by FilesgateConfiguration in any case.
    */
  var processingTimeout: Duration = 1000 millis

  /**
    * Optional db service name to use for the step. It can be used for the Storage
    * @return
    */
  val dbServiceName: Option[String] = Try{Option(config.getString("db-service"))}.getOrElse(None)

  /**
    * Optional priority for step, only used for the source as of now.
    */
  val priority: Int = Try{config.getInt("priority")}.getOrElse(1)

  /**
    * This information is only useful for the DownloadStep as of now. If the directory is given, we store
    * downloaded files in the given repository. They must be manually cleaned by the Storage step.
    */
  val directory: Option[String] =  Try{Option(config.getString("directory"))}.getOrElse(None)

  override def toString: String = s"Step($tpe, $name, $loadBalancerType)"
}

case class PipelineInstanceMetadata(numberId: Int, steps: List[Step])

case class PipelineMetadata(id: String, pipelineInstancesMetadata: List[PipelineInstanceMetadata], private val config: Config) {
  // Max number of pipeline instances (this object is just one instance of them)
  val instances: Int = Try{config.getInt("pipeline-instance.quantity")}.getOrElse(1)

  // Should we restart the pipeline if the stream finish successfully ?
  val restartWhenFinished: Boolean = Try{config.getBoolean("restart-when-finished")}.getOrElse(false)
}

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
    * This must remain a lazy val as we don't have the EngineLeader.component / EngineLeader.package at boot.
    * We have multiple PipelineMetadata for each instance of a Pipeline, to ease the manipulation
    */
  lazy val pipelines: List[PipelineMetadata] = {
    if(EngineLeader.COMPONENT == null || EngineLeader.PACKAGE == null) {
      throw new Exception("EngineLeader has not yet its COMPONENT or PACKAGE.")
    }

    val set: List[(String, ConfigValue)] = config.getObject("filesgate.pipelines").asScala.toList
    val res = set.map{
      case (key, value) =>
        val id = key
        val currentPipeline: Config = value.asInstanceOf[ConfigObject].toConfig
        val quantity = Try{currentPipeline.getInt("pipeline-instance.quantity")}.getOrElse(1)

        val instancesMetadata = (0 until quantity).map(instanceNumber => {
          val steps: List[Step] = currentPipeline.getConfigList("steps").asScala.toList
            .map(rawStep => {
              val tpe = rawStep.getString("type")
              val rawName = rawStep.getString("name")
              // We might want to add the current Component/Package & the pipeline id in the step name
              val rawNameWithPipeline: String = if(rawName.startsWith(id+".")) {
                val n = rawName.split(".").drop(1)
                s"$id.$instanceNumber.$n"
              } else {
                s"$id.$instanceNumber.$rawName"
              }

              val name = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, rawNameWithPipeline)

              val loadBalancerType: LoadBalancerType = LoadBalancerType.loadFromConfig(rawStep.getConfig("balancer"))
              val step = Step(tpe, name, loadBalancerType, rawStep)
              step.processingTimeout = Try{rawStep.getLong("timeout-step-ms") millis}.getOrElse(timeoutBetweenPipelineSteps)

              // Check to be sure that the user set the appropriate config
              if(step.dbServiceName.isEmpty && (step.tpe == Metadata.TYPE || step.tpe == Storage.TYPE || step.tpe == FailureHandling.TYPE)) {
                throw new Exception(s"Missing db-service information for step $rawNameWithPipeline.")
              }

              step
            })

          // We add missing pipeline steps between the one defined by the user, and order them correctly
          val allSteps = completePipelineSteps(id, steps)

          logger.info(s"PipelineStep '$id': $allSteps")
          PipelineInstanceMetadata(instanceNumber, steps)
        }).toList

        PipelineMetadata(id, instancesMetadata, currentPipeline)
    }

    res
  }


  /**
    * Add missing (mandatory) pipeline steps between the ones defined by the user. If they weren't overrided.
    * We take care of having the correct order at the end.
    */
  private def completePipelineSteps(pipelineId: String, userSteps: List[Step]): List[Step] = {

    // Complete the steps and directly order them correctly
    // Important note: you need to be sure to keep the config order of the pipeline steps for a specific step
    val completeSteps: List[Step] = FilesgateConfiguration.PIPELINE_STEP_TYPES.flatMap {
      stepType => {
        val matchingSteps = userSteps.filter(_.tpe == stepType.TYPE)
        if (matchingSteps.nonEmpty) {
          Option(matchingSteps)
        } else if(stepType.IMPORTANT_STEP) {
          logger.warn(s"No specific step for the ${stepType.TYPE} phase, you must add it manually or use a default one.")
          None
        } else { // Normal behavior, nothing to add
          None
        }
      }
    }.flatten

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