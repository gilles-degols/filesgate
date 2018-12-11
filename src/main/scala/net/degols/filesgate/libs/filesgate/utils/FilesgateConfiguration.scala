package net.degols.filesgate.libs.filesgate.utils

import java.io.File

import com.google.inject.Inject
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import net.degols.filesgate.libs.cluster.{Tools => ClusterTools}
import net.degols.filesgate.libs.cluster.messages.{Communication, LoadBalancerType}
import net.degols.filesgate.libs.filesgate.core.EngineLeader
import org.slf4j.LoggerFactory
import javax.inject.Singleton
import net.degols.filesgate.libs.filesgate.pipeline.download.Download
import net.degols.filesgate.libs.filesgate.pipeline.matcher.Matcher
import net.degols.filesgate.libs.filesgate.pipeline.poststorage.PostStorage
import net.degols.filesgate.libs.filesgate.pipeline.predownload.PreDownload
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.PreStorage
import net.degols.filesgate.libs.filesgate.pipeline.source.Source
import net.degols.filesgate.libs.filesgate.pipeline.storage.Storage

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
  override def toString: String = s"Step($tpe, $name, $loadBalancerType)"
}
case class PipelineMetadata(id: String, steps: List[Step], instances: Int)

/**
  * Created by Gilles.Degols on 03-09-18.
  */
@Singleton
class FilesgateConfiguration @Inject()(val defaultConfig: Config) {
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
                              Step(tpe, name, loadBalancerType)
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
    // The default steps that we might have. Only the "source" is mandatory and should be implemented by the developer (unless a mapping is given).
    // The "download" is a mandatory step, but does not need to be provided by the user.
    val defaultStepTypes = Map(
      Source.TYPE      -> true, // Mandatory for the developer
      Matcher.TYPE     -> false,
      PreDownload.TYPE -> false,
      Download.TYPE    -> true, // Mandatory for the internal working
      PreStorage.TYPE  -> false,
      Storage.TYPE     -> false,
      PostStorage.TYPE -> false
    )

    // Complete the steps and directly order them correctly
    val completeSteps: List[Step] = defaultStepTypes.flatMap {
      case (stepType, mandatory) => {
        val matchingSteps = userSteps.filter(_.tpe == stepType)
        if (matchingSteps.size > 1) {
          logger.error(s"More than one step for ${pipelineId}: ${stepType}. This is not yet supported. Abort.")
          throw new Exception("Invalid pipeline configuration")
          None
        } else if (matchingSteps.nonEmpty) {
          matchingSteps.headOption
        } else if(mandatory) {
          logger.error(s"Missing mandatory step for ${pipelineId}: $stepType. Abort.")
          throw new Exception("Invalid pipeline configuration")
          None
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