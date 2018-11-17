package net.degols.filesgate.libs.filesgate.utils

import java.io.File
import java.util
import java.util.Map

import com.google.inject.Inject
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import net.degols.filesgate.libs.cluster.Tools
import net.degols.filesgate.libs.cluster.messages.Communication
import net.degols.filesgate.libs.filesgate.core.EngineLeader
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

/**
  *
  * @param tpe
  * @param name full name to the actor
  * @param maxInstances
  */
case class Step(tpe: String, name: String, maxInstances: Int)
case class PipelineMetadata(id: String, steps: List[Step], instances: Int)

/**
  * Created by Gilles.Degols on 03-09-18.
  */
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
  val config: Config = defaultConfig.withFallback(projectConfig).withFallback(fallbackConfig)

  /**
    * Configuration for the cluster system. We merge multiple configuration files: One embedded, the other one from the project
    * using the cluster library
    */
  val clusterConfig: Config = config.getConfig("cluster")

  lazy val localHostname: String = Tools.runCommand("hostname")

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
  val checkPipelineStepState: FiniteDuration = config.getInt("filesgate.internal.engine-actor.check-pipeline-instance-step-ms") millis

  /**
    * The various pipelines defined in the configuration.
    * This must remain a lazy val as we don't have the EngineLeader.component / EngineLeader.package at boot
    */
  lazy val pipelines: List[PipelineMetadata] = {
    val set: util.Set[util.Map.Entry[String, ConfigValue]] = config.getConfig("filesgate.pipeline").entrySet()
    val res = toMap(set) map {
      case (key, value) =>
        val id = key
        // TODO: Add arbitrary steps (download, storage, ...) between the override from the developer
        val steps = value.asInstanceOf[Config].getObjectList("step-ids").iterator().asScala
                            .map(rawStep => {
                              val tpe = rawStep.get("type").asInstanceOf[String]
                              val rawName = rawStep.get("name").asInstanceOf[String]
                              // We might want to add the current Component/Package
                              val name = if(!rawName.contains(":")) {
                                if(EngineLeader.COMPONENT == null || EngineLeader.PACKAGE == null) {
                                  throw new Exception("EngineLeader has not yet its COMPONENT or PACKAGE.")
                                }
                                Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, rawName)
                              } else { // We assume we gave a full path directly (maybe to another jvm)
                                rawName
                              }

                              val maxInstances = Try{rawStep.get("max-instances").asInstanceOf[Int]}.getOrElse(-1)
                              Step(tpe, name, maxInstances)
                            }).toList
        val instances = value.asInstanceOf[Config].getInt("pipeline-instance.quantity")

        PipelineMetadata(id, steps, instances)
    }

    res.toList
  }

  /**
    * Methods to get data from the embedded configuration, or the project configuration (it can override it)
    */
  private def getStringList(path: String): List[String] = {
    config.getStringList(path).asScala.toList
  }
}