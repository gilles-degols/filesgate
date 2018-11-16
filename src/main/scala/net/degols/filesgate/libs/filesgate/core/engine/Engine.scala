package net.degols.filesgate.libs.filesgate.core.engine

import javax.inject.{Inject, Singleton}

import akka.actor.ActorContext
import net.degols.filesgate.libs.filesgate.core.pipelinemanager.PipelineManager
import net.degols.filesgate.libs.filesgate.core.{StartPipelineInstances, StopPipelineInstances}
import net.degols.filesgate.libs.filesgate.utils.FilesgateConfiguration
import org.slf4j.{Logger, LoggerFactory}

@Singleton
class Engine @Inject()(filesgateConfiguration: FilesgateConfiguration) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Set by the EngineActor
  var context: ActorContext = _

  /**
    * Must remain a lazy val to get the context.
    */
  lazy val pipelineManagers: List[PipelineManager] = configurationService.pipelines.map(metadata => {
    val pipelineManager = new PipelineManager(metadata, configurationService, tools, cluster)
    pipelineManager.context = context
    pipelineManager
  })

  /**
    * Check status of a PipelineManager. Start or stop it depending of the PipelineSteps that we have.
    */
  def checkEveryPipelineStatus(): Unit = {
    pipelineManagers.foreach(manager => {
      if(manager.hasAllPipelineSteps && !manager.running) {
        logger.info(s"Pipeline $manager has all the steps, will ask to start the engine.")
        context.self ! StartPipelineInstances(manager.id)
      } else if(!manager.hasAllPipelineSteps && manager.running) {
        logger.info(s"Pipeline $manager has not all the steps anymore, will ask to start the engine.")
        context.self ! StopPipelineInstances(manager.id)
      }
    })
  }
}
