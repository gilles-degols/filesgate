package net.degols.filesgate.libs.filesgate.core.pipelineinstance

import akka.actor.Actor

/**
  * Handle one instance of a given pipeline
  */
class PipelineInstanceActor extends Actor{
  override def receive: Receive = ???
}

object PipelineInstanceActor {
  val name: String = "Core.PipelineInstanceActor"
}
