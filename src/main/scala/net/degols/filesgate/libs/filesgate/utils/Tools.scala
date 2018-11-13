package net.degols.filesgate.service

import java.util.concurrent.Executors
import javax.inject.Inject

import scala.concurrent.ExecutionContext

class Tools @Inject()(configurationService: ConfigurationService) {
  val threadPool = Executors.newFixedThreadPool(10)
  implicit val executionContext =  ExecutionContext.fromExecutor(threadPool)
}
