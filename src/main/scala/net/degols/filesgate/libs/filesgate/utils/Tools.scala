package net.degols.filesgate.libs.filesgate.utils

import java.util.concurrent.Executors
import javax.inject.Inject

import scala.concurrent.ExecutionContext

class Tools @Inject()(configurationService: FilesgateConfiguration) {
  val threadPool = Executors.newFixedThreadPool(10)
  implicit val executionContext =  ExecutionContext.fromExecutor(threadPool)
}
