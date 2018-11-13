package net.degols.filesgate.libs.filesgate.pipeline

import net.degols.filesgate.libs.filesgate.pipeline.prestorage.PreStorageMessage

/**
  * Contain the logic of any PipelineStep. Must be extended when we implement a PipelineStep
  */
abstract class PipelineStepService {
  def id: String

  def process(message: Any): Any
}
