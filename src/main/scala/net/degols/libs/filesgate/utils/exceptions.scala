package net.degols.libs.filesgate.utils

class UnknownPipelineStep(val message: String) extends Exception(message)
class FileNotDownloaded(val message: String) extends Exception(message)
class FileNotFound(val message: String) extends Exception(message)
class MetadataNotFound(val message: String) extends Exception(message)
class FailedDatabaseOperation(val message: String) extends Exception(message)
class MissingStepActor(val message: String) extends Exception(message)

