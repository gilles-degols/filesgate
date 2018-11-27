package net.degols.filesgate.libs.filesgate.storage

trait StorageResult
case class SaveOperation() extends StorageResult
case class UpdateOperation() extends StorageResult
case class DeleteOperation() extends StorageResult