package net.degols.libs.filesgate.storage

import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.failurehandling.FailureHandlingMessage

import scala.concurrent.Future

trait StorageMetadataApi {
  def upsert(failureHandlingMessage: FailureHandlingMessage): Future[UpdateOperation]

  def upsert(fileMetadata: FileMetadata): Future[UpdateOperation]

  def get(id: String): Future[FileMetadata]

  def delete(id: String): Future[Boolean]
}
