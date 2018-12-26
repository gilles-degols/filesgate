package net.degols.libs.filesgate.storage

import net.degols.libs.filesgate.orm.FileMetadata

import scala.concurrent.Future
import scala.util.Try

trait StorageMetadataApi {
  def upsert(fileMetadata: FileMetadata): Future[UpdateOperation]

  def get(id: String): Future[FileMetadata]

  def delete(id: String): Future[Boolean]
}
