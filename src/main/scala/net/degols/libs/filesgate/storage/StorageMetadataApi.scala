package net.degols.libs.filesgate.storage

import net.degols.libs.filesgate.orm.FileMetadata

import scala.concurrent.Future
import scala.util.Try

trait StorageMetadataApi {
  def update(fileMetadata: FileMetadata): Future[UpdateOperation]

  def save(fileMetadata: FileMetadata): Future[SaveOperation]

  def get(id: String): Future[FileMetadata]

  def delete(id: String): Future[Boolean]
}
