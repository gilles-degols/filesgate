trait StorageMetadataApi {
  def update(fileMetadata: FileMetadata): Future[Try[UpdateOperation]]

  def save(fileMetadata: FileMetadata): Future[Try[SaveOperation]]

  def get(id: String): Future[Try[FileMetadata]]

  def delete(id: String): Future[Try[Boolean]]

  def listToRecover(priority: Int, quantity: Int): Future[Try[List[FileMetadata]]]
}
