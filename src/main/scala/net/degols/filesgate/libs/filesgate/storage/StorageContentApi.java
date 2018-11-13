trait StorageContentApi {
  /**
    * Save a fileContent. Keep in mind that the "fileContent" received can have an incomplete number of chunks
    * if we tried to continue a previous download of a big file (so we will have the offset).
    * @param fileContent
    * @param expectedSize useful to know if we should store the object as a "big file" or as a "small file". This information
    *                     can be important for performance in the underlying StorageSystem
    * @return
    */
  def save(fileContent: FileContent, expectedSize: Option[Long] = None): Future[Try[SaveOperation]]

  /**
    * Return a FileContent. The first chunk will be loaded by default, other chunks will be loaded in memory when needed
    * @param id
    * @return
    */
  def get(id: String): Future[Try[FileContent]]

  /**
    * Delete a FileContent
    */
  def delete(id: String): Future[Try[Boolean]]
}
