package net.degols.filesgate.libs.filesgate.orm

/**
  * Subset of a FileContent. If the file is smaller than an arbitrary value, it might be stored differently by the StorageContentApi.
  * For example, if we know that we have a small file, we can store it in MongoDB in a single document. If we have a file
  * bigger than a specific size, we will need to store in gridfs in MongoDB.
  */
class FileContentChunk {

}
