package net.degols.filesgate.storage.systems.mongo

/**
  * Configuration for a specific instance of mongodb. We can have two instances of MongoDB: One for the content, another
  * for the metadata. This class is used to carry the related configuration (uri, ...)
  */
case class MongoConfiguration(uri: String)
