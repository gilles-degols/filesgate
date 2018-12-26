package net.degols.libs.filesgate.orm

import play.api.libs.Codecs
import play.api.libs.json.{JsObject, Json}

/**
  * Contains the metadata linked to a file.
  */
@SerialVersionUID(0L)
case class FileMetadata(url: String) {
  var metadata: JsObject = Json.obj()
  var downloaded: Boolean = false

  final val id: String = Codecs.sha1(url.getBytes)
}
