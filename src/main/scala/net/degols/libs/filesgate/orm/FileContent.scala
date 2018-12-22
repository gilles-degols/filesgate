package net.degols.libs.filesgate.orm

import play.api.libs.json.JsObject

/**
  * Contain the raw data of a FileMetadata. Depending on the size of the file, it might be in RAM or on disk.
  */
@SerialVersionUID(0L)
class FileContent(val json: JsObject) {


}
