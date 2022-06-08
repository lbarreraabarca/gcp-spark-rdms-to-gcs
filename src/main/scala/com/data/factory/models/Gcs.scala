package com.data.factory.models

import com.data.factory.exceptions.RequestException

import scala.collection.mutable.Map

class Gcs extends Serializable {
  var errorList: Map[String, String] = Map.empty[String, String]
  val objectMessage = "gcs.%s"
  val errorMessage = "cannot be null or empty"
  val formatMessage = "%s format is not valid. This must be some these values: %s"

  var landingPath: String = _
  var fileFormat: String = _
  var writeMode: String = _
  var validFileFormats: List[String] = List("avro", "parquet", "csv")
  var validWriteModes: List[String] = List("overwrite", "append")

  def addError(field: String, message: String) = {
    errorList += (field -> message)
  }

  def this(landingPath: String, fileFormat: String, writeMode: String){
    this()
    if (landingPath == null || landingPath.isEmpty) addError(String.format(objectMessage, "landingPath"), errorMessage)
    else this.landingPath = landingPath

    if (fileFormat == null || fileFormat.isEmpty) addError(String.format(objectMessage, "format"), errorMessage)
    else if (!validFileFormats.contains(fileFormat)) addError(String.format(objectMessage, "format"), String.format(formatMessage, fileFormat, validFileFormats.mkString(",")))
    else this.fileFormat = fileFormat

    if (writeMode == null || writeMode.isEmpty) addError(String.format(objectMessage, "writeMode"), errorMessage)
    else if (!validWriteModes.contains(writeMode)) addError(String.format(objectMessage, "writeMode"), String.format(formatMessage, writeMode, validWriteModes.mkString(",")))
    else this.writeMode = writeMode
  }

  def isValid(): Boolean = {
    if (!this.errorList.isEmpty) throw RequestException("this arguments cannot be empty or null:".concat(this.errorList.toString))
    else this.errorList.isEmpty
  }

}
