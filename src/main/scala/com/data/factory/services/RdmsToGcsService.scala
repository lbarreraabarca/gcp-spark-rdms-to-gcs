package com.data.factory.services

import com.data.factory.adapters.RdmsOperator
import com.data.factory.exceptions.{ControllerException, RdmsToGcsServiceException}
import com.data.factory.models.{RdmsToGcs, Response, ResponseSuccess}
import com.data.factory.ports.Encoder
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame

class RdmsToGcsService extends Serializable {
  val log = Logger("RdmsToGcsService")
  var rdmsOperator: RdmsOperator = _
  var encoder: Encoder = _
  val INVALID_ARGUMENT: String = "%s cannot be null or empty."

  def this(rdmsOperator: RdmsOperator, encoder: Encoder){
    this()
    if (rdmsOperator == null) throw RdmsToGcsServiceException(INVALID_ARGUMENT.format("rdmsOperator"))
    else this.rdmsOperator = rdmsOperator

    if (encoder == null) throw RdmsToGcsServiceException(INVALID_ARGUMENT.format("encoder"))
    else this.encoder = encoder
  }

  def invoke(request: RdmsToGcs): Response = {
    try {
      log.info("Validating request.")
      request.isValid
      val jdbcUrl: String = request.rdms.getJdbcUrl()
      val jdbcDriverClassName: String = request.rdms.getJdbcDriverClassName()
      val encodedQuery: String = request.rdms.query
      val query: String = this.encoder.decode(encodedQuery)
      val fetchSize: Integer = request.sparkProperties.fetchSize

      log.info("Getting data from {}", jdbcUrl)
      val df: DataFrame = this.rdmsOperator.getTable(jdbcUrl, jdbcDriverClassName, query, fetchSize)
      log.info("Row count {}", df.count().toString)
      val landingPath: String = request.gcs.landingPath
      log.info("Saving data to {}", landingPath)
      this.rdmsOperator.saveTable(landingPath, df, request.gcs.fileFormat, request.gcs.writeMode)
      new ResponseSuccess("RdmsToGcs service", "Service ended successfully.")
    } catch{
      case e:Exception => throw ControllerException(e.getMessage)
    }
  }
}
