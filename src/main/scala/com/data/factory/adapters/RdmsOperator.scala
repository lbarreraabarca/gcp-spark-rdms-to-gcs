package com.data.factory.adapters

import com.data.factory.exceptions.RdmsException
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.mutable.Map

class RdmsOperator {

  var session: SparkSession = _
  var projectId: String = _
  val objectErrorMessage: String = "%s cannot be null."
  val stringErrorMessage: String = "%s cannot be null or empty."

  val mylog = Logger("RdmsOperator")

  def this(session: SparkSession, projectId: String){
    this()

    if (session == null) throw RdmsException(objectErrorMessage.format("session"))
    else this.session = session

    if (projectId == null || projectId.isEmpty) throw new RdmsException(stringErrorMessage.format("projectId"))
    else this.projectId = projectId
  }

  def getTable(jdbcUrl: String, jdbcDriverClassName: String, query: String, fetchSize: Integer): DataFrame ={
    if (jdbcUrl == null || jdbcUrl.isEmpty) throw RdmsException(stringErrorMessage.format("url"))
    if (jdbcDriverClassName == null || jdbcDriverClassName.isEmpty) throw new RdmsException(stringErrorMessage.format("driverClassName"))
    if (query == null || query.isEmpty) throw RdmsException(stringErrorMessage.format("query"))

    val jdbcProperties: Map[String, String] = Map.empty[String, String]
    jdbcProperties.put(JDBCOptions.JDBC_URL, jdbcUrl)
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS, jdbcDriverClassName)
    jdbcProperties.put(JDBCOptions.JDBC_QUERY_STRING, query)
    jdbcProperties.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE, fetchSize.toString)
    this.session.read.format("jdbc").options(jdbcProperties).load()
  }

  def saveTable(output: String, df: DataFrame, fileFormat: String, mode: String = "overwrite") = {
    if (output == null || output.isEmpty) throw new RdmsException(stringErrorMessage.format("output"))
    if (df == null) throw new RdmsException(objectErrorMessage.format("dataFrame"))
    if (fileFormat == null || fileFormat.isEmpty) throw new RdmsException(stringErrorMessage.format("fileFormat"))
    writeTableWithMode(df, output, fileFormat, mode)
  }

  private def writeTableWithMode(df: DataFrame, output: String, fileFormat: String, mode: String) = {
    mode match {
      case "overwrite" => df.write.mode(SaveMode.Overwrite).format(fileFormat).save(output)
      case "append" => df.write.mode(SaveMode.Append).format(fileFormat).save(output)
      case _ => throw RdmsException(s"mode: $mode not implemented")
    }
  }
}
