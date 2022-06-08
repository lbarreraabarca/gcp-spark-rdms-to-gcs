package com.data.factory

import com.data.factory.adapters.{Base64Encoder, RdmsOperator, SparkSessionFactory}
import com.data.factory.exceptions.ControllerException
import com.data.factory.models.RdmsToGcs
import com.data.factory.ports.Encoder
import com.data.factory.services.RdmsToGcsService
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.Logger

object App extends Serializable{
  val log = Logger("App")

     private def makeRequest(jsonInput: String): RdmsToGcs =
       try{
         implicit val formats = Serialization.formats(NoTypeHints)
         read[RdmsToGcs](jsonInput)
       } catch{
         case e:Exception => throw  ControllerException(e.getClass.toString.concat(":").concat(e.getMessage.toString))
       }

    def main(args: Array[String]): Unit = {
        val encodedInput: String = args(0)
        try{
          log.info("Creating sparkSession.")
          val sessionFactory = new SparkSessionFactory()
          val session = sessionFactory.makeCluster()

          log.info("Parsing request.")
          val encoder: Encoder = new Base64Encoder
          val decodedInput: String = encoder.decode(encodedInput)
          val mapper: ObjectMapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val objectNode = mapper.readTree(decodedInput).at("/rdmsToGcs").asInstanceOf[ObjectNode]
          val jsonRequest = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode)
          val request: RdmsToGcs = this.makeRequest(jsonRequest)

          log.info("Creating operators.")
          val projectId: String = scala.util.Properties.envOrElse("ENV_GCP_PROJECT_ID", "null_projectId")
          val rdms: RdmsOperator = new RdmsOperator(session, projectId)

          log.info("Invoking service.")
          val service: RdmsToGcsService = new RdmsToGcsService(rdms, encoder)
          service.invoke(request)
          log.info("Process ended successfully.")
      }
      catch{
          case e:Exception => throw ControllerException(e.getMessage)
      }
    }
}
