package com.data.factory.adapters

import com.data.factory.exceptions.QueueException
import com.data.factory.ports.Queue

import java.util.ArrayList
import java.util.List
import java.util.concurrent.TimeUnit
import com.google.api.core.ApiFuture
import org.threeten.bp.Duration
import com.google.api.gax.core.ExecutorProvider
import com.google.api.gax.core.InstantiatingExecutorProvider
import com.google.api.gax.batching.BatchingSettings
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.ProjectTopicName
import com.google.api.gax.core.CredentialsProvider
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.api.gax.core.FixedCredentialsProvider
import com.typesafe.scalalogging.Logger

class PubsubQueue extends Queue with Serializable {
    
    val mylog = Logger(classOf[PubsubQueue])
    var projectId: String = _
    var credentialsPath: String = _
    
    def this(projectId: String){
        this()
        if (this.validateProjectId(projectId)){
            this.projectId = projectId
        }
    }

    def this(projectId: String, credentialsPath: String){
        this(projectId)
        if (credentialsPath.nonEmpty) this.credentialsPath = credentialsPath
        else this.credentialsPath = ""
    }

    private def validateProjectId(projectId: String): Boolean = {
        projectId match {
            case null => throw new QueueException("projectId cannot be null")
            case projectId if projectId.isEmpty => throw new QueueException("projectId cannot be empty")
            case _ => return true
        }
    }

    private def getCredentials(): CredentialsProvider = {
        if (this.credentialsPath.nonEmpty) FixedCredentialsProvider.create(
            ServiceAccountCredentials.fromStream(getClass.getResourceAsStream(this.credentialsPath)))
        else FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault)
    }

    private def getSingleThreadedPublisher(topicName: ProjectTopicName): Publisher ={
        var requestBytesThreshold: Long = 5000L // default : 1 byte
        var publishDelayThreshold: Duration = Duration.ofMillis(100)
        val batchingSettings: BatchingSettings =
            BatchingSettings.newBuilder()
                .setRequestByteThreshold(requestBytesThreshold)
                .setDelayThreshold(publishDelayThreshold)
                .build()
        val executorProvider: ExecutorProvider =
            InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(1).build()
        val credentialsProvider: CredentialsProvider = this.getCredentials()
        val publisher: Publisher =
            Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider).setBatchingSettings(batchingSettings).setExecutorProvider(executorProvider).build()
        return publisher
    }

    private def validateMessage(message: String): Boolean = {
        message match {
            case null => throw new QueueException("message cannot be null")
            case message if message.isEmpty => throw new QueueException("message cannot be empty")
            case _ => return true
        }     
    }

    private def validateTopic(topic: String): Boolean = {
        topic match {
            case null => throw new QueueException("topic cannot be null")
            case topic if topic.isEmpty => throw new QueueException("topic cannot be empty")
            case _ => return true
        }     
    }

    def publish(message: String, topic: String): Unit = {
        this.validateMessage(message)
        this.validateTopic(topic)
        mylog.info("Sending message to PubSub: {}", message)
        mylog.info("projectId: {}", this.projectId)
        mylog.info("topic: {}", topic)
        var messageId: String = ""
        val topicName: ProjectTopicName = ProjectTopicName.of(this.projectId, topic)
        val futures: List[ApiFuture[String]] = new ArrayList()

        var publisher: Publisher = null
        try 
        {
            publisher = this.getSingleThreadedPublisher(topicName)
            val data: ByteString = ByteString.copyFromUtf8(message)
            val pubsubMessage: PubsubMessage = PubsubMessage.newBuilder().setData(data).build()
            val messageIdFuture: ApiFuture[String] = publisher.publish(pubsubMessage)
        }

        catch {

            case ex: Exception =>
            {
                ex.printStackTrace()
                throw new QueueException("message cannot be published")
            }
        }

        finally
        {
            if (publisher != null){
                try{
                    //mylog.info("Closing connection to PubSub")
                    publisher.shutdown()
                    publisher.awaitTermination(5, TimeUnit.SECONDS)
                } 
                catch{
                    case ex: Exception =>
                    {
                        throw new QueueException("The Following error has occurred when closing connection")
                    }
                }
            }
        }       
        
    }

}