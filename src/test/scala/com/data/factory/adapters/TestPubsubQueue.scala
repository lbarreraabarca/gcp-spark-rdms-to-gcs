package com.data.factory.adapters

import com.falabella.products.models._
import com.falabella.products.ports._
import com.falabella.products.exceptions._
import org.scalatest.Assertions._
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalamock.scalatest.MockFactory
import com.google.api.core.ApiFuture
import com.google.pubsub.v1.ProjectTopicName
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.pubsub.v1.Topic
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PushConfig
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.api.gax.core.CredentialsProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.PubsubMessage
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.api.gax.core.ExecutorProvider
import com.google.api.gax.core.InstantiatingExecutorProvider
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import org.scalatest._
import com.typesafe.scalalogging.Logger


class TestPubsubQueue extends FlatSpec with BeforeAndAfterAll {
    //final val done: ApiFuture<Void>
    val mylog = Logger(classOf[TestPubsubQueue])
    /*
    private def getCredentials(): CredentialsProvider = {
        return FixedCredentialsProvider.create(
            ServiceAccountCredentials.fromStream(getClass.getResourceAsStream("/service-account.json")))
    }

    private def deleteTopic(projectId: String, topicId: String): Unit = {
        
        val credentialsProvider: CredentialsProvider = this.getCredentials()
        val topicAdminSettings: TopicAdminSettings = TopicAdminSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider)
            .build()
        val topicAdminClient = TopicAdminClient.create(topicAdminSettings)

        try {
            val topicName = ProjectTopicName.of(projectId, topicId)
            topicAdminClient.deleteTopic(topicName)
        }
        catch {
            case x: Exception => {
                print(x.getMessage)
            }
        }
        finally {
            topicAdminClient.close()
        }
    }

    private def createTopic(projectId: String, topicId: String): Unit = {
        
        val credentialsProvider: CredentialsProvider = this.getCredentials()
        val topicAdminSettings: TopicAdminSettings = TopicAdminSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider)
            .build()
        val topicAdminClient = TopicAdminClient.create(topicAdminSettings)
        
        try {
            val topicName = ProjectTopicName.of(projectId, topicId)
            topicAdminClient.createTopic(topicName)
        }
        catch {
            case x: Exception => {
                print(x.getMessage)
            }
        }
        finally {
            topicAdminClient.close()
        }
    }

    private  def createSubscription(projectId: String, topicId: String, subscriptionId: String): Unit = {
        
        val credentialsProvider: CredentialsProvider = this.getCredentials()
        val subscriptionAdminSettings: SubscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider)
            .build()
        
        val subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)
        try {
            val topicName = ProjectTopicName.of(projectId, topicId)
            val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
            subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0)
        }
        catch {
            case x: Exception => {
                print(x.getMessage)
            }
        }
        finally {
            subscriptionAdminClient.close()
        }
    }

    private def deleteSubscription(projectId: String, subscriptionId: String): Unit = {
        
        val credentialsProvider: CredentialsProvider = this.getCredentials()
        val subscriptionAdminSettings: SubscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider)
            .build()
        
        val subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)
        try {
            val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
            subscriptionAdminClient.deleteSubscription(subscriptionName)
        }
        catch {
            case x: Exception => {
                print(x.getMessage)
            }
        }
        finally {
            subscriptionAdminClient.close()
        }
    }

    private def pullMessage(projectId: String, subscription: String): String = {
        val subscriptionId = "projects/%s/subscriptions/%s".format(projectId, subscription)
        val subscriptionName: ProjectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
        var messageId: String = ""
        val credentialsProvider: CredentialsProvider = this.getCredentials()
        val executorProvider: ExecutorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(4).build()
        val pool: ExecutorService = Executors.newCachedThreadPool()

        val receiver: MessageReceiver =
            new MessageReceiver() {
            @Override
            def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer) {
                // handle incoming message, then ack/nack the received message
                mylog.info("Id : {}", message.getMessageId())
                messageId = message.getMessageId()
                mylog.info("Data : {}", message.getData().toStringUtf8())
                consumer.ack()
            }
        }
        var subscriber: Subscriber = null
        try {
            subscriber = Subscriber.newBuilder(subscriptionId, receiver)
                .setCredentialsProvider(credentialsProvider)
                .setParallelPullCount(2)
                .setExecutorProvider(executorProvider)
                .build()
            subscriber.startAsync().awaitRunning()
            //subscriber.awaitTerminated()
            return messageId
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync()awaitTerminated()
                //subscriber.shutdown()
            }
        }
    }

    override def beforeAll() {
        val projectId = "fal-retail-dtlk-uat"
        val topic = "test_publish_topic"
        val subscription = "test_publish_sub"
        this.createTopic(projectId, topic)
        this.createSubscription(projectId, topic, subscription)
    }
    
    override def afterAll() {
        val projectId = "fal-retail-dtlk-uat"
        val topic = "test_publish_topic"
        val subscription = "test_publish_sub"
        this.deleteTopic(projectId, topic)
        this.deleteSubscription(projectId, subscription)
    }

    "constructor" should "create a valid object when receive valid projectId" in {
        
        //Arrange
        val projectId: String = "fal-retail-dtlk-uat"

        //Act
        val queueOperator = new PubsubQueue(projectId)

        //Assert
        assert(queueOperator.isInstanceOf[PubsubQueue])
    }

    it should "return an QueueException when receive null projectId" in {
        //Arrange
        val projectId: String = null

        //Act
        val actual =
        intercept[QueueException] {
             val queueOperator = new PubsubQueue(projectId)
        }
        val expected = "projectId cannot be null"
        
        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return an QueueException when receive empty projectId" in {
        //Arrange
        val projectId: String = ""
        
        //Act
        val actual =
        intercept[QueueException] {
             val queueOperator = new PubsubQueue(projectId)
        }
        val expected = "projectId cannot be empty"
        
        //Assert
        assert(actual.getMessage == expected)
    }

    "publish" should "publish a message when receive valid message and topic" in {
        
        //Arrange
        val projectId: String = "fal-retail-dtlk-uat"
        val message: String = "message"
        val topic: String = "test_publish_topic"
        val subscription: String = "test_publish_sub"
        
        //Act
        val queueOperator = new PubsubQueue(projectId, "/service-account.json")
        val actual = queueOperator.publish(message, topic)

        //Assert
        assert(true)
    }

    it should "return an QueueException when receive valid projectId, empty message and valid topic" in {
        //Arrange
        val projectId: String = "fal-retail-dtlk-uat"
        val message: String = ""
        val topic: String = "test_publish_topic"
        val queueOperator = new PubsubQueue(projectId)

        //Act
        val actual =
        intercept[QueueException] {
             val messageId = queueOperator.publish(message, topic)
        }
        val expected = "message cannot be empty"
        
        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return an QueueException when receive valid projectId, null message and valid topic" in {
        //Arrange
        val projectId: String = "fal-retail-dtlk-uat"
        val message: String = null
        val topic: String = "test_publish_topic"
        val queueOperator = new PubsubQueue(projectId)

        //Act
        val actual =
        intercept[QueueException] {
             val messageId = queueOperator.publish(message, topic)
        }
        val expected = "message cannot be null"
        
        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return an QueueException when receive valid projectId, valid message and empty topic" in {
        //Arrange
        val projectId: String = "fal-retail-dtlk-uat"
        val message: String = "message"
        val topic: String = ""
        val queueOperator = new PubsubQueue(projectId)

        //Act
        val actual =
        intercept[QueueException] {
             val messageId = queueOperator.publish(message, topic)
        }
        val expected = "topic cannot be empty"
        
        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return an QueueException when receive valid projectId, valid message and null topic" in {
        //Arrange
        val projectId: String = "fal-retail-dtlk-uat"
        val message: String = "message"
        val topic: String = null
        val queueOperator = new PubsubQueue(projectId)

        //Act
        val actual =
        intercept[QueueException] {
             val messageId = queueOperator.publish(message, topic)
        }
        val expected = "topic cannot be null"
        
        //Assert
        assert(actual.getMessage == expected)
    }

    // it should "return an QueueException when receive valid projectId, valid message and invalid topic" in {
    //     //Arrange
    //     val projectId: String = "fal-retail-dtlk-uat"
    //     val message: String = "message"
    //     val topic: String = "test_publish_topic_not_found"
    //     val queueOperator = new PubsubQueue(projectId)

    //     //Act
    //     val actual =
    //     intercept[QueueException] {
    //          val messageId = queueOperator.publish(message, topic)
    //     }
    //     val expected = "topic cannot be found"
        
    //     //Assert
    //     assert(actual.getMessage == expected)
    // }
     */

}
