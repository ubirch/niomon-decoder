package com.ubirch

import java.nio.charset.StandardCharsets
import java.util.Base64

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import com.ubirch.kafkasupport.MessageEnvelope
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.native.Serialization._

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

package object messagedecoder {
  implicit val formats: DefaultFormats = DefaultFormats

  val conf: Config = ConfigFactory.load
  implicit val system: ActorSystem = ActorSystem("message-decoder")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val kafkaUrl: String = conf.getString("kafka.url")

  val producerConfig: Config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaUrl)

  val consumerConfig: Config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafkaUrl)
      .withGroupId("message-receiver")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  val incomingTopic: String = conf.getString("kafka.topic.incoming")
  val outgoingTopic: String = conf.getString("kafka.topic.outgoing")
  val errorsTopic: String = conf.getString("kafka.topic.errors")


  val decoderStream: RunnableGraph[DrainingControl[Done]] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(incomingTopic))
      .map {
        msg =>
          val messageEnvelope = MessageEnvelope.fromRecord(msg.record)
          val recordToSend = transform(messageEnvelope.payload) match {
            case scala.util.Success(value) =>
              system.log.debug(s"decoded: $value")
              val transformedEnvelope = MessageEnvelope(
                value, messageEnvelope.headers,
                Some(Base64.getEncoder.encodeToString(messageEnvelope.payload)))
              MessageEnvelope.toRecord(outgoingTopic, msg.record.key(), transformedEnvelope)
            case scala.util.Failure(exception) =>
              system.log.error("error while decoding!", exception.getCause)
              val error = mutable.HashMap("error" -> exception.getMessage)
              if (exception.getCause != null) error("cause") = exception.getCause.getMessage

              val errorEnvelope = MessageEnvelope(write(error), messageEnvelope.headers)
              MessageEnvelope.toRecord(errorsTopic, msg.record.key(), errorEnvelope)
          }

          ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
            recordToSend,
            msg.committableOffset
          )
      }
      .toMat(Producer.commitableSink(producerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)


  def transform(payload: Array[Byte]): Try[String] = Try {
    val protocolMessage = payload(0) match {
      case '{' => JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ => MsgPackProtocolDecoder.getDecoder.decode(payload)
    }

    new ObjectMapper().writeValueAsString(protocolMessage, payload)
  }
}
