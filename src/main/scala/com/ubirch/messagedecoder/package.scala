package com.ubirch

import java.nio.charset.StandardCharsets

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, RestartSink, RestartSource, RunnableGraph, Sink, Source}
import com.typesafe.config.{Config, ConfigFactory}
import com.ubirch.kafkasupport.MessageEnvelope
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.jackson.Serialization._

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

package object messagedecoder {
  implicit val formats: DefaultFormats = DefaultFormats

  import org.json4s.jackson.JsonMethods._

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

  val kafkaSource: Source[ConsumerMessage.CommittableMessage[String, Array[Byte]], NotUsed] =
    RestartSource.withBackoff(
      minBackoff = 2.seconds,
      maxBackoff = 1.minute,
      randomFactor = 0.2
    ) { () => Consumer.committableSource(consumerSettings, Subscriptions.topics(incomingTopic)) }

  val kafkaSink: Sink[ProducerMessage.Envelope[String, String, ConsumerMessage.Committable], NotUsed] =
    RestartSink.withBackoff(
      minBackoff = 2.seconds,
      maxBackoff = 1.minute,
      randomFactor = 0.2
    ) { () => Producer.commitableSink(producerSettings) }

  val decoderStream: RunnableGraph[UniqueKillSwitch] =
    kafkaSource
      .viaMat(KillSwitches.single)(Keep.right)
      .map {
        msg =>
          val messageEnvelope = MessageEnvelope.fromRecord(msg.record)
          val recordToSend = transform(messageEnvelope.payload) match {
            case scala.util.Success(value) =>
              system.log.debug(s"decoded: $value")
              val transformedEnvelope = MessageEnvelope(value, messageEnvelope.headers)
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
      .to(kafkaSink)


  def transform(payload: Array[Byte]): Try[String] = Try {
    val protocolMessage = payload(0) match {
      case '{' =>
        JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ =>
        MsgPackProtocolDecoder.getDecoder.decode(payload)
    }

    mapper.writeValueAsString(protocolMessage)
  }
}
