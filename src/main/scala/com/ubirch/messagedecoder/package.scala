package com.ubirch

import java.nio.charset.StandardCharsets

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
import com.ubirch.protocol.ProtocolMessageEnvelope
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContextExecutor

package object messagedecoder {

  val conf: Config = ConfigFactory.load
  implicit val system: ActorSystem = ActorSystem("message-decoder")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val mapper: ObjectMapper = new ObjectMapper
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


  val decoderStream: RunnableGraph[DrainingControl[Done]] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(incomingTopic))
      .map { msg =>
        val messageEnvelope = MessageEnvelope.fromRecord(msg.record)
        val transformed = transform(messageEnvelope)
        val recordToSend = MessageEnvelope.toRecord(outgoingTopic, msg.record.key(), transformed)

        ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
          recordToSend,
          msg.committableOffset
        )
           }
      .toMat(Producer.commitableSink(producerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)

  /**
    * ToDo BjB 21.09.18 : actual transformation should happen somewhere below
    */
  def transform(envelope: MessageEnvelope[Array[Byte]]): MessageEnvelope[String] = {
    MessageEnvelope(transformPayload(envelope.payload), envelope.headers)
  }

  private def transformPayload(payload: Array[Byte]): String = {
    val protocolMessage = payload(0) match {
      case '{' => JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ => MsgPackProtocolDecoder.getDecoder.decode(payload)
    }

    val envelope = new ProtocolMessageEnvelope(protocolMessage)
    envelope.setRaw(payload)
    mapper.writeValueAsString(envelope)
  }
}
