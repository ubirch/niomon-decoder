/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Keep, RestartSink, RestartSource, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import com.typesafe.config.{Config, ConfigFactory}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka._
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.jackson.Serialization._

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
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
      .map { msg =>
        val recordToSend = transform(msg.record.value()) match {
          case scala.util.Success(value) =>
            system.log.debug(s"decoded: $value")

            // signer down the line doesn't support the legacy version, so we're upgrading the version here
            if ((value.getVersion >> 4) == 1) {
              system.log.debug("detected old version of protocol, upgrading")
              value.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (value.getVersion & 0x0f))
            }

            val transformedEnvelope = MessageEnvelope(value)
            val payload = EnvelopeSerializer.serializeToString(transformedEnvelope)

            // the type ascription is technically unnecessary, but idea complains if it isn't there
            msg.record.toProducerRecord(outgoingTopic).copy(value = payload): ProducerRecord[String, String]
          case scala.util.Failure(exception) =>
            system.log.error("error while decoding!", exception.getCause)
            val error = mutable.HashMap("error" -> exception.getMessage)
            if (exception.getCause != null) error("cause") = exception.getCause.getMessage
            // the type ascription is technically unnecessary, but idea complains if it isn't there
            msg.record.toProducerRecord(errorsTopic).copy(value = write(error)): ProducerRecord[String, String]
        }

        ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
          recordToSend, // ignore IDEA here, this compiles
          msg.committableOffset
        )
      }
      .to(kafkaSink)


  def transform(payload: Array[Byte]): Try[ProtocolMessage] = Try {
    payload(0) match {
      case '{' =>
        JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ =>
        MsgPackProtocolDecoder.getDecoder.decode(payload)
    }
  }
}
