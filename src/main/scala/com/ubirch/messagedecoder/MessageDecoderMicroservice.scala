package com.ubirch.messagedecoder

import java.nio.charset.StandardCharsets

import com.ubirch.kafka.{MessageEnvelope, _}
import com.ubirch.messagedecoder.MessageDecoderMicroservice._
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import com.ubirch.protocol.{ProtocolException, ProtocolMessage}
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.DefaultFormats

import scala.util.Try


class MessageDecoderMicroservice(runtime: NioMicroservice[Array[Byte], MessageEnvelope])
  extends NioMicroserviceLogic[Array[Byte], MessageEnvelope](runtime) {
  implicit val formats: DefaultFormats = DefaultFormats

  override def processRecord(input: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, MessageEnvelope] = {
    val value = try transform(input.value()).get catch {
      case pe: ProtocolException => throw WithHttpStatus(400, pe)
    }
    logger.info(s"decoded: $value", v("requestId", input.key()))

    // signer down the line doesn't support the legacy version, so we're upgrading the version here
    if ((value.getVersion >> 4) == 1) {
      logger.warn("detected old version of protocol, upgrading", v("requestId", input.key))
      value.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (value.getVersion & 0x0f))
    }

    input.toProducerRecord(outputTopics("valid"), MessageEnvelope(value))
  }
}

object MessageDecoderMicroservice {
  def apply(runtime: NioMicroservice[Array[Byte], MessageEnvelope]): MessageDecoderMicroservice =
    new MessageDecoderMicroservice(runtime)

  def transform(payload: Array[Byte]): Try[ProtocolMessage] = Try {
    payload(0) match {
      case '{' =>
        JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ =>
        MsgPackProtocolDecoder.getDecoder.decode(payload)
    }
  }
}
