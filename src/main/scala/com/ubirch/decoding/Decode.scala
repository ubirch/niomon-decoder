package com.ubirch.decoding

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.kafka.{MessageEnvelope, _}
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import com.ubirch.protocol.{ProtocolException, ProtocolMessage}
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.Try

/**
 * Represents a decoding function
 */
trait Decode extends (ConsumerRecord[String, Array[Byte]] => ProducerRecord[String, MessageEnvelope])

object Decode {

  def transform(payload: Array[Byte]): Try[ProtocolMessage] = Try {
    payload(0) match {
      case '{' =>
        JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ =>
        MsgPackProtocolDecoder.getDecoder.decode(payload)
    }
  }

}

/**
 * Represents the default decoding function
 * @param topic it is the topic to send decoded messages to.
 */
class DefaultDecode(topic: String) extends Decode with LazyLogging {

  override def apply(input: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, MessageEnvelope] = {

    val pm = try Decode.transform(input.value()).get catch {
      case pe: ProtocolException => throw WithHttpStatus(BAD_REQUEST, pe)
    }
    if (pm.getPayload == null) throw WithHttpStatus(BAD_REQUEST, new ProtocolException("Protocol message payload is null"))

    val headerUUID = Try(
      input.findHeader(HARDWARE_ID_HEADER_KEY)
        .map(UUID.fromString)
        .get
    ).getOrElse(throw WithHttpStatus(BAD_REQUEST, new Exception(s"$HARDWARE_ID_HEADER_KEY not found in headers")))

    if (headerUUID != pm.getUUID) throw WithHttpStatus(FORBIDDEN, new ProtocolException("Header UUID does not match protocol message UUID"))

    logger.info(s"decoded: $pm", v("requestId", input.key()))

    // signer down the line doesn't support the legacy version, so we're upgrading the version here
    if ((pm.getVersion >> 4) == 1) {
      logger.warn("detected old version of protocol, upgrading", v("requestId", input.key))
      pm.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (pm.getVersion & 0x0f))
    }

    input.toProducerRecord(topic, MessageEnvelope(pm))

  }

}


