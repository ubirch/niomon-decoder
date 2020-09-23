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

  lazy val jSONProtocolDecoder: JSONProtocolDecoder = JSONProtocolDecoder.getDecoder
  lazy val msgPackProtocolDecoder: MsgPackProtocolDecoder = MsgPackProtocolDecoder.getDecoder

  def transform(payload: Array[Byte]): Try[ProtocolMessage] = Try {
    payload(0) match {
      case '{' =>
        jSONProtocolDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ =>
        msgPackProtocolDecoder.decode(payload)
    }
  }

}

/**
 * Represents the default decoding function
 * @param topic it is the topic to send decoded messages to.
 */
class DefaultDecode(topic: String) extends Decode with LazyLogging {

  override def apply(record: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, MessageEnvelope] = {

    try {

      val requestId = record.requestIdHeader().orNull

      val pm = try Decode.transform(record.value()).get catch {
        case pe: ProtocolException => throw WithHttpStatus(BAD_REQUEST, pe)
      }
      if (pm.getPayload == null) throw WithHttpStatus(BAD_REQUEST, new ProtocolException("Protocol message payload is null"))

      val headerUUID = Try(
        record.findHeader(HARDWARE_ID_HEADER_KEY)
          .map(UUID.fromString)
          .get
      ).getOrElse(throw WithHttpStatus(BAD_REQUEST, new NoSuchElementException(s"$HARDWARE_ID_HEADER_KEY not found in headers")))

      if (headerUUID != pm.getUUID) throw WithHttpStatus(FORBIDDEN, new IllegalArgumentException("Header UUID does not match protocol message UUID"))

      logger.info(s"decoded: $pm", v("requestId", requestId))

      // signer down the line doesn't support the legacy version, so we're upgrading the version here
      if ((pm.getVersion >> 4) == 1) {
        logger.warn("detected old version of protocol, upgrading", v("requestId", requestId))
        pm.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (pm.getVersion & 0x0f))
      }

      record.toProducerRecord(topic, MessageEnvelope(pm))

    } catch {
      case e: WithHttpStatus =>
        throw WithHttpStatus(e.status, e.cause, Some(xcode(e.cause)))
      case e: Exception =>
        throw WithHttpStatus(BAD_REQUEST, e, Some(xcode(e)))
    }

  }

  def xcode(reason: Throwable): Int = reason match {
    case _: NoSuchElementException => 2100
    case _: IllegalArgumentException => 2200
    case _: ProtocolException => 2300
    case _ => 2500
  }

}


