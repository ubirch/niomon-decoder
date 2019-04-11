package com.ubirch.messagedecoder

import java.nio.charset.StandardCharsets

import com.ubirch.kafka.{EnvelopeSerializer, MessageEnvelope}
import com.ubirch.messagedecoder.MessageDecoderMicroservice._
import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.json4s.DefaultFormats

import scala.util.Try

class MessageDecoderMicroservice extends NioMicroservice[Array[Byte], String]("message-decoder") {
  implicit val formats: DefaultFormats = DefaultFormats

  override def process(input: Array[Byte]): (String, String) = {
    val value = transform(input).get
    logger.debug(s"decoded: $value")

    // signer down the line doesn't support the legacy version, so we're upgrading the version here
    if ((value.getVersion >> 4) == 1) {
      logger.debug("detected old version of protocol, upgrading")
      value.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (value.getVersion & 0x0f))
    }

    val transformedEnvelope = MessageEnvelope(value)
    val payload = EnvelopeSerializer.serializeToString(transformedEnvelope)

    payload -> "valid"
  }
}

object MessageDecoderMicroservice {
  def transform(payload: Array[Byte]): Try[ProtocolMessage] = Try {
    payload(0) match {
      case '{' =>
        JSONProtocolDecoder.getDecoder.decode(new String(payload, StandardCharsets.UTF_8))
      case _ =>
        MsgPackProtocolDecoder.getDecoder.decode(payload)
    }
  }
}
