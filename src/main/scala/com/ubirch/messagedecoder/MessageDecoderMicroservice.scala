package com.ubirch.messagedecoder

import java.nio.charset.StandardCharsets

import com.ubirch.kafka.{EnvelopeSerializer, MessageEnvelope}
import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import MessageDecoderMicroservice._

class MessageDecoderMicroservice extends NioMicroservice[Array[Byte], String]("message-decoder") {
  implicit val formats: DefaultFormats = DefaultFormats

  override def process(input: Array[Byte]): (String, String) = {
    transform(input) match {
      case Success(value) =>
        logger.debug(s"decoded: $value")

        // signer down the line doesn't support the legacy version, so we're upgrading the version here
        if ((value.getVersion >> 4) == 1) {
          logger.debug("detected old version of protocol, upgrading")
          value.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (value.getVersion & 0x0f))
        }

        val transformedEnvelope = MessageEnvelope(value)
        val payload = EnvelopeSerializer.serializeToString(transformedEnvelope)

        payload -> "valid"

      // TODO: maybe remove this whole section and let NioMicroservice handle all errors in a consistent way?
      case Failure(exception) =>
        logger.error("error while decoding!", exception)
        val error = mutable.HashMap("error" -> exception.getMessage)
        if (exception.getCause != null) error("cause") = exception.getCause.getMessage

        write(error) -> "error"
    }
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
