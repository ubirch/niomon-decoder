package com.ubirch.messagedecoder

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

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
import org.redisson.api.RMapCache

import scala.util.Try


class MessageDecoderMicroservice(runtime: NioMicroservice[Array[Byte], MessageEnvelope])
  extends NioMicroserviceLogic[Array[Byte], MessageEnvelope](runtime) {

  implicit val formats: DefaultFormats = DefaultFormats

  //This cache is shared with verification-microservice (not a part of niomon) for faster verification on its side
  private val uppCache: RMapCache[Array[Byte], String] = context.redisCache.redisson.getMapCache("verifier-upp-cache")
  private val uppTtl = config.getDuration("verifier-upp-cache.timeToLive")
  private val uppMaxIdleTime = config.getDuration("verifier-upp-cache.maxIdleTime")

  override def processRecord(input: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, MessageEnvelope] = {
    val pm = try transform(input.value()).get catch {
      case pe: ProtocolException => throw WithHttpStatus(400, pe)
    }

    try {
      val hash = pm.getPayload.asText().getBytes(StandardCharsets.UTF_8)
      uppCache.fastPut(hash, b64(input.value()), uppTtl.toNanos, TimeUnit.NANOSECONDS, uppMaxIdleTime.toNanos, TimeUnit.NANOSECONDS)
    } catch {
      case ex: Throwable => logger.error("couldn't store upp to cache", ex)
    }
    logger.info(s"decoded: $pm", v("requestId", input.key()))

    // signer down the line doesn't support the legacy version, so we're upgrading the version here
    if ((pm.getVersion >> 4) == 1) {
      logger.warn("detected old version of protocol, upgrading", v("requestId", input.key))
      pm.setVersion((ProtocolMessage.ubirchProtocolVersion << 4) | (pm.getVersion & 0x0f))
    }

    input.toProducerRecord(outputTopics("valid"), MessageEnvelope(pm))
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

  private def b64(x: Array[Byte]): String = if (x != null) Base64.getEncoder.encodeToString(x) else null

}
