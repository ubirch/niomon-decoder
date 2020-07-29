package com.ubirch.decoding

import java.security.SignatureException
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.client.protocol.MultiKeyProtocolVerifier
import com.ubirch.kafka.RichAnyConsumerRecord
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.protocol.codec.MsgPackProtocolDecoder
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

trait Verify extends (ConsumerRecord[String, Array[Byte]] => ConsumerRecord[String, Array[Byte]])

class DefaultVerify(verifier: MultiKeyProtocolVerifier) extends Verify with LazyLogging {

  override def apply(record: ConsumerRecord[String, Array[Byte]]): ConsumerRecord[String, Array[Byte]] = {

    try {

      val requestId = record.requestIdHeader().orNull

      val hardwareId = Try(
        record.findHeader(HARDWARE_ID_HEADER_KEY)
          .map(UUID.fromString)
          .get
      ).getOrElse{
        val errorMsg = s"Header with key $HARDWARE_ID_HEADER_KEY is missing. Cannot verify msgpack."
        logger.error(errorMsg, v("requestId", requestId))
        throw new SignatureException(errorMsg)
      }

      //Todo: Check the length of package
      val dataToVerifyAndSignature =
        Try(MsgPackProtocolDecoder.getDecoder.getDataToVerifyAndSignature(record.value()))
        .getOrElse{
          val errorMsg = s"error building message parts for hardwareId $hardwareId."
          logger.error(errorMsg, v("requestId", requestId))
          throw new SignatureException("Invalid parts for verification")
        }
      val dataToVerify = dataToVerifyAndSignature(0)
      val signature = dataToVerifyAndSignature(1)

      //Todo: Use cached KeyServiceClient
      verifier
        .verifyMulti(hardwareId, dataToVerify, 0, dataToVerify.length, signature) match {

        case Some(key) =>
          logger.info(s"signature_verified_for=$hardwareId", v("requestId", requestId))
          record.withExtraHeaders(("algorithm", key.getSignatureAlgorithm))
        case None =>
          val errorMsg = s"signature verification failed for msgpack of hardwareId $hardwareId."
          logger.error(errorMsg, v("requestId", requestId))
          throw new SignatureException("Invalid signature")
      }


    } catch {
      case e: Exception =>
        throw WithHttpStatus(FORBIDDEN, e)
    }
  }

}
