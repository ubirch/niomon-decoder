package com.ubirch.decoding

import java.security.SignatureException
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.client.protocol.MultiKeyProtocolVerifier
import com.ubirch.kafka.RichAnyConsumerRecord
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bouncycastle.util.encoders.Hex

trait Verify extends (ConsumerRecord[String, Array[Byte]] => ConsumerRecord[String, Array[Byte]])

object Verify extends LazyLogging {
  def differentiateUbirchMsgPackVersion(msgPack: Array[Byte]): Int = {
    val hexMsgPack = Hex.toHexString(msgPack)
    hexMsgPack(2) match {
      case '1' =>
        //logger.info("msgPack version 1 was found")
        3
      case '2' =>
        //logger.info("msgPack version 2 was found")
        2
      case 'c' if hexMsgPack.slice(176, 178) == "54" =>
        //logger.info("trackle msgPack was found")
        3
      case thirdLetter =>
        val errorMsg = s"Couldn't identify Ubirch msgPack protocol as third letter is neither 1, 2 or 'c' but $thirdLetter"
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
    }
  }
}

class DefaultVerify(verifier: MultiKeyProtocolVerifier) extends Verify with LazyLogging {

  override def apply(input: ConsumerRecord[String, Array[Byte]]): ConsumerRecord[String, Array[Byte]] = {

    try {

      val requestId = input.requestIdHeader().orNull

      input.findHeader(HARDWARE_ID_HEADER_KEY) match {

        case Some(hardwareIdHeader: String) =>

          val hardwareId = UUID.fromString(hardwareIdHeader)
          val msgPack = input.value()

          //Todo: Should I check the length of the package before splitting it?
          val signatureIdentifierLength = Verify.differentiateUbirchMsgPackVersion(msgPack)
          val restOfMessage = msgPack.dropRight(64 + signatureIdentifierLength)
          val signature = msgPack.takeRight(64)

          //Todo: Use cached KeyServiceClient
          verifier
            .verifyMulti(hardwareId, restOfMessage, 0, restOfMessage.length, signature) match {
            case Some(key) =>
              logger.info(s"signature_verified_for=$hardwareId", v("requestId", requestId))
              input.withExtraHeaders(("algorithm", key.getSignatureAlgorithm))
            case None =>
              val errorMsg = s"signature verification failed for msgPack of hardwareId $hardwareId."
              logger.error(errorMsg, v("requestId", requestId))
              throw new SignatureException("Invalid signature")
          }
        case None =>
          val errorMsg = s"Header with key $HARDWARE_ID_HEADER_KEY is missing. Cannot verify msgPack."
          logger.error(errorMsg, v("requestId", requestId))
          throw new SignatureException(errorMsg)
      }

    } catch {
      case e: Exception =>
        throw WithHttpStatus(FORBIDDEN, e)
    }
  }

}
