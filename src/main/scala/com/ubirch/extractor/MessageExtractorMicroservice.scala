package com.ubirch.extractor

import com.ubirch.client.protocol.MultiKeyProtocolVerifier
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

class MessageExtractorMicroservice(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier,
                                   runtime: NioMicroservice[Array[Byte], MessageEnvelope])
  extends NioMicroserviceLogic[Array[Byte], MessageEnvelope](runtime) {

  val protocolVerifier: MultiKeyProtocolVerifier = verifierFactory(context)

  def verify: Verify = new DefaultVerify(protocolVerifier)
  def decode: Decode = new DefaultDecode(outputTopics("valid"))

  override def processRecord(input: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, MessageEnvelope] = {
    (verify andThen decode)(input)
  }
}

object MessageExtractorMicroservice {
  def apply(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier)
           (runtime: NioMicroservice[Array[Byte], MessageEnvelope]): MessageExtractorMicroservice =
    new MessageExtractorMicroservice(verifierFactory, runtime)
}
