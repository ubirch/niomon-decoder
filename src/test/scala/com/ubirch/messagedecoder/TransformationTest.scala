package com.ubirch.messagedecoder

import com.ubirch.kafkasupport.MessageEnvelope
import org.scalatest.{FlatSpec, Matchers}

class TransformationTest extends FlatSpec with Matchers {

  "Message Payload Transformation" should "transform empty message" in {
    val result = transform(MessageEnvelope("{}".getBytes))
    result.payload should equal("{\"raw\":\"e30=\",\"message\":{\"version\":0,\"hint\":0}}")
  }

  "Message Payload Transformation" should "return headers from input to result" in {
    val result = transform(MessageEnvelope("{}".getBytes, Map("key" -> "value")))
    result.headers should contain("key" -> "value")
  }


}
