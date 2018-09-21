package com.ubirch.messagedecoder

import com.ubirch.kafkasupport.MessageEnvelope
import org.scalatest.{FlatSpec, Matchers}

class TransformationTest extends FlatSpec with Matchers{

  "Message Payload Transformation" should "transform lowercase input to uppercase" in {
    val lowercaseInput = MessageEnvelope("lowercase input".getBytes)

    val result = transform(lowercaseInput)

    result.payload should equal("LOWERCASE INPUT")
  }

 "Message Payload Transformation" should "return headers from input to result" in {
    val someInput = MessageEnvelope("".getBytes, Map("key" -> "value"))

    val result = transform(someInput)

    result.headers should contain ("key"->"value")
  }



}
