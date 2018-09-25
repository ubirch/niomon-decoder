package com.ubirch.messagedecoder

import org.scalatest.{FlatSpec, Matchers}

class TransformationTest extends FlatSpec with Matchers {

  "Message Payload Transformation" should "transform empty message" in {
    val result = transform("{}".getBytes)
    result.isSuccess shouldBe true
    result.get should equal("{\"raw\":\"e30=\",\"message\":{\"version\":0,\"hint\":0}}")
  }
}
