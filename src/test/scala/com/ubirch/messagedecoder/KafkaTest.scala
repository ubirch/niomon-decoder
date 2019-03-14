/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch.messagedecoder

import java.nio.charset.StandardCharsets
import java.util.Base64

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

//noinspection TypeAnnotation
class KafkaTest extends FunSuite with Matchers with BeforeAndAfterAll with EmbeddedKafka {
  implicit val formats = DefaultFormats
  implicit val bytesSerializer = new ByteArraySerializer

  test("decode a simple json message") {
    val binaryMessage = "{\"version\":18}".getBytes(StandardCharsets.UTF_8)
    publishToKafka(new ProducerRecord("fromreceiver", "valid", binaryMessage))

    val toVerifierMessages = consumeNumberStringMessagesFrom("toverifier", 1)

    toVerifierMessages.size should be(1)

    val msg = parse(toVerifierMessages.head) \ "ubirchPacket"
    (msg \ "signed").extractOpt should be(None)
    (msg \ "version").extract[Int] should be(34)
    (msg \ "hint").extract[Int] should be(0)
  }

  test("send an error message if json decoding fails") {
    val binaryMessage = "{broken}".getBytes(StandardCharsets.UTF_8)
    publishToKafka(new ProducerRecord("fromreceiver", "broken", binaryMessage))

    val toErrorsMessages = consumeNumberStringMessagesFrom("errors", 1)
    toErrorsMessages.size should be(1)

    (parse(toErrorsMessages.head) \ "error").extract[String] should equal("extraction of signed data failed")
  }

  test("decode a simple msgpack message") {
    val msgpackData = Hex.decodeHex("9512b06eac4d0b16e645088c4622e7451ea5a1ccef01da0040578a5b22ceb3e1d0d0f8947c098010133b44d3b1d2ab398758ffed11507b607ed37dbbe006f645f0ed0fdbeb1b48bb50fd71d832340ce024d5a0e21c0ebc8e0e".toCharArray)
    publishToKafka(new ProducerRecord("fromreceiver", "valid", msgpackData))

    val toVerifierMessages = consumeNumberStringMessagesFrom("toverifier", 1)
    toVerifierMessages.size should be(1)

    val msg = parse(toVerifierMessages.head) \ "ubirchPacket"
    (msg \ "signed").extract[String] should equal("lRKwbqxNCxbmRQiMRiLnRR6loczvAQ==")
    (msg \ "version").extract[Int] should be(34)
    (msg \ "hint").extract[Int] should be(0xEF)
    (msg \ "uuid").extract[String] should equal("6eac4d0b-16e6-4508-8c46-22e7451ea5a1")
    (msg \ "payload").extract[Int] should be(1)
  }

  test("decode a msgpack message with binary payload") {
    val msgpackData = Base64.getDecoder.decode("lhPEEK7woe2YvkMLmDP4cDqRKqTEQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAxBBzb21lIGJ5dGVzIQABAgOfxEBs2Nmi5a1gN0E9vHeKI7IGogRKzuQrIHN/EyQYKOXCeIGGrcmEFipr3sB2R+u0GmPmZp+ASRyop1HergptSUcF")
    publishToKafka(new ProducerRecord("fromreceiver", "valid", msgpackData))

    val toVerifierMessages = consumeNumberStringMessagesFrom("toverifier", 1)
    toVerifierMessages.size should be(1)

    val msg = parse(toVerifierMessages.head) \ "ubirchPacket"
    (msg \ "version").extract[Int] should equal(35)
    (msg \ "uuid").extract[String] should equal("aef0a1ed-98be-430b-9833-f8703a912aa4")
    (msg \ "chain").extract[String] should equal("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
    (msg \ "hint").extract[Int] should equal(0)
    (msg \ "signed").extract[String] should equal("lhPEEK7woe2YvkMLmDP4cDqRKqTEQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAxBBzb21lIGJ5dGVzIQABAgOf")
    (msg \ "signature").extract[String] should equal("bNjZouWtYDdBPbx3iiOyBqIESs7kKyBzfxMkGCjlwniBhq3JhBYqa97AdkfrtBpj5mafgEkcqKdR3q4KbUlHBQ==")

    // binary payloads get deserialized as base64 strings
    val p = (msg \ "payload").extract[String]
    p should equal("c29tZSBieXRlcyEAAQIDnw==")
    Base64.getDecoder.decode(p) should equal("some bytes!".getBytes(StandardCharsets.UTF_8) ++ Array[Byte](0, 1, 2, 3, 0x9f.toByte))
  }

  test("send an error message if msgpack decoding fails") {
    val msgpackData = Hex.decodeHex("FF3344".toCharArray)
    publishToKafka(new ProducerRecord("fromreceiver", "broken", msgpackData))

    val toErrorsMessages = consumeNumberStringMessagesFrom("errors", 1)
    toErrorsMessages.size should be(1)

    (parse(toErrorsMessages.head) \ "error").extract[String] should equal("msgpack decoding failed")
  }

  var microservice: MessageDecoderMicroservice = _
  var microserviceControl: DrainingControl[Done] = _

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    microservice = new MessageDecoderMicroservice()
    microserviceControl = microservice.run
  }

  override def afterAll(): Unit = {
    microserviceControl.drainAndShutdown()(microservice.system.dispatcher)
    EmbeddedKafka.stop()
  }
}