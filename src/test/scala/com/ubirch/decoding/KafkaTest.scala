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

package com.ubirch.decoding

import java.nio.charset.StandardCharsets
import java.util.{Base64, UUID}

import com.typesafe.config.ConfigFactory
import com.ubirch.kafka.{MessageEnvelope, RichAnyProducerRecord}
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceMock}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

//noinspection TypeAnnotation
class KafkaTest extends FunSuite with Matchers with BeforeAndAfterAll {

  val decodingSystem = (ns: NioMicroservice[Array[Byte], MessageEnvelope]) => new MessageDecodingMicroservice(_ => null, ns) {
    override def verify: Verify = (record: ConsumerRecord[String, Array[Byte]]) => record
  }

  private val microservice = NioMicroserviceMock[Array[Byte], MessageEnvelope](x => decodingSystem(x))
  microservice.name = "niomon-decoder"
  microservice.outputTopics = Map("valid" -> "toverifier")
  microservice.errorTopic = Some("errors")
  microservice.config = ConfigFactory.load().getConfig("niomon-decoder")

  import microservice.kafkaMocks._

  implicit val formats = DefaultFormats
  implicit val bytesSerializer = new ByteArraySerializer

  test("decode a simple json message") {

    val requestId = UUID.randomUUID()
    val hardwareId = "d9ea390e-b202-4ff6-92fb-8ca9142426ea"
    val binaryMessage = "{\"version\":18, \"uuid\":\"d9ea390e-b202-4ff6-92fb-8ca9142426ea\", \"payload\":1}".getBytes(StandardCharsets.UTF_8)

    publishToKafka(new ProducerRecord("fromreceiver", requestId.toString, binaryMessage)
      .withExtraHeaders("x-ubirch-hardware-id" -> hardwareId))

    val toVerifierMessages = consumeNumberStringMessagesFrom("toverifier", 1)

    toVerifierMessages.size should be(1)

    val msg = parse(toVerifierMessages.head) \ "ubirchPacket"
    (msg \ "signed").extractOpt should be(None)
    (msg \ "version").extract[Int] should be(34)
    (msg \ "hint").extract[Int] should be(0)
  }

  test("send an error message if json decoding fails") {
    val hardwareId = "6eac4d0b-16e6-4508-8c46-22e7451ea5a1"
    val binaryMessage = "{broken}".getBytes(StandardCharsets.UTF_8)
    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", binaryMessage)
      .withExtraHeaders("x-ubirch-hardware-id" -> hardwareId)
      .withRequestIdHeader()("broken"))

    val toErrorsMessages = consumeNumberStringMessagesFrom("errors", 1)

    toErrorsMessages.size should be(1)

    toErrorsMessages.head should equal("""{"error":"ProtocolException: extraction of signed data failed","causes":["JsonParseException: Unexpected character ('b' (code 98)): was expecting double-quote to start field name\n at [Source: (String)\"{broken}\"; line: 1, column: 3]"],"microservice":"niomon-decoder","requestId":"broken"}""")
  }

  test("decode a simple msgpack message") {
    val hardwareId = "6eac4d0b-16e6-4508-8c46-22e7451ea5a1"
    val msgpackData = Base64.getDecoder.decode("lRKwbqxNCxbmRQiMRiLnRR6loczvAdoAQFeKWyLOs+HQ0PiUfAmAEBM7RNOx0qs5h1j/7RFQe2B+03274Ab2RfDtD9vrG0i7UP1x2DI0DOAk1aDiHA68jg4=")
    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", msgpackData)
      .withExtraHeaders("x-ubirch-hardware-id" -> hardwareId)
      .withRequestIdHeader()("valid"))

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
    val hardwareId = "aef0a1ed-98be-430b-9833-f8703a912aa4"
    val msgpackData = Base64.getDecoder.decode("lhPEEK7woe2YvkMLmDP4cDqRKqTEQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAxBBzb21lIGJ5dGVzIQABAgOfxEBs2Nmi5a1gN0E9vHeKI7IGogRKzuQrIHN/EyQYKOXCeIGGrcmEFipr3sB2R+u0GmPmZp+ASRyop1HergptSUcF")
    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", msgpackData)
      .withExtraHeaders("x-ubirch-hardware-id" -> hardwareId)
      .withRequestIdHeader()("valid"))

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
    val msgpackData = Base64.getDecoder.decode("/zNE")
    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", msgpackData).withRequestIdHeader()("broken"))

    val toErrorsMessages = consumeNumberStringMessagesFrom("errors", 1)
    toErrorsMessages.size should be(1)

    toErrorsMessages.head should equal(
      """{"error":"ProtocolException: msgpack decoding failed","causes":["MessageTypeException: Expected Array, but got Integer (ff)"],"microservice":"niomon-decoder","requestId":"broken"}""")
  }

  test("fail when no hardware id header is provided") {

    val binaryMessage = "{\"version\":18, \"uuid\":\"d9ea390e-b202-4ff6-92fb-8ca9142426ea\", \"payload\":1}".getBytes(StandardCharsets.UTF_8)

    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", binaryMessage).withRequestIdHeader()("error"))

    val toErrorMessages = consumeNumberStringMessagesFrom("errors", 1)
    toErrorMessages.size should be(1)

    toErrorMessages.head should equal(
      """{"error":"NoSuchElementException: x-ubirch-hardware-id not found in headers","causes":[],"microservice":"niomon-decoder","requestId":"error"}""")

  }

  test("fail when no hardware id header is different to the message uuid") {

    val binaryMessage = "{\"version\":18, \"uuid\":\"d9ea390e-b202-4ff6-92fb-8ca9142426ea\", \"payload\":1}".getBytes(StandardCharsets.UTF_8)

    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", binaryMessage)
      .withExtraHeaders("x-ubirch-hardware-id" -> "d9ea390e-b202-4ff6-92fb-8ca9142426eb")
      .withRequestIdHeader()("error"))

    val toErrorMessages = consumeNumberStringMessagesFrom("errors", 1)
    toErrorMessages.size should be(1)

    toErrorMessages.head should equal(
      """{"error":"IllegalArgumentException: Header UUID does not match protocol message UUID","causes":[],"microservice":"niomon-decoder","requestId":"error"}""")

  }

  test("fail when no payload is provided") {

    val binaryMessage = "{\"version\":18, \"uuid\":\"d9ea390e-b202-4ff6-92fb-8ca9142426ea\"}".getBytes(StandardCharsets.UTF_8)

    publishToKafka(new ProducerRecord[String, Array[Byte]]("fromreceiver", binaryMessage)
      .withExtraHeaders("x-ubirch-hardware-id" -> "d9ea390e-b202-4ff6-92fb-8ca9142426ea")
      .withRequestIdHeader()("error")
    )

    val toErrorMessages = consumeNumberStringMessagesFrom("errors", 1)
    toErrorMessages.size should be(1)

    toErrorMessages.head should equal(
      """{"error":"ProtocolException: Protocol message payload is null","causes":[],"microservice":"niomon-decoder","requestId":"error"}""")

  }

}
