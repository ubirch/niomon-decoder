package com.ubirch.messagedecoder

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Base64

import akka.stream.UniqueKillSwitch
import cakesolutions.kafka.testkit.KafkaServer
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.ubirch.kafkasupport.MessageEnvelope
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{BytesDeserializer, BytesSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

//noinspection TypeAnnotation
class KafkaTest extends FunSuite with Matchers with BeforeAndAfterAll {

  test("decode a simple json message") {
    val binaryMessage = Bytes.wrap("{\"version\":18}".getBytes(StandardCharsets.UTF_8))
    producer.send(MessageEnvelope.toRecord("fromreceiver", "valid", MessageEnvelope(binaryMessage)))

    val toVerifierRecords = decodedConsumer.poll(Duration.ofSeconds(10))
    toVerifierRecords.count() should be(1)

    val toVerifierMessages = toVerifierRecords.iterator()
    val decodedMessage = MessageEnvelope.fromRecord(toVerifierMessages.next())
    val msg = parse(decodedMessage.payload)
    (msg \ "signed").extractOpt should be(None)
    (msg \ "version").extract[Int] should be(18)
    (msg \ "hint").extract[Int] should be(0)
  }

  test("send an error message if json decoding fails") {
    val binaryMessage = Bytes.wrap("{broken}".getBytes(StandardCharsets.UTF_8))
    producer.send(MessageEnvelope.toRecord("fromreceiver", "broken", MessageEnvelope(binaryMessage)))
    errorsConsumer.subscribe(List("errors").asJava)

    val toErrorsRecords = errorsConsumer.poll(Duration.ofSeconds(10))
    toErrorsRecords.count() should be(1)

    val errorMessages = toErrorsRecords.iterator()
    val errorMessage = MessageEnvelope.fromRecord(errorMessages.next())
    (parse(errorMessage.payload) \ "error").extract[String] should equal("extraction of signed data failed")
  }

  test("decode a simple msgpack message") {
    val msgpackData = Hex.decodeHex("9512b06eac4d0b16e645088c4622e7451ea5a1ccef01da0040578a5b22ceb3e1d0d0f8947c098010133b44d3b1d2ab398758ffed11507b607ed37dbbe006f645f0ed0fdbeb1b48bb50fd71d832340ce024d5a0e21c0ebc8e0e".toCharArray)
    val msgEnvelope = MessageEnvelope(Bytes.wrap(msgpackData))
    producer.send(MessageEnvelope.toRecord("fromreceiver", "valid", msgEnvelope))

    val toVerifierRecords = decodedConsumer.poll(Duration.ofSeconds(10))
    toVerifierRecords.count() should be(1)

    val toVerifierMessages = toVerifierRecords.iterator()
    val decodedMessage = MessageEnvelope.fromRecord(toVerifierMessages.next())
    val msg = parse(decodedMessage.payload)
    (msg \ "signed").extract[String] should equal("lRKwbqxNCxbmRQiMRiLnRR6loczvAQ==")
    (msg \ "version").extract[Int] should be(18)
    (msg \ "hint").extract[Int] should be(0xEF)
    (msg \ "uuid").extract[String] should equal("6eac4d0b-16e6-4508-8c46-22e7451ea5a1")
    (msg \ "payload").extract[Int] should be(1)
  }

  test("decode a msgpack message with binary payload") {
    val msgpackData = Base64.getDecoder.decode("lhPEEK7woe2YvkMLmDP4cDqRKqTEQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAxBBzb21lIGJ5dGVzIQABAgOfxEBs2Nmi5a1gN0E9vHeKI7IGogRKzuQrIHN/EyQYKOXCeIGGrcmEFipr3sB2R+u0GmPmZp+ASRyop1HergptSUcF")
    val msgEnvelope = MessageEnvelope(Bytes.wrap(msgpackData))
    producer.send(MessageEnvelope.toRecord("fromreceiver", "valid", msgEnvelope))

    val toVerifierRecords = decodedConsumer.poll(Duration.ofSeconds(10))
    toVerifierRecords.count() should be(1)

    val toVerifierMessages = toVerifierRecords.iterator()
    val decodedMessage = MessageEnvelope.fromRecord(toVerifierMessages.next())
    val msg = parse(decodedMessage.payload)
    (msg \ "version").extract[Int] should equal (19)
    (msg \ "uuid").extract[String] should equal ("aef0a1ed-98be-430b-9833-f8703a912aa4")
    (msg \ "chain").extract[String] should equal ("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
    (msg \ "hint").extract[Int] should equal (0)
    (msg \ "signed").extract[String] should equal ("lhPEEK7woe2YvkMLmDP4cDqRKqTEQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAxBBzb21lIGJ5dGVzIQABAgM=")
    (msg \ "signature").extract[String] should equal ("bNjZouWtYDdBPbx3iiOyBqIESs7kKyBzfxMkGCjlwniBhq3JhBYqa97AdkfrtBpj5mafgEkcqKdR3q4KbUlHBQ==")

    // binary payloads get deserialized as base64 strings
    val p = (msg \ "payload").extract[String]
    p should equal ("c29tZSBieXRlcyEAAQIDnw==")
    Base64.getDecoder.decode(p) should equal ("some bytes!".getBytes(StandardCharsets.UTF_8) ++ Array[Byte](0, 1, 2, 3, 0x9f.toByte))
  }

  test("send an error message if msgpack decoding fails") {
    val msgpackData = Hex.decodeHex("FF3344".toCharArray)
    val msgEnvelope = MessageEnvelope(Bytes.wrap(msgpackData))
    producer.send(MessageEnvelope.toRecord("fromreceiver", "broken", msgEnvelope))
    errorsConsumer.subscribe(List("errors").asJava)

    val toErrorsRecords = errorsConsumer.poll(Duration.ofSeconds(10))
    toErrorsRecords.count() should be(1)

    val errorMessages = toErrorsRecords.iterator()
    val errorMessage = MessageEnvelope.fromRecord(errorMessages.next())
    (parse(errorMessage.payload) \ "error").extract[String] should equal("msgpack decoding failed")
  }

  val kafkaServer = new KafkaServer(9892)
  val producer: KafkaProducer[String, Bytes] = createBytesProducer(kafkaServer.kafkaPort)
  val decodedConsumer = createStringConsumer(kafkaServer.kafkaPort, "1")
  val errorsConsumer = createStringConsumer(kafkaServer.kafkaPort, "2")
  var stream: UniqueKillSwitch = _


  override def beforeAll(): Unit = {
    kafkaServer.startup()
    createTopics("fromreceiver", "toverifier", "errors")
    stream = decoderStream.run()
    decodedConsumer.subscribe(List("toverifier").asJava)
    errorsConsumer.subscribe(List("errors").asJava)
  }

  override def afterAll(): Unit = {
    stream.shutdown()
    producer.close()
    decodedConsumer.close()
    errorsConsumer.close()
    kafkaServer.close()
  }


  private def createStringConsumer(kafkaPort: Int, groupId: String) = {
    KafkaConsumer(
      KafkaConsumer.Conf(new StringDeserializer(), new StringDeserializer(),
        bootstrapServers = s"localhost:$kafkaPort",
        groupId = groupId,
        autoOffsetReset = OffsetResetStrategy.EARLIEST)
    )
  }

  private def createBytesConsumer(kafkaPort: Int, groupId: String) = {
    KafkaConsumer(
      KafkaConsumer.Conf(new StringDeserializer(), new BytesDeserializer(),
        bootstrapServers = s"localhost:$kafkaPort",
        groupId = groupId,
        autoOffsetReset = OffsetResetStrategy.EARLIEST)
    )
  }

  private def createStringProducer(kafkaPort: Int) = {
    KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(), new StringSerializer(),
        bootstrapServers = s"localhost:$kafkaPort",
        acks = "all"))
  }

  private def createBytesProducer(kafkaPort: Int) = {
    KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(), new BytesSerializer(),
        bootstrapServers = s"localhost:$kafkaPort",
        acks = "all"))
  }

  def createTopics(topicName: String*): Unit = {
    val adminClient = createAdmin(kafkaServer.kafkaPort)
    val topics = topicName.map(new NewTopic(_, 1, 1))
    val createTopicsResult = adminClient.createTopics(topics.toList.asJava)
    // finish futures
    topicName.foreach(t => createTopicsResult.values.get(t).get())
    adminClient.close()
  }

  private def createAdmin(kafkaPort: Int) = {
    val configMap = Map[String, AnyRef](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$kafkaPort",
      AdminClientConfig.CLIENT_ID_CONFIG -> "admin",
    )
    AdminClient.create(configMap.asJava)
  }

}