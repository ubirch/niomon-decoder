package com.ubirch.messagedecoder

import akka.Done
import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.Keep
import org.apache.kafka.clients.producer.ProducerRecord

object Main {

  def main(args: Array[String]) {
    def transformValue(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]) = {
      new String(msg.record.value()).toUpperCase.getBytes
    }

    val control: DrainingControl[Done] =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(incomingTopic))
        .map { msg =>
          // ToDo BjB 19.09.18 : actual transformation should happen somewhere here
          val transformed = transformValue(msg)
          ProducerMessage.Message[String, Array[Byte], ConsumerMessage.CommittableOffset](
            new ProducerRecord(outgoingTopic, transformed),
            msg.committableOffset
          )
        }
        .toMat(Producer.commitableSink(producerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
  }
}
