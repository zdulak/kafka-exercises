import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import scala.jdk.CollectionConverters._

object WordCountApp extends  App {
  val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("group.id", "inputs")

  val consumer = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(List("input").asJava)

  val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](producerProps)

  import scala.concurrent.duration.DurationInt
  import scala.jdk.DurationConverters._

  while (true) {
    val records = consumer.poll(100.millisecond.toJava).asScala
    records.foreach { record =>
      val wordsOccurrences = record.value.split(" ")
        .groupBy(identity).view.mapValues(_.length).toSeq
//    Alternative solution
//        .foldLeft(Map.empty[String, Int]) {
//          case (occ, word) => occ + (word -> (occ.getOrElse(word, 0) + 1))
//        }.toSeq
      wordsOccurrences.foreach {
        case (key, value) => producer.send(new ProducerRecord[String, String]("output", key, value.toString))
      }
    }
  }
}
