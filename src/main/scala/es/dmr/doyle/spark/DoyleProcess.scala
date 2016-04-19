package es.dmr.doyle.spark

import java.util
import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by hadoop on 8/27/15.
 */
object DoyleProcess {

  private val AppName = "DoyleExtractPlacesStatistics"


  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil) {

    /**
      * Consumes messages from one or more topics in Kafka and does a NLP place extraction and sliding count of
      * occurrences per place.
      *
      * Usage: DoyleJob <zkQuorum> <group> <topics> <numThreads> <topicOut>
      *
      *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
      *   <group> is the name of kafka consumer group
      *   <topics> is a list of one or more kafka topics to consume from
      *   <numThreads> is the number of threads the kafka consumer should use
      *   <topicOut>   the topic to publish to
      */

    if (args.size < 5) {
      println("Usage: DoyleJob  <zkQuorum> <group> <topics> <numThreads> <topicOut>")
      sys.exit(-1)
    }

    val ssc = {
      val sparkConf = new SparkConf().setAppName(AppName).setJars(jars)
      sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
      for (m <- master) {
        sparkConf.setMaster(m)
      }
      new StreamingContext(sparkConf, Seconds(5))
    }

    ssc.checkpoint("doyle_checkpoint")

    val List(zkQuorum, group, topics, numThreads, outTopic) = args

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val extracts = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = extracts.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(10), 2)

    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    wordCounts.foreachRDD(rdd =>
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](props)
        partition.foreach {
          case countPair: Tuple2[String, Long] => {
            producer.send(new ProducerRecord[String, String](outTopic, countPair._1, countPair._2.toString))
          }
        }
      })
    )

    // Not possible to combine kafka writer with checkpointing,
    // someone is having a look in Cloudera
    // https://github.com/cloudera/spark-kafka-writer/issues/5
    // Produce to kafka
    //val producerConf = new Properties()
    //producerConf.put("serializer.class", "kafka.serializer.IntegerEncoder")
    //producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
    //producerConf.put("metadata.broker.list", "localhost:6667")
    //producerConf.put("request.required.acks", "1")
    //wordCounts.writeToKafka[String, Integer](producerConf,
    // (count: Tuple2[String, Long]) => new KeyedMessage(outTopic, count._1, count._2.toInt))

    ssc.start()
    ssc.awaitTermination()
  }
}
