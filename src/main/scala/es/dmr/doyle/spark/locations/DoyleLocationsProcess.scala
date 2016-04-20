package es.dmr.doyle.spark.locations

import java.util

import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.util.{Triple => NLPTriple}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.JavaConverters._

/**
 * Created by hadoop on 8/27/15.
 */
object DoyleLocationsProcess {

  private val AppName = "DoyleExtractPlacesStatistics"

  // Holds a reference to the NLP classifier
  object ClassifierHolder {
    val DEFAULT_CLASSIFIER = "classifiers/english.all.3class.distsim.crf.ser.gz"
    val LOCATION: String = "LOCATION"

    def classifier() = { CRFClassifier.getClassifier(DEFAULT_CLASSIFIER) }

    private def getElements(text: String, f: NLPTriple[String, Integer, Integer] => Boolean): List[String] = {
      return classifier.classifyToCharacterOffsets(text).asScala.filter(f).map(item => text.substring(item.second(), item.third())).toList
    }

    def getLocations(text: String): List[String] = {
      return getElements(text, _.first().compare(LOCATION)==0)
    }
  }


  def execute(master: Option[String], args: List[String], jars: List[String]) {

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

    // Initialization of Kafka producer
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


    val List(zkQuorum, group, topics, numThreads, outTopic) = args

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val extracts = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val locations = extracts.flatMap(ClassifierHolder.getLocations(_))
    val locationCounts = locations.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(10), 2)

    locationCounts.foreachRDD(rdd =>
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](props)
        partition.foreach {
          case countPair: Tuple2[String, Long] => {
            producer.send(new ProducerRecord[String, String](outTopic, countPair._1,
              "{'place':" + countPair._1 + ", 'occurences':" + countPair._2.toString + "}"))
          }
        }
      })
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
