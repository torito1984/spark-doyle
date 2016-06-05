# Playground with Arthur Conan Doyle and Spark

This code is meant to be run in connection with a Kafka producer (see https://github.com/torito1984/kafka-doyle-generator.git).

The code is built with sbt. In this example we show how to extract locations from pieces of text with Standford NLP and combine that with Spark to produce a refined topic from a source topic. A count of the occurrences of different locations in
the passages published by kafka-doyle-generator or any other similar generator is generated directly from the text. This count is published back to Kafa under a different topic. See DoyleLocationsProcess.scala for details. A more simple example is depicted in DoyleProcess.scala, where a simple wordcount every minute and updated every 10 seconds is published.

In order to run the code:

- Run run_streaming.sh for the simple wordcount example. Make sure the topic names are in accordance to your producer!
- Run run_streaming_locations.sh in order to run the location extractor.

The code supposes that a Kafka installation and a producer are available. It has been tested with Kafka 0.9.0.4 in Hortonworks HDP 2.4.0. The expected location of Kafka is localhost:6667
