name := "spark-streaming-doyle"

version := "1.0"

//scalaVersion := "2.10.4"

resolvers +=
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.2",
  "org.cloudera.spark.streaming.kafka" % "spark-kafka-writer" % "0.1.0" intransitive()
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
    case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
    case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
    case PathList("org", "apache", "spark", "unused", xs @ _*) => MergeStrategy.last // For Log$Logger.class
    case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
}
