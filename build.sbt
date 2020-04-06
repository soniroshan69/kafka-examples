name := "kafka-beginner"
version := "1.0"
scalaVersion := "2.11.11"
mainClass in (Compile,packageBin) := Some("practice.kafka.KsWordCount")
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.4.1"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30"