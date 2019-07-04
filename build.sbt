name := "SparksbtDemo"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.3.5"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.3.5"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.3.5"