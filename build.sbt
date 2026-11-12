name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.2.1"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.27"
val nettyVersion = "4.1.68.Final"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core" % sparkVersion,
  "org.apache.spark"        %% "spark-sql"  % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api"  % "2.12.4",
  "org.apache.logging.log4j" % "log4j-core" % "2.12.4",
  // postgres for DB connectivity
  "org.postgresql"           % "postgresql" % postgresVersion
)

// Ensure Spark's Netty version takes precedence
dependencyOverrides ++= Seq(
  "io.netty" % "netty-all" % nettyVersion,
  "io.netty" % "netty-buffer" % nettyVersion,
  "io.netty" % "netty-common" % nettyVersion,
  "io.netty" % "netty-handler" % nettyVersion,
  "io.netty" % "netty-transport" % nettyVersion,
  "io.netty" % "netty-transport-native-epoll" % nettyVersion,
  "io.netty" % "netty-transport-native-unix-common" % nettyVersion
)
