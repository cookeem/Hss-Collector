name := "hss-collector"

version := "1.0"

scalaVersion := "2.11.8"
libraryDependencies ++= {
  val akkaV = "2.4.16"
  val akkaHttpV = "10.0.3"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-remote" % akkaV,
    "com.typesafe.akka" %% "akka-cluster" % akkaV,
    "com.typesafe.akka" %% "akka-cluster-metrics" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
//    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.8.8",
    //必须exclude log4j以及slf4j，否则会直接throw冲突异常
    "org.apache.kafka" %% "kafka" % "0.10.1.1" exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"),
    //必须把log4j-api和log4j-to-slf4j加进来，并且版本必须为2.6.2
    "org.apache.logging.log4j" % "log4j-api" % "2.6.2",
    "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.6.2",
    "org.scala-lang.modules" %% "scala-async" % "0.9.5",
    "redis.clients" % "jedis" % "2.9.0",
    "commons-net" % "commons-net" % "3.5",
    "org.apache.commons" % "commons-csv" % "1.4",
		"com.twitter" %% "chill-bijection" % "0.8.0",
		"com.esotericsoftware" % "kryo" % "3.0.3",
    "com.github.romix.akka" %% "akka-kryo-serialization" % "0.4.0",
    "joda-time" % "joda-time" % "2.9.4",
    "org.joda" % "joda-convert" % "1.8.1",
    "commons-cli" % "commons-cli" % "1.3.1",
    "org.elasticsearch.client" % "transport" % "5.2.0",
    "com.typesafe.play" %% "play-json" % "2.5.12"
  )
}

//libraryDependencies += "com.lihaoyi" % "ammonite_2.11.8" % "0.7.4"
//initialCommands in console := """ammonite.Main().run()"""

//sbt使用代理
//javaOptions in console ++= Seq(
//  "-Dhttp.proxyHost=cmproxy-sgs.gmcc.net",
//  "-Dhttp.proxyPort=8081"
//)
//javaOptions in run ++= Seq(
//  "-Dhttp.proxyHost=cmproxy-sgs.gmcc.net",
//  "-Dhttp.proxyPort=8081"
//)
