name := "hss-collector"

version := "1.0"

scalaVersion := "2.11.8"
libraryDependencies ++= {
  val akkaV = "2.4.8"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-remote" % akkaV,
    "com.typesafe.akka" %% "akka-cluster" % akkaV,
    "com.typesafe.akka" %% "akka-cluster-metrics" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.8.8",
    "org.apache.kafka" %% "kafka" % "0.9.0.1",
    "org.scala-lang.modules" %% "scala-async" % "0.9.5",
    "redis.clients" % "jedis" % "2.8.1",
    "commons-net" % "commons-net" % "3.5",
    "org.apache.commons" % "commons-csv" % "1.4",
		"com.twitter" %% "chill-bijection" % "0.8.0",
		"com.esotericsoftware" % "kryo" % "3.0.3",
    "com.github.romix.akka" %% "akka-kryo-serialization" % "0.4.0",
    "joda-time" % "joda-time" % "2.9.4",
    "org.joda" % "joda-convert" % "1.8.1",
    "commons-cli" % "commons-cli" % "1.3.1",
    "org.elasticsearch" % "elasticsearch" % "2.3.1"
  )
}

libraryDependencies += "com.lihaoyi" % "ammonite_2.11.8" % "0.7.4"
initialCommands in console := """ammonite.Main().run()"""

//sbt使用代理
//javaOptions in console ++= Seq(
//  "-Dhttp.proxyHost=cmproxy-sgs.gmcc.net",
//  "-Dhttp.proxyPort=8081"
//)
//javaOptions in run ++= Seq(
//  "-Dhttp.proxyHost=cmproxy-sgs.gmcc.net",
//  "-Dhttp.proxyPort=8081"
//)
