name := "raptorq-demo"

organization := "com.changlinli"

version := "0.1.0"

scalaVersion := "2.13.1"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"
libraryDependencies += "co.fs2" %% "fs2-core" % "2.1.0"
libraryDependencies += "co.fs2" %% "fs2-io" % "2.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.scalatestplus" %% "scalacheck-1-14" % "3.1.0.1" % "test"
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0"
libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "1.3.4"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6"

assemblyMergeStrategy in assembly := {
  // For now we're not going to use anything JDK-9 related
  case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

Test / fork := true
Test / javaOptions += "-Xmx6G"
Test / javaOptions += "-Dorg.slf4j.simpleLogger.defaultLogLevel=info"
Test / javaOptions += "-Xlog:gc*:stdout:tid,time,uptime"
Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
Compile / run / fork := true
Compile / run / javaOptions += "-Xmx6G"
Compile / run / javaOptions += "-Xms6G"
Compile / run / javaOptions += "-Xlog:gc*:stdout:tid,time,uptime"
Compile / run / javaOptions += "-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
Compile / run / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"
connectInput in run := true
