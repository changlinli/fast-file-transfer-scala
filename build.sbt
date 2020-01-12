name := "raptorq-demo"

organization := "com.changlinli"

version := "0.1.0"

scalaVersion := "2.13.1"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"
libraryDependencies += "co.fs2" %% "fs2-core" % "2.1.0"
libraryDependencies += "co.fs2" %% "fs2-io" % "2.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0"

assemblyMergeStrategy in assembly := {
  // For now we're not going to use anything JDK-9 related
  case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

Test / fork := true
Test / javaOptions += "-Xmx16G"
Compile / run / fork := true
connectInput in run := true
