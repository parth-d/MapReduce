name := "MapperReducer3"

version := "0.1"

scalaVersion := "3.0.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"