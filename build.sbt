name := "MapperReducer3"

version := "0.1"

scalaVersion := "3.0.2"

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "com.typesafe" % "config" % "1.4.1"
)