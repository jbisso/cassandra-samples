name := "vdb-server"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.1"
)     

play.Project.playJavaSettings

