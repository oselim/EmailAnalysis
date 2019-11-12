name := "EmailAnalysis"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/javax.mail/javax.mail-api
libraryDependencies += "javax.mail" % "javax.mail-api" % "1.6.2"
// https://mvnrepository.com/artifact/com.sun.mail/javax.mail
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.6.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"