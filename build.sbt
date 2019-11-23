name := "EmailAnalysis"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"

// https://mvnrepository.com/artifact/javax.mail/javax.mail-api
libraryDependencies += "javax.mail" % "javax.mail-api" % "1.6.2"
// https://mvnrepository.com/artifact/com.sun.mail/javax.mail
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.6.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// https://mvnrepository.com/artifact/dmarcous/spark-betweenness
libraryDependencies += "dmarcous" % "spark-betweenness" % "1.0-s_2.10"

// https://mvnrepository.com/artifact/com.twitter/algebird-core
libraryDependencies += "com.twitter" %% "algebird-core" % "0.13.6"
