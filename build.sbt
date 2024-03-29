name := "EmailAnalysis"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"

//resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

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

// one or all from:
libraryDependencies += "ml.sparkling" %% "sparkling-graph-examples" % "0.0.7"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-loaders" % "0.0.7"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.7"

// https://mvnrepository.com/artifact/graphframes/graphframes
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
