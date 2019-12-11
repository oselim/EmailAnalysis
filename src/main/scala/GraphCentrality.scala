import java.io.{ByteArrayInputStream, File}
import java.nio.charset.CodingErrorAction
import java.util.Properties

import com.centrality.kBC.KBetweenness
import javax.mail.{Address, Session}
import util.SerializableMimeMessage
import harmonicCentrality.HarmonicCentrality
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import org.apache.spark.sql.Column
import java.util.concurrent.TimeUnit

import scala.io.{Codec, Source}
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.log4j.{Level, Logger}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame


object GraphCentrality extends App {

  val preprocessStartTime: Long = System.nanoTime()
  val programStartTime = System.nanoTime()

  val cpu = 8
  //val datasetDir = "C:\\Users\\Selim-admin\\IdeaProjects\\EmailAnalysis-sparking\\enron-sample-dataset"
  val datasetDir = "S:\\6410-proposal\\maildir"
  val checkPointDir = "S:\\6410-proposal\\checkPointing"
  val applicationName = "LocalAllData"
  val outputDir = "S:\\6410-proposal\\LocalResearchOutputs\\GraphCentrality\\" + applicationName + "_cpu_"
  val unknownRecipient = "NonRecipient"

  // System.setProperty("hadoop.home.dir", "C:\\Users\\Selim-admin\\IdeaProjects\\EmailAnalysis-sparking\\null\\bin")

  val conf = new SparkConf().setAppName(applicationName).setMaster("local[" + cpu + "]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.TRACE)
  val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

  sc.setCheckpointDir(checkPointDir)

  println("Spark Started.")

  System.setProperty("mail.mime.address.strict", "false")

  @transient
  val session = Session.getDefaultInstance(new Properties() {
    put("mail.smtp.host", "host")
    put("mail.mime.address.strict", "false")
  })

  println("Program Started.")

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    val files = these.filter(_.isFile)
    files ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  val files: Array[File] = recursiveListFiles(new File(datasetDir))
  println("Files Count: " + files.length)
  println("File List created.")
  println("File read Starting.")

  val filesRDD: RDD[String] = sc.parallelize(files.map(_.getAbsolutePath), cpu)

  val mimeMessageRDD: RDD[SerializableMimeMessage] = filesRDD.map(file => {
    implicit val codec: Codec = Codec.ISO8859
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val source = Source.fromFile(file)
    val fileString: String = source.mkString
    new SerializableMimeMessage(new MimeMessage(session, new ByteArrayInputStream(fileString.getBytes)))
  })

  val fromAndRecipientsArrayTuples: RDD[(Array[Address], Array[Address])] = mimeMessageRDD.map(serializableMimeMessage => {
    val fromAddresses = serializableMimeMessage.getMimeMessage.getFrom
/*    if (fromAddresses == null ){
      fromAddresses = Array.apply(new InternetAddress(unknownRecipient, false))
    }*/
    var allRecipients = serializableMimeMessage.getMimeMessage.getAllRecipients
    if (allRecipients == null) {
      allRecipients = Array.apply(new InternetAddress(unknownRecipient, false))
    }
    (fromAddresses, allRecipients)
  })

  val fromAndRecipientsTuples: RDD[(String, String)] = fromAndRecipientsArrayTuples.flatMap(x => {
    x._1.flatMap(fromAddress => {
      x._2.map(receiverAddress => {
        (fromAddress.toString, receiverAddress.toString)
      })
    })
  })

  val vertexFromRDD: RDD[(VertexId, String)] = fromAndRecipientsTuples.map(x => {
    val fromString = x._1
    (fromString.hashCode.toLong, fromString)
  })

  val vertexReceiverRDD: RDD[(VertexId, String)] = fromAndRecipientsTuples.map(x => {
    val receiverString = x._2
    var vertexID = 0L
    if (!receiverString.equals(unknownRecipient))
      vertexID = receiverString.hashCode.toLong
    (vertexID, receiverString)
  })

  val vertexRDD: RDD[(VertexId, String)] = vertexFromRDD.union(vertexReceiverRDD).distinct()

  val edgeRDD: RDD[Edge[Int]] = fromAndRecipientsTuples.map(x => {
    val fromString = x._1
    val receiverString = x._2
    var destinationID = 0L
    if (!receiverString.equals(unknownRecipient))
      destinationID = receiverString.hashCode.toLong
    new Edge[Int](fromString.hashCode.toLong, destinationID, 1)
  })
  println("Edge Count: " + edgeRDD.count())
  println("Vertex Count: " + vertexRDD.count())
  println("Preprocess finished. File Read finished.")

  val preprocessTime: Long = System.nanoTime() - preprocessStartTime
  val graphBuildingStartTime = System.nanoTime()

  val graph = Graph(vertexRDD, edgeRDD)
  val graphFrame: GraphFrame = GraphFrame.fromGraphX(graph)

  println("Graphs created.")
  println("Algorithms running.")

  val graphBuildingTime = System.nanoTime() - graphBuildingStartTime

  /*
  println("Closeness Centrality: ")
  val closenessCentralityStartTime: Long = System.nanoTime()
  val graphClosenessCentrality: Graph[Double, PartitionID] = sparkSession.time( graph.closenessCentrality() ) //VertexMeasureConfiguration(treatAsUndirected = true))
  val closenessCentralityTime: Long = System.nanoTime() - closenessCentralityStartTime
*/
  println("PageRank: " )
  val pageRankStartTime: Long = System.nanoTime()
  val pageRank: Graph[Double, Double] = sparkSession.time(graph.pageRank(0.0001) )
  val pageRankTime: Long = System.nanoTime() - pageRankStartTime
/*
  println("K-Betweenness: ")
  val KBetweennessStartTime: Long = System.nanoTime()
  val kBetweenness: Graph[Double, Double] = sparkSession.time(KBetweenness.run(graph, 2) )
  val kBetweennessTime: Long = System.nanoTime() - KBetweennessStartTime
*/
  println("Eigen Vector Centrality: " )
  val eigenVectorCentralityStartTime: Long = System.nanoTime()
  val graphEigenVectorCentrality: Graph[Double, PartitionID] = sparkSession.time(graph.eigenvectorCentrality() )
  val eigenVectorCentralityTime: Long = System.nanoTime() - eigenVectorCentralityStartTime

  println("Harmonic Centrality: " )
  val harmonicCentralityStartTime: Long = System.nanoTime()
  val harmonicCentrality: Graph[Double, PartitionID] = sparkSession.time(HarmonicCentrality.harmonicCentrality(graph) )
  val harmonicCentralityTime: Long = System.nanoTime() - harmonicCentralityStartTime

  println("Algorithms finished.")
  val outputStartTime: Long = System.nanoTime()


  case class OutputClass(id: Long,
                         email: String,
                         inDegree: Option[Int],
                         outDegree: Option[Int],
                         //closenessCentrality: Option[Double],
                         pageRank: Option[Double],
                         //kBetweennessCentrality: Option[Double],
                         eigenVectorCentrality: Option[Double],
                         harmonicCentrality: Option[Double]
                        )


  val vertexDF: DataFrame = sparkSession.createDataFrame(
    graph.vertices
      .leftOuterJoin(graph.inDegrees)
      .leftOuterJoin(graph.outDegrees)
      //.leftOuterJoin(graphClosenessCentrality.vertices)
      .leftOuterJoin(pageRank.vertices)
      //.leftOuterJoin(kBetweenness.vertices)
      .leftOuterJoin(graphEigenVectorCentrality.vertices)
      .leftOuterJoin(harmonicCentrality.vertices)

      .map( {
        case (
          vertexID,
          (
            (
              (
                //(
                  //(
                    (
                      (email,
                      inDegreeOutput),
                      outDegreeOutput),
                    //closenessCent),
                  pageRankValue),
              //  kBetweennessOutput),
              eigenVectorCent),
            harmonicCentralityOutput)
          )
        =>
          OutputClass(
            vertexID,
            email,
            inDegreeOutput,
            outDegreeOutput,
            //closenessCent,
            pageRankValue,
            //kBetweennessOutput,
            eigenVectorCent,
            harmonicCentralityOutput
          )
      })).na.fill(0).sort("pageRank")

  //vertexDF.show(571, truncate = false)

  val outputTime: Long = System.nanoTime() - outputStartTime
  val programTime: Long = System.nanoTime() - programStartTime


  def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))

  vertexDF.repartition(1)
    .withColumn("preprocessTime", lit(TimeUnit.MILLISECONDS.convert(preprocessTime, TimeUnit.NANOSECONDS)))
    .withColumn("graphBuildingTime", lit(TimeUnit.MILLISECONDS.convert(graphBuildingTime, TimeUnit.NANOSECONDS)))
    //.withColumn("closenessCentralityTime", lit(TimeUnit.MILLISECONDS.convert(closenessCentralityTime, TimeUnit.NANOSECONDS)))
    .withColumn("pageRankTime", lit(TimeUnit.MILLISECONDS.convert(pageRankTime, TimeUnit.NANOSECONDS)))
    //.withColumn("kBetweennessTime", lit(TimeUnit.MILLISECONDS.convert(kBetweennessTime, TimeUnit.NANOSECONDS)))
    .withColumn("eigenVectorCentralityTime", lit(TimeUnit.MILLISECONDS.convert(eigenVectorCentralityTime, TimeUnit.NANOSECONDS)))
    .withColumn("harmonicCentralityTime", lit(TimeUnit.MILLISECONDS.convert(harmonicCentralityTime, TimeUnit.NANOSECONDS)))
    .withColumn("outputTime", lit(TimeUnit.MILLISECONDS.convert(outputTime, TimeUnit.NANOSECONDS)))
    .withColumn("programTime", lit(TimeUnit.MILLISECONDS.convert(programTime, TimeUnit.NANOSECONDS)))
    .withColumn("NumberOfFiles", lit(files.length))
    .withColumn("NumberOfVertices", lit(graph.numVertices))
    .withColumn("NumberOfEdges", lit(graph.numEdges))
    .withColumn("SparkContextConf", lit(sc.getConf.getAll.deep.toString()))
    .write.option("header", value = true).csv(outputDir + cpu)


}
