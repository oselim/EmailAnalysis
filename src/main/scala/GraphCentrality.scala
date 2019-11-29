import java.io.{ByteArrayInputStream, File}
import java.util.Properties

import com.centrality.kBC.KBetweenness
import javax.mail.{Address, Session}
import util.FileUtil
import harmonicCentrality.HarmonicCentrality
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component

import scala.io.Source
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._


object GraphCentrality extends App {

  System.setProperty("mail.mime.address.strict", "false")

  val session = Session.getDefaultInstance(new Properties() {
    put("mail.smtp.host", "host")
    put("mail.mime.address.strict", "false")
  })

  private val fileUtil = new FileUtil()
  private val files: Array[File] = fileUtil.recursiveListFiles(new File("enron-sample-dataset"))

  private val vertexArray = new ArrayBuffer[(VertexId, String)]()
  private val edgeArray = new ArrayBuffer[Edge[Int]]()

  def parseAddresses(fromAddresses: Array[Address], receiverAddresses: Array[Address]): Unit = {
    fromAddresses.foreach(from => {
      val fromString = from.toString
      addToVertex(fromString)

      receiverAddresses.foreach(receiver => {
        val receiverString = receiver.toString
        addToVertex(receiverString)
        addToEdge(fromString, receiverString)
      })
    })
  }

  def addToVertex(email: String): Unit = {
    var vID = 0L
    if (!email.equals("NonRecipient"))
      vID = email.hashCode.toLong

    if (!vertexArray.exists(vertex => vertex._1.equals(vID))) {
      vertexArray.append((vID, email))
    }
  }

  def addToEdge(sourceAddress: String, destinationAddress: String): Unit = {
    val sourceID = sourceAddress.hashCode.toLong
    var destinationID = 0L
    if (!destinationAddress.equals("NonRecipient"))
      destinationID = destinationAddress.hashCode.toLong
    edgeArray.append(Edge(sourceID, destinationID, 1))
  }

  files.foreach(file => {
    val source = Source.fromFile(file)
    val fileString: String = source.mkString
    val mimeMessage = new MimeMessage(session, new ByteArrayInputStream(fileString.getBytes))
    val fromAddresses = mimeMessage.getFrom
    var allRecipients = mimeMessage.getAllRecipients
    if (allRecipients == null) {
      allRecipients = Array.apply(new InternetAddress("NonRecipient", false))
    }
    parseAddresses(fromAddresses, allRecipients)

    source.close()
  })

  val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

  private val vertexRDD: RDD[(VertexId, String)] = sc.parallelize(vertexArray)
  private val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

  private val graph = Graph(vertexRDD, edgeRDD)

  println(graph.numEdges)
  println(graph.numVertices)

  println("Closeness Centrality: ")
  private val graphClosenessCentrality: Graph[Double, PartitionID] = sparkSession.time( graph.closenessCentrality() ) //VertexMeasureConfiguration(treatAsUndirected = true))

  println("PageRank: " )
  private val pageRank: Graph[Double, Double] = sparkSession.time(graph.pageRank(0.0001) )

  println("K-Betweenness: ")
  private val kBetweenness: Graph[Double, Double] = sparkSession.time(KBetweenness.run(graph, 2) )

  println("Eigen Vector Centrality: " )
  private val graphEigenVectorCentrality: Graph[Double, PartitionID] = sparkSession.time(graph.eigenvectorCentrality() )

  println("Harmonic Centrality: " )
  private val harmonicCentrality: Graph[Double, PartitionID] = sparkSession.time(HarmonicCentrality.harmonicCentrality(graph) )

  case class OutputClass(id: Long,
                         email: String,
                         inDegree: Option[Int],
                         outDegree: Option[Int],
                         closenessCentrality: Option[Double],
                         pageRank: Option[Double],
                         kBetweennessCentrality: Option[Double],
                         eigenVectorCentrality: Option[Double],
                         harmonicCentrality: Option[Double]
                        )


  private val vertexDF: DataFrame = sparkSession.createDataFrame(
    graph.vertices
      .leftOuterJoin(graph.inDegrees)
      .leftOuterJoin(graph.outDegrees)
      .leftOuterJoin(graphClosenessCentrality.vertices)
      .leftOuterJoin(pageRank.vertices)
      .leftOuterJoin(kBetweenness.vertices)
      .leftOuterJoin(graphEigenVectorCentrality.vertices)
      .leftOuterJoin(harmonicCentrality.vertices)

      .map( {
        case (
          vertexID,
          (
            (
              (
                (
                  (
                    (
                      (email,
                      inDegreeOutput),
                      outDegreeOutput),
                    closenessCent),
                  pageRankValue),
                kBetweennessOutput),
              eigenVectorCent),
            harmonicCentralityOutput)
          )
        =>
          OutputClass(
            vertexID,
            email,
            inDegreeOutput,
            outDegreeOutput,
            closenessCent,
            pageRankValue,
            kBetweennessOutput,
            eigenVectorCent,
            harmonicCentralityOutput
          )
      })).na.fill(0).sort("pageRank", "kBetweennessCentrality")

  vertexDF.show(571, truncate = false)

}
