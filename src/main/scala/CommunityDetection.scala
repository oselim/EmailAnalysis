import java.io.{ByteArrayInputStream, File}
import java.nio.charset.CodingErrorAction
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.io.Codec
import GraphCentrality.sparkSession
import javax.mail.{Address, Session}
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import util.FileUtil

import scala.io.Source
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object CommunityDetection extends App {

  private var t0: Long = System.nanoTime()

  System.setProperty("mail.mime.address.strict", "false")

  implicit val codec: Codec = Codec.ISO8859
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  val session = Session.getDefaultInstance(new Properties() {
    put("mail.smtp.host", "host")
    put("mail.mime.address.strict", "false")
  })

  private val fileUtil = new FileUtil()
  private val files: Array[File] = fileUtil.recursiveListFiles(new File("S:\\6410-proposal\\maildir"))

  private val vertexArray =
    new ArrayBuffer[(VertexId, String)]()
  private val edgeArray =
    new ArrayBuffer[Edge[Int]]()

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

  sc.setCheckpointDir("S:\\6410-proposal\\checkPointing\\")

  private var t1: Long = System.nanoTime()
  println("Pre-processing took Elapsed time of " + (t1 - t0) + " nanoseconds;  " + TimeUnit.MILLISECONDS.convert((t1 - t0), TimeUnit.NANOSECONDS) + " Millis")

  t0 = System.nanoTime()

  private val vertexRDD: RDD[(VertexId, String)] = sc.parallelize(vertexArray)
  private val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

  private val graph = Graph(vertexRDD, edgeRDD)
  private val graphFrame: GraphFrame = GraphFrame.fromGraphX(graph)

  t1 = System.nanoTime()
  println("Graph building took Elapsed time of " + (t1 - t0) + " nanoseconds;  " + TimeUnit.MILLISECONDS.convert((t1 - t0), TimeUnit.NANOSECONDS) + " Millis")

  println(graph.numEdges)
  println(graph.numVertices)

  println("Triangle Count: ")
  private val triangleCounts: Graph[PartitionID, Int] = sparkSession.time(graph.triangleCount())

  println("Connected Components: ")
  private val connectedComponents: Graph[VertexId, Int] = sparkSession.time(graph.connectedComponents())

  println("Strongly Connected Components: ")
  private val stronglyConnectedComponents: Graph[VertexId, PartitionID] = sparkSession.time(graph.stronglyConnectedComponents(10))

  println("In Neighbor IDs: ")
  private val inNeighbors: VertexRDD[Array[VertexId]] = sparkSession.time(graph.collectNeighborIds(EdgeDirection.In))

  println("Out Neighbor IDs: ")
  private val outNeighborIds: VertexRDD[Array[VertexId]] = sparkSession.time(graph.collectNeighborIds(EdgeDirection.Out))

  println("Neighborhood Connectivity: ")
  private val graphNeighborhoodConnectivity: Graph[Double, PartitionID] = sparkSession.time(graph.neighborhoodConnectivity())

  println("Vertex Embeddedness: ")
  private val graphVertexEmbeddedness: Graph[Double, PartitionID] = sparkSession.time(graph.vertexEmbeddedness())

  println("Local Clustering: ")
  private val graphLocalClustering: Graph[Double, PartitionID] = sparkSession.time(graph.localClustering())

  println("SCAN (PSCAN): ")
  private val pScanCommunityDetection: Graph[Long, PartitionID] = sparkSession.time(graph.PSCAN())

  //  println("Label propagation based graph coarsening: ")
  //  private val graphLabelPropagation: Graph[Component, PartitionID] = sparkSession.time(graph.LPCoarse())

  println("Label Propagation Algorithm GraphFrame: ")
  private val gfLabelPropagationDF: DataFrame = sparkSession.time(graphFrame.labelPropagation.maxIter(5).run())

  case class OutputClass(id: Long,
                         email: String,
                         triangleCount: Option[Int],
                         neighborhoodConnectivity: Option[Double],
                         graphVertexEmbeddedness: Option[Double],
                         graphLocalClustering: Option[Double],
                         connectedComponent: Option[Long],
                         stronglyConnectedComponent: Option[Long],
                         pScanCommunityDetection: Option[Long],
                         inNeighbors: Option[Array[VertexId]],
                         outNeighborIds: Option[Array[VertexId]]
                         //                         graphLabelPropagation: Option[Component]
                        )


  private val vertexDF: DataFrame = sparkSession.createDataFrame(
    graph.vertices
      .leftOuterJoin(triangleCounts.vertices)
      .leftOuterJoin(graphNeighborhoodConnectivity.vertices)
      .leftOuterJoin(graphVertexEmbeddedness.vertices)
      .leftOuterJoin(graphLocalClustering.vertices)
      .leftOuterJoin(connectedComponents.vertices)
      .leftOuterJoin(stronglyConnectedComponents.vertices)
      .leftOuterJoin(pScanCommunityDetection.vertices)
      .leftOuterJoin(inNeighbors)
      .leftOuterJoin(outNeighborIds)
      //      .leftOuterJoin(graphLabelPropagation.vertices)
      .map({
        case (vertexID,
        (
          (
            (
              (
                (
                  (
                    (
                      //                      (
                      (
                        (email,
                        triangleCountOutput),
                        neighborConnectivity),
                      graphVertexEmbedded),
                    graphLocalCluster),
                  connectedComponent),
                stronglyConnectedComponent),
              pScanCommunityDetection),
            inNeighbors),
          outNeighborIds)
          //          graphLabelPropagation)

          )
        =>
          OutputClass(
            vertexID,
            email,
            triangleCountOutput,
            neighborConnectivity,
            graphVertexEmbedded,
            graphLocalCluster,
            connectedComponent,
            stronglyConnectedComponent,
            pScanCommunityDetection,
            inNeighbors,
            outNeighborIds
            //            graphLabelPropagation
          )
      }))
    .join(gfLabelPropagationDF, Seq("id"), "left_outer").drop("attr").withColumnRenamed("label", "graphFrameLabelPropagation")
    .sort("neighborhoodConnectivity")

  vertexDF.show(571, truncate = true)

}
