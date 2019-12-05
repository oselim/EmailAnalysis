import java.io.{ByteArrayInputStream, File}
import java.nio.charset.CodingErrorAction
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.io.Codec
import javax.mail.{Address, Session}
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.sql.Column

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

  val preprocessStartTime: Long = System.nanoTime()
  val programStartTime = System.nanoTime()


  val cpu = 6

  val conf = new SparkConf().setAppName("LocalAllData").setMaster("local[" + cpu + "]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

  sc.setCheckpointDir("S:\\6410-proposal\\checkPointing\\")

  println("Spark Started.")

  System.setProperty("mail.mime.address.strict", "false")

  implicit val codec: Codec = Codec.ISO8859
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

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

  val files: Array[File] = recursiveListFiles(new File("S:\\6410-proposal\\maildir"))

  println("File List created.")

  val vertexArray =
    new ArrayBuffer[(VertexId, String)]()
  val edgeArray =
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

  println("File read Starting.")

  files.foreach(file => {
    val fileRDD = sc.wholeTextFiles("file:///" + file )
    val fileString: String = fileRDD.first()._2
    val mimeMessage = new MimeMessage(session, new ByteArrayInputStream(fileString.getBytes))
    val fromAddresses = mimeMessage.getFrom
    var allRecipients = mimeMessage.getAllRecipients
    if (allRecipients == null) {
      allRecipients = Array.apply(new InternetAddress("NonRecipient", false))
    }
    parseAddresses(fromAddresses, allRecipients)
  })

  println("Preprocess finished. File Read finished.")

  private val preprocessTime: Long = System.nanoTime() - preprocessStartTime
  private val graphBuildingStartTime = System.nanoTime()

  private val vertexRDD: RDD[(VertexId, String)] = sc.parallelize(vertexArray)
  private val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

  private val graph = Graph(vertexRDD, edgeRDD)
  private val graphFrame: GraphFrame = GraphFrame.fromGraphX(graph)

  println("Graphs created.")
  println("Algorithms running.")

  private val graphBuildingTime = System.nanoTime() - graphBuildingStartTime

  private val triangleStartTime: Long = System.nanoTime()
  private val triangleCounts: Graph[PartitionID, Int] = graph.triangleCount()
  private val triangleTime: Long = System.nanoTime() - triangleStartTime

  private val ccStartTime: Long = System.nanoTime()
  private val connectedComponents: Graph[VertexId, Int] = graph.connectedComponents()
  private val ccTime: Long = System.nanoTime() - ccStartTime

  private val sccStartTime: Long = System.nanoTime()
  private val stronglyConnectedComponents: Graph[VertexId, PartitionID] = graph.stronglyConnectedComponents(10)
  private val sccTime: Long = System.nanoTime() - sccStartTime

  private val inNeighborStartTime: Long = System.nanoTime()
  private val inNeighbors: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
  private val inNeighborTime: Long = System.nanoTime() - inNeighborStartTime

  private val outNeighborStartTime: Long = System.nanoTime()
  private val outNeighborIds: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)
  private val outNeighborTime: Long = System.nanoTime() - outNeighborStartTime

  private val neighborhoodConnectivityStartTime: Long = System.nanoTime()
  private val graphNeighborhoodConnectivity: Graph[Double, PartitionID] = graph.neighborhoodConnectivity()
  private val neighborhoodConnectivityTime: Long = System.nanoTime() - neighborhoodConnectivityStartTime

  private val vertexEmbeddednessStartTime: Long = System.nanoTime()
  private val graphVertexEmbeddedness: Graph[Double, PartitionID] = graph.vertexEmbeddedness()
  private val vertexEmbeddednessTime: Long = System.nanoTime() - vertexEmbeddednessStartTime

  private val localClusteringStartTime: Long = System.nanoTime()
  private val graphLocalClustering: Graph[Double, PartitionID] = graph.localClustering()
  private val localClusteringTime: Long = System.nanoTime() - localClusteringStartTime

  private val pScanStartTime: Long = System.nanoTime()
  private val pScanCommunityDetection: Graph[Long, PartitionID] = graph.PSCAN()
  private val pScanTime: Long = System.nanoTime() - pScanStartTime

  private val labelPropagationStartTime: Long = System.nanoTime()
  private val gfLabelPropagationDF: DataFrame = graphFrame.labelPropagation.maxIter(5).run()
  private val labelPropagationTime: Long = System.nanoTime() - labelPropagationStartTime

  println("Algorithms finished.")
  private val outputStartTime: Long = System.nanoTime()

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
      .map({
        case (vertexID,
        (
          (
            (
              (
                (
                  (
                    (
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
          )
      }))
    .join(gfLabelPropagationDF, Seq("id"), "left_outer").drop("attr").withColumnRenamed("label", "graphFrameLabelPropagation")
    .sort("neighborhoodConnectivity")

  private val outputTime: Long = System.nanoTime() - outputStartTime
  private val programTime: Long = System.nanoTime() - programStartTime

  def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))

  vertexDF.repartition(1)
    .withColumn("inNeighbors", stringify(new Column("inNeighbors")))
    .withColumn("outNeighborIds", stringify(new Column("outNeighborIds")))
    .withColumn("preprocessTime", lit(TimeUnit.MILLISECONDS.convert(preprocessTime, TimeUnit.NANOSECONDS)))
    .withColumn("graphBuildingTime", lit(TimeUnit.MILLISECONDS.convert(graphBuildingTime, TimeUnit.NANOSECONDS)))
    .withColumn("triangleTime", lit(TimeUnit.MILLISECONDS.convert(triangleTime, TimeUnit.NANOSECONDS)))
    .withColumn("ccTime", lit(TimeUnit.MILLISECONDS.convert(ccTime, TimeUnit.NANOSECONDS)))
    .withColumn("sccTime", lit(TimeUnit.MILLISECONDS.convert(sccTime, TimeUnit.NANOSECONDS)))
    .withColumn("inNeighborTime", lit(TimeUnit.MILLISECONDS.convert(inNeighborTime, TimeUnit.NANOSECONDS)))
    .withColumn("outNeighborTime", lit(TimeUnit.MILLISECONDS.convert(outNeighborTime, TimeUnit.NANOSECONDS)))
    .withColumn("neighborhoodConnectivityTime", lit(TimeUnit.MILLISECONDS.convert(neighborhoodConnectivityTime, TimeUnit.NANOSECONDS)))
    .withColumn("vertexEmbeddednessTime", lit(TimeUnit.MILLISECONDS.convert(vertexEmbeddednessTime, TimeUnit.NANOSECONDS)))
    .withColumn("localClusteringTime", lit(TimeUnit.MILLISECONDS.convert(localClusteringTime, TimeUnit.NANOSECONDS)))
    .withColumn("pScanTime", lit(TimeUnit.MILLISECONDS.convert(pScanTime, TimeUnit.NANOSECONDS)))
    .withColumn("labelPropagationTime", lit(TimeUnit.MILLISECONDS.convert(labelPropagationTime, TimeUnit.NANOSECONDS)))
    .withColumn("outputTime", lit(TimeUnit.MILLISECONDS.convert(outputTime, TimeUnit.NANOSECONDS)))
    .withColumn("programTime", lit(TimeUnit.MILLISECONDS.convert(programTime, TimeUnit.NANOSECONDS)))
    .withColumn("NumberOfFiles", lit(files.length))
    .withColumn("NumberOfVertices", lit(graph.numVertices))
    .withColumn("NumberOfEdges", lit(graph.numEdges))
    .withColumn("SparkContextConf", lit(sc.getConf.getAll.deep.toString()))
    .write.option("header", value = true).csv("S:\\6410-proposal\\LocalResearchOutputs\\CommunityDetection\\local_fulldata_cpu" + cpu)

}
