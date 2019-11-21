import java.io.{ByteArrayInputStream, File}
import java.util.Properties

import javax.mail.{Address, Session}
import edu.phd.EmailParser.util.FileUtil

import scala.io.Source
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.ml.feature.VectorAssembler


object EmailParser extends App {

  System.setProperty("mail.mime.address.strict", "false")

  val session = Session.getDefaultInstance(new Properties() {
    put("mail.smtp.host", "host")
    put("mail.mime.address.strict", "false")
  })

  private val fileUtil = new FileUtil()
  private val files: Array[File] = fileUtil.recursiveListFiles(new File("enron-sample-dataset"))

  private val vertexArray = new ArrayBuffer[(Long, String)]()
  private val edgeArray = new ArrayBuffer[Edge[(String, String)]]()

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
    edgeArray.append(Edge(sourceID, destinationID, (sourceAddress, destinationAddress)))
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
  private val edgeRDD: RDD[Edge[(String, String)]] = sc.parallelize(edgeArray)

  private val graph = Graph(vertexRDD, edgeRDD)
  println(graph.numEdges)
  println(graph.numVertices)


  private val pageRank: Graph[Double, Double] = graph.pageRank(0.0001)
  private val triangleCounts: Graph[PartitionID, (String, String)] = graph.triangleCount()
  private val connectedComponents: Graph[VertexId, (String, String)] = graph.connectedComponents()
  private val inNeighbors: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
  private val outNeighborIds: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)

  case class OutputClass(id: Long, pageRank: Double, email: String, inDegree: Int, outDegree: Int, triangleCount: Int,
                         connectedComponent: VertexId, inNeighbors: Int, outNeighborIds: Int)

  private val vertexDF: DataFrame = sparkSession.createDataFrame(
    graph.vertices.fullOuterJoin(pageRank.vertices)
      .fullOuterJoin(graph.inDegrees)
      .fullOuterJoin(graph.outDegrees)
      .fullOuterJoin(triangleCounts.vertices)
      .fullOuterJoin(connectedComponents.vertices)
      .fullOuterJoin(inNeighbors)
      .fullOuterJoin(outNeighborIds)
      .map(vertex => {
        val vertexID = vertex._1
        val pageRankValue = vertex._2._1.get._1.get._1.get._1.get._1.get._1.get._2.get
        val email = vertex._2._1.get._1.get._1.get._1.get._1.get._1.get._1.get
        val inDegree = vertex._2._1.get._1.get._1.get._1.get._1.get._2.getOrElse(0)
        val outDegree = vertex._2._1.get._1.get._1.get._1.get._2.getOrElse(0)
        val triangleCount = vertex._2._1.get._1.get._1.get._2.get
        val connectedComponent = vertex._2._1.get._1.get._2.get
        val inNeighbors = vertex._2._1.get._2.get.toVector
        val outNeighborIds = vertex._2._2.get.toVector

        OutputClass(vertexID, pageRankValue, email, inDegree, outDegree, triangleCount, connectedComponent, inNeighbors.length,
          outNeighborIds.length)
      }))

//  vertexDF.show(571, truncate = true)

  private val assembler = new VectorAssembler().setInputCols(Array("connectedComponent", "pageRank", "inDegree", "outDegree",
    "triangleCount", "inNeighbors", "outNeighborIds")).setOutputCol("features")

  private val featuredDataFrame: DataFrame = assembler.transform(vertexDF)

  private val model: KMeansModel = new KMeans()
    .setSeed(1)
    .setK(2)
    .setFeaturesCol("features")
    .setPredictionCol("predictions")
    .fit(featuredDataFrame)

  model.clusterCenters.foreach(println)

  private val predictedDataFrame: DataFrame = model.transform(featuredDataFrame)

  predictedDataFrame.show(600, truncate = false)

}
