import java.io.{ByteArrayInputStream, File}
import java.util.Properties

import javax.mail.{Address, Session}
import edu.phd.EmailParser.util.FileUtil

import scala.io.Source
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
// import classes required for using GraphX
import org.apache.spark.graphx._


object EmailParser extends App {

  System.setProperty("mail.mime.address.strict", "false")

  val session = Session.getDefaultInstance(new Properties() {
    put("mail.smtp.host", "host")
    put("mail.mime.address.strict", "false")
  })

  private var fileUtil = new FileUtil()

  private var files: Array[File] = fileUtil.recursiveListFiles(new File("enron-sample-dataset"))

  private var vertexArray = new ArrayBuffer[(Long, (String, String, String))]()
  private var edgeArray = new ArrayBuffer[Edge[ (String, String) ]]()

  def parseAddresses(fromAddresses: Array[Address], receiverAddresses: Array[Address], xOriginHeader: Array[String], messageID: String): Unit = {
    fromAddresses.foreach(from => {
      //TODO algoritma yazilmali
      //TODO from base alinarak her bir from icin bir vertex eklenip butun to cc bcc dolasilip vertexler eklenip, egdeler eklenmeli. Her bir vertex icin contains controlu yapilmali. edgeler icin de contains kontrolu yapilabilir.


      val xOrigin = xOriginHeader(0)
      val fromString = from.toString
      addToVertex(fromString, xOrigin, messageID)

      receiverAddresses.foreach(receiver => {
        val receiverString = receiver.toString
        addToVertex(receiverString, xOrigin, messageID)
        addToEdge(fromString, receiverString, xOrigin, messageID)
      })
    })
  }

  def addToVertex(email: String, xOrigin: String, messageId: String): Unit = {
    var vID = 0L
    if (!email.equals("default"))
      vID = email.hashCode.toLong

    if (!vertexArray.exists(vertex => vertex._1.equals(vID))) {
      vertexArray.append((vID, (xOrigin, email, messageId)))
    }
  }

  def addToEdge(sourceAddress: String, destinationAddress: String, xOrigin: String, messageId: String): Unit = {
    val sourceID = sourceAddress.hashCode.toLong
    var destinationID = 0L
    if (!destinationAddress.equals("default"))
      destinationID = destinationAddress.hashCode.toLong
    edgeArray.append(Edge(sourceID, destinationID, (xOrigin, messageId)))
  }

  files.foreach(file => {
    val source = Source.fromFile(file)
    val fileString: String = source.mkString
    val mimeMessage = new MimeMessage(session, new ByteArrayInputStream(fileString.getBytes))
    val fromAddresses = mimeMessage.getFrom
    var allRecipients = mimeMessage.getAllRecipients
    if (allRecipients == null) {
      allRecipients = Array.apply(new InternetAddress("default", false))
    }
    val xOriginHeader = mimeMessage.getHeader("X-Origin")
    val messageID = mimeMessage.getMessageID

    parseAddresses(fromAddresses, allRecipients, xOriginHeader, messageID)

    source.close()
  })

  val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()


  private val vertexRDD: RDD[(VertexId, (String, String, String))] = sc.parallelize(vertexArray)
  private val edgeRDD: RDD[Edge[(String, String)]] = sc.parallelize(edgeArray)

  private val graph = Graph(vertexRDD, edgeRDD)
  println(graph.numEdges)
  println(graph.numVertices)

  case class OutputClass(id: Long, pageRank: Double, email: String)

  private val pageRank: Graph[Double, Double] = graph.pageRank(0.0001)
  private val sortedPageRankGraph: RDD[(VertexId, (Double, (String, String, String)))] = pageRank.vertices.join(vertexRDD).sortBy(_._2._1, ascending = false)
//  println(sortedPageRankGraph.collect().deep)


  //  println(graph.inDegrees.collect().deep )
  //  println(graph.outDegrees.collect().deep )


  private val vertexDF: DataFrame = sparkSession.createDataFrame(sortedPageRankGraph.map(x => OutputClass(x._1, x._2._1, x._2._2._2) ) )

  vertexDF.show(truncate = false)

/*
  graph.vertices.filter(vertex => {
    val vertexID = vertex._1
    graph.
  })
*/


}
