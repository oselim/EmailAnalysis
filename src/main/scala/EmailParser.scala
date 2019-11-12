import java.io.{ByteArrayInputStream, File}
import java.util.Properties
import javax.mail.{Address, Message, Session}
import edu.phd.EmailParser.util.FileUtil
import scala.io.Source
import javax.mail.internet.MimeMessage
import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.rdd.RDD
// import classes required for using GraphX
import org.apache.spark.graphx._


object EmailParser extends App {
  val session = Session.getDefaultInstance(new Properties() { put("mail.smtp.host", "host") })

  private var fileUtil = new  FileUtil()

  private var files: Array[File] = fileUtil.recursiveListFiles(new File("enron-sample-dataset"))

  private var vertexArray = new ArrayBuffer[(Long, (String, String))]()
  private var edgeArray = new ArrayBuffer[(Long, Long, (String, String))]()

  def parseAddresses(fromAddresses: Array[Address], toAddresses: Array[Address], ccAddresses: Array[Address], bccAddresses: Array[Address], xOriginHeader: Array[String], messageID: String): Unit = {
    fromAddresses.par.foreach(from => {
      //TODO algoritma yazilmali
      //TODO from base alinarak her bir from icin bir vertex eklenip butun to cc bcc dolasilip vertexler eklenip, egdeler eklenmeli. Her bir vertex icin contains controlu yapilmali. edgeler icin de contains kontrolu yapilabilir.

    })
  }

  def addToVertex( email: String, xOrigin: String, messageId:String ): Unit ={
    //TODO vertexID olusturulacak, hash ve ya getBytes ile
    //TODO contains ile kontrol edilecek
  }

  def addToEdge( sourceAddress: String, destinationAddress:String, xOrigin: String, messageId:String): Unit ={
    //TODO sourceAddress ve destinationAddress long tipine donusturulecek, hash ve ya getbytes ile
    //TODO contains ile kontrol edilecek
    //TODO https://www.youtube.com/watch?v=QV6L1zRQ6JM&list=PLkK1Z8GOwcNNIhfdiioIoLVTHVj49GqmS&index=7
    
  }

  files.par.foreach(file => {
    val source = Source.fromFile(file)
    val fileString: String = source.mkString
    val mimeMessage = new MimeMessage(session, new ByteArrayInputStream(fileString.getBytes))
    val fromAddresses = mimeMessage.getFrom
    val toAddresses = mimeMessage.getRecipients( Message.RecipientType.TO)
    val ccAddresses = mimeMessage.getRecipients( Message.RecipientType.CC)
    val bccAddresses = mimeMessage.getRecipients( Message.RecipientType.BCC)
    val xOriginHeader = mimeMessage.getHeader("X-Origin")
    val messageID = mimeMessage.getMessageID

    parseAddresses(fromAddresses, toAddresses, ccAddresses, bccAddresses, xOriginHeader, messageID)

    source.close()
  })

  val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val sc = new SparkContext(conf)






}
