package util

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import javax.mail.MessagingException
import javax.mail.Session
import javax.mail.internet.MimeMessage
import com.sun.mail.smtp.SMTPOutputStream

@SerialVersionUID(3763328805281033284L)
class SerializableMimeMessage(var mimeMessage: MimeMessage) extends Serializable {
  @throws[IOException]
  private def writeObject(oos: ObjectOutputStream) = { // convert
    val baos = new ByteArrayOutputStream
    val os = new SMTPOutputStream(baos)
    try mimeMessage.writeTo(os)
    catch {
      case e: MessagingException =>
        throw new IOException("MimeMessage could not be serialized.", e)
    }
    os.flush()
    val serializedEmail = baos.toByteArray
    // default serialization
    oos.defaultWriteObject()
    // write the object
    oos.writeInt(serializedEmail.length)
    oos.write(serializedEmail)
  }

  @throws[ClassNotFoundException]
  @throws[IOException]
  private def readObject(ois: ObjectInputStream) = { // default deserialization
    ois.defaultReadObject()
    // read the object
    val len = ois.readInt
    val serializedEmail = new Array[Byte](len)
    ois.readFully(serializedEmail)
    val bais = new ByteArrayInputStream(serializedEmail)
    try mimeMessage = new MimeMessage(null.asInstanceOf[Session], bais)
    catch {
      case e: MessagingException =>
        throw new IOException("MimeMessage could not be deserialized.", e)
    }
  }

  def getMimeMessage = mimeMessage
}