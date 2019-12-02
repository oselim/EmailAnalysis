package util

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


class HDFSUtil {


  val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
  private val files: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path("/opt/phd/maildir"), true)
  val filenames: ListBuffer[String] = ListBuffer[ String ]( )
  while ( files.hasNext ) {
    files.next().getPath.toString
    filenames += files.next().getPath.toString
  }
   filenames.foreach(file => {

   })
  filenames.take(600)
  println(filenames.length)

  private val fileNamesRDD: RDD[String] = sc.parallelize(filenames)
  fileNamesRDD.collect().deep
  fileNamesRDD.map( file => {

  })

  private val file: RDD[(String, String)] = sc.wholeTextFiles("/opt/phd/enron-sample-dataset/allen-p/_sent_mail/10.")
  private val fileString: String = file.first()._2


}
