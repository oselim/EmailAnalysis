package edu.phd.EmailParser.util

import java.io.File


class FileUtil {

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    val files = these.filter(_.isFile)
    files ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }


}
