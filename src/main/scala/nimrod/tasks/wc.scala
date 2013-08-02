package nimrod.tasks

import java.io.File

object wc {
  def apply(file : File) = if(file.getPath() endsWith ".gz") {
    io.Source.fromInputStream(new java.util.zip.GZIPInputStream(new java.io.FileInputStream(file))).getLines.size
  } else {
    io.Source.fromFile(file).getLines.size
  }
  def apply(file : String) : Int = apply(new File(file))
}
