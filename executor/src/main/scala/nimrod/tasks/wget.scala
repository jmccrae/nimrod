package nimrod.tasks

import java.io._
import java.net._
import nimrod._

class wget(url : URL, protected val messenger : TaskMessenger) extends Task {
  private var noclobber = false
  private var outputStream : OutputStream = System.out

  private val digits = "(\\d+)*".r

  private def fileSize(conn : URLConnection) = {
    conn.setRequestProperty("User-Agent",
                "Mozilla/5.0 (X11; Linux i686; rv:2.0b12) Gecko/20110222 Firefox/4.0b12");
    conn.setRequestProperty("Accept-Language",
                "el-gr,el;q=0.8,en-us;q=0.5,en;q=0.3");
    conn.setRequestProperty("Accept-Charset",
                "ISO-8859-7,utf-8;q=0.7,*;q=0.7");
    conn.getHeaderField("Content-Length") match {
      case digits(n) => n.toLong
      case _ => -1l
    }
  }

  def run : Int = {
    try {
      val conn = url.openConnection()
      val size = fileSize(conn)
    /*  if(size >= 0) {
        setPips((size/4096).toInt)
      }*/
      val stream = conn.getInputStream()
      var buf = new Array[Byte](4096)
      var read = stream.read(buf)
      while(read != -1) {
        outputStream.write(buf,0,read)
        read = stream.read(buf)
        //pip
      }
      outputStream.flush
      if(outputStream != System.out) {
        outputStream.close
      }
      //done
      return 0
    } catch {
      case x : Exception => {
        x.printStackTrace
        -1
      }
    }
  }

  def >(file : File) : wget = {
    this generates FileArtifact(file)
    outputStream = new FileOutputStream(file)
    return this
  }

  def >(file : String) : wget = this > (new File(file))

  override def toString = "wget " + url
}

object wget {
  def apply(url : URL)(implicit wf : Workflow) = wf.register(new wget(url, wf))
  def apply(url : String)(implicit wf : Workflow) = wf.register(new wget(new URL(url), wf))
}
