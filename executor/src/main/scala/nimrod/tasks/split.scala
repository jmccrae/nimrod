package nimrod.tasks

import nimrod._
import java.io.File
import java.io.PrintWriter
import java.util.Scanner

class split(file : File, lineCount : Int, namer : Int => String, protected val messenger : TaskMessenger) extends Task {
  def run = {
    val in = new Scanner(file)
    var part = 1
    var linesRead = 0
    val f = new File(namer(part))
    if(!f.getParentFile().exists()) {
      f.getParentFile().mkdirs()
    }
    var out = new PrintWriter(f)
    while(in.hasNextLine()) {
      val line = in.nextLine()

      if(linesRead / lineCount >= part) {
        out.flush
        out.close
        part += 1
        val f = new File(namer(part))
        if(!f.getParentFile().exists()) {
          f.getParentFile().mkdirs()
        }
        out = new PrintWriter(f)
      }
      out.println(line)
      linesRead += 1
    }
    out.flush
    out.close
    0
  }

  override def toString = "split " + file.getCanonicalPath()
}

object split {
  def apply(lines : Int, file : File)(namer : Int => String)(implicit workflow : Workflow) = {
    workflow.register(new split(file,lines,namer, workflow))
  }
  def apply(lines : Int, path : String)(namer : Int => String)(implicit workflow : Workflow) = {
    workflow.register(new split(new File(path),lines,namer, workflow))
  }
}
