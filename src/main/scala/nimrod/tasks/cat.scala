package nimrod.tasks

import nimrod._
import java.io.File
import java.io.PrintStream
import java.util.Scanner

class cat(files : => Seq[File]) extends Task {
  private var outFile : Option[File] = None
  override def exec = {
    val out = outFile match {
      case Some(file) => new PrintStream(file)
      case None => System.out
    }
    for(file <- files) {
      val in = new Scanner(file)
      while(in.hasNextLine) {
        out.println(in.nextLine)
      }
    }
    out.flush
    out.close
    0
  }

  def >(file : File) = {
    outFile = Some(file)
    this
  }
  def >(path : String) = {
    outFile = Some(new File(path))
    this
  }

  override def toString = "cat " + files.mkString(" ")
}

object cat {
  /*def apply(files : Any*)(implicit workflow : Workflow) = workflow.register(new cat({files.map({
    case f : File => f
    case s : String => new File(s)
    case _ => throw new IllegalArgumentException()
  }).toSeq}))*/
  def apply(files : => Seq[File])(implicit workflow : Workflow) = workflow.register(new cat(files))
}
