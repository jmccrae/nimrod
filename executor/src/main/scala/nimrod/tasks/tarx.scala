import nimrod._

import java.io.File

sealed trait Compression {
  def tarCommand : String
}
object GZip extends Compression {
  val tarCommand = "z"
}
object BZip2 extends Compression {
  val tarCommand = "j"
}
object NoCompression extends Compression {
  val tarCommand = ""
}

class tarx(file : File, compression : Compression, messenger : TaskMessenger) extends
Do(List("tar",compression.tarCommand+"xvf",file.getCanonicalPath),messenger)

object tarxz {
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new tarx(file,GZip,workflow))
  def apply(file : String)(implicit workflow : Workflow) = workflow.register(new tarx(new File(file),GZip,workflow))
}

object tarxj {
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new tarx(file,BZip2,workflow))
  def apply(file : String)(implicit workflow : Workflow) = workflow.register(new tarx(new File(file),BZip2,workflow))
}

object tarx {
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new tarx(file,NoCompression,workflow))
  def apply(file : String)(implicit workflow : Workflow) = workflow.register(new tarx(new File(file),NoCompression,workflow))
}


