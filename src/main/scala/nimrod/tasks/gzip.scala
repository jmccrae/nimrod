package nimrod.tasks
import nimrod._
import java.io.File

class gzip(file : File, messenger : TaskMessenger) extends Do(List("gzip"), messenger) {
  requires(file)
  val targetFile = new File(file.getCanonicalPath()+".gz")
  generates(new FileArtifact(targetFile))

  this < file
  this > targetFile

  override def toString = "gzip " + file
}

object gzip {
  def apply(path : String)(implicit wf : Workflow) =  wf.register(new gzip(new File(path), wf))
  def apply(file : File)(implicit wf : Workflow) = wf.register(new gzip(file, wf))
}
